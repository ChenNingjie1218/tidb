// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/inference/embedding/batcher"
	"github.com/pingcap/tidb/pkg/inference/embedding/jina"
	"github.com/pingcap/tidb/pkg/inference/embedding/mock"
	"github.com/pingcap/tidb/pkg/inference/embedding/openai"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"golang.org/x/sync/singleflight"
)

const (
	// EmbeddingCacheSize is the number of entries in the embedding cache.
	// This cache is mainly used for deduplicating embedding requests when user
	// calls the same SQL embed function in multiple places. It does also served
	// as a normal cache to reduce embedding API calls.
	EmbeddingCacheSize = 10000
)

// EmbedFn provides SQL adaptor for Embedding models. This is supposed to be put inside a
// Domain so that it can be shared across sessions.
type EmbedFn struct {
	embedder *batcher.BatchEmbedder
	sf       singleflight.Group
	cache    *ristretto.Cache
}

// NewEmbedFn creates a new EmbedFn instance.
func NewEmbedFn() *EmbedFn {
	configureJinaAPIKey := fmt.Sprintf("SET @@GLOBAL.%s='<API_KEY>'", strings.ToUpper(variable.TiDBExpEmbedJinaAPIKey))
	configureOpenAIAPIKey := fmt.Sprintf("SET @@GLOBAL.%s='<API_KEY>'", strings.ToUpper(variable.TiDBExpEmbedOpenAIAPIKey))

	embedder := batcher.NewBatchEmbedder()
	embedder.RegisterEmbedder("jina", jina.NewJinaEmbedder(jina.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedJinaAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf("JinaAI API key is not configured, to configure the API key: %s", configureJinaAPIKey),
		ErrUnauthorized:  fmt.Errorf("JinaAI returns status unauthorized, check your JinaAI API key. To reconfigure a new API key: %s", configureJinaAPIKey),
	}))
	embedder.RegisterEmbedder("openai", openai.NewOpenAIEmbedder(openai.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedOpenAIAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf("OpenAI API key is not configured, to configure the API key: %s", configureOpenAIAPIKey),
		ErrUnauthorized:  fmt.Errorf("OpenAI returns status unauthorized, check your OpenAI API key. To reconfigure a new API key: %s", configureOpenAIAPIKey),
	}))
	if intest.InTest {
		embedder.RegisterEmbedder("mock", mock.NewMockEmbedder())
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        EmbeddingCacheSize,
		MaxCost:            EmbeddingCacheSize,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(err)
	}
	return &EmbedFn{
		embedder: embedder,
		cache:    cache,
	}
}

// Embed generates embeddings for the given text. It handles with cache and batching.
func (e *EmbedFn) Embed(shouldCancel func() bool, modelWithProvider string, text string, opts map[string]any) ([]float32, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In TiDB expression execution, usually only a Killed flag can be retrieved.
	// This creates the context from the Killed flag which checks at 1s interval.
	go func() {
		for {
			if shouldCancel() {
				cancel()
				return
			}
			select {
			case <-time.After(1 * time.Second):
				// Check the shouldCancel condition every second.
			case <-ctx.Done():
				return
			}
		}
	}()

	if opts == nil {
		opts = make(map[string]any)
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize opts: %w", err)
	}
	cacheKey := fmt.Sprintf("%s/%s/%s", modelWithProvider, text, string(optsJSON))
	if cached, found := e.cache.Get(cacheKey); found {
		return cached.([]float32), nil
	}
	result, err, _ := e.sf.Do(modelWithProvider+"/"+text, func() (any, error) {
		embeddings, err := e.embedder.CreateEmbeddings(ctx, modelWithProvider, []string{text}, opts)
		if err != nil {
			return nil, err
		}
		if len(embeddings) == 0 {
			return nil, fmt.Errorf("no embeddings returned for model %s and text %s", modelWithProvider, text)
		}
		e.cache.Set(cacheKey, embeddings[0], 1)
		return embeddings[0], nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]float32), err
}

// Close releases resources held by the EmbedFn.
func (e *EmbedFn) Close() {
	e.cache.Close()
}

// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotequery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const executorPingInterval = 30 * time.Second

// SQLSession is a session that can execute SQL.
type SQLSession interface {
	Execute(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	GetSessionVars() *variable.SessionVars
}

// Executor is a remote query executor.
type Executor struct {
	httpClient *http.Client
	masterAddr string
	done       chan struct{}

	QueryAddr string
	Query     string `json:"query"`
	User      string `json:"user"`
	UserHost  string `json:"userHost"`
	DB        string `json:"db"`
}

// NewExecutor creates a new remote query executor.
func NewExecutor(queryAddr string) (*Executor, error) {
	e := &Executor{
		httpClient: util.InternalHTTPClient(),
		QueryAddr:  queryAddr,
		done:       make(chan struct{}),
	}
	err := e.loadQuery()
	if err != nil {
		return nil, err
	}
	go e.start()
	return e, nil
}

func (e *Executor) loadQuery() error {
	res, err := e.httpClient.Get(util.ComposeURL(e.QueryAddr, ""))
	if err != nil {
		logutil.BgLogger().Error("failed to load query", zap.Error(err))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("load_query", "failed").Inc()
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("failed to load query", zap.String("status", res.Status))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("load_query", "failed").Inc()
		return errors.New("failed to load query")
	}
	err = json.NewDecoder(res.Body).Decode(e)
	if err != nil {
		logutil.BgLogger().Error("failed to decode query", zap.Error(err))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("load_query", "failed").Inc()
		return err
	}
	logutil.BgLogger().Info("query loaded", zap.String("queryAddr", e.QueryAddr))
	metrics.RemoteQueryWorkerCounter.WithLabelValues("load_query", "ok").Inc()
	return nil
}

func (e *Executor) start() {
	ticker := time.NewTicker(executorPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.ping()
		}
	}
}

func (e *Executor) ping() {
	res, err := e.httpClient.Get(util.ComposeURL(e.QueryAddr, "/ping"))
	if err != nil {
		logutil.BgLogger().Error("failed to ping server", zap.Error(err))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("ping", "failed").Inc()
		return
	}
	res.Body.Close()
	metrics.RemoteQueryWorkerCounter.WithLabelValues("load_query", "ok").Inc()
}

// Close closes the executor.
func (e *Executor) Close() {
	close(e.done)
}

// Execute executes the query.
func (e *Executor) Execute(ctx context.Context, se SQLSession) error {
	start := time.Now()
	se.GetSessionVars().CurrentDB = e.DB
	se.GetSessionVars().User = &auth.UserIdentity{Username: e.User, Hostname: e.UserHost}
	// TODO: other variables
	rss, err := se.Execute(ctx, e.Query)
	if err != nil {
		logutil.BgLogger().Error("failed to execute query", zap.Error(err))
		e.postError(err.Error())
		return err
	}
	rs := rss[0]
	defer rs.Close()
	firstChunk := true
	var codec *chunk.Codec
	for {
		chk := rs.NewChunk(nil)
		err := rs.Next(ctx, chk)
		if err != nil {
			logutil.BgLogger().Error("failed to fetch result", zap.Error(err))
			e.postError(err.Error())
			return err
		}
		if firstChunk {
			firstChunk = false
			fields := make([]*ColumnField, 0, len(rs.Fields()))
			fieldTypes := make([]*types.FieldType, 0, len(rs.Fields()))
			for _, field := range rs.Fields() {
				fields = append(fields, &ColumnField{
					Column:       field.Column,
					ColumnAsName: field.ColumnAsName,
					Table:        field.Table,
					TableAsName:  field.TableAsName,
					DBName:       field.DBName,
				})
				fieldTypes = append(fieldTypes, &field.Column.FieldType)
			}
			codec = chunk.NewCodec(fieldTypes)
			data, err := json.Marshal(fields)
			if err != nil {
				logutil.BgLogger().Error("failed to marshal meta", zap.Error(err))
				e.postError(err.Error())
				return err
			}
			err = e.postData(data)
			if err != nil {
				logutil.BgLogger().Error("failed to post meta chunk", zap.Error(err))
				return err
			}
			metrics.RemoteQueryWorkerDuration.WithLabelValues("first_chunk").Observe(time.Since(start).Seconds())
		}
		var chunkData []byte
		if chk.NumRows() > 0 {
			chunkData = codec.Encode(chk)
		}
		err = e.postData(chunkData)
		if err != nil {
			logutil.BgLogger().Error("failed to post data chunk", zap.Error(err))
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	metrics.RemoteQueryWorkerDuration.WithLabelValues("execute").Observe(time.Since(start).Seconds())
	return nil
}

func (e *Executor) postData(data []byte) error {
	start := time.Now()
	res, err := e.httpClient.Post(util.ComposeURL(e.QueryAddr, ""), "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		logutil.BgLogger().Error("failed to post query result", zap.Error(err))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("post_chunk", "failed").Inc()
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("failed to post query result", zap.String("status", res.Status))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("post_chunk", "failed").Inc()
		return errors.New("failed to post query result")
	}
	metrics.RemoteQueryWorkerCounter.WithLabelValues("post_chunk", "ok").Inc()
	metrics.RemoteQueryWorkerDuration.WithLabelValues("post_chunk").Observe(time.Since(start).Seconds())
	return nil
}

func (e *Executor) postError(errMessage string) {
	res, err := e.httpClient.Post(util.ComposeURL(e.QueryAddr, "/error"), "text/plain", bytes.NewReader([]byte(errMessage)))
	if err != nil {
		logutil.BgLogger().Error("failed to post error message", zap.Error(err))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("post_error", "failed").Inc()
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("failed to post error", zap.String("status", res.Status))
		metrics.RemoteQueryWorkerCounter.WithLabelValues("post_error", "failed").Inc()
		return
	}
	metrics.RemoteQueryWorkerCounter.WithLabelValues("post_error", "ok").Inc()
	return
}

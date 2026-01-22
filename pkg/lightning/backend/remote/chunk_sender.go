// Copyright 2025 PingCAP, Inc.
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

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// PutChunkResult is json data that returned by remote server PUT API.
type PutChunkResult struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
	Canceled       bool   `json:"canceled"`
	Finished       bool   `json:"finished"`
	Error          string `json:"error"`
}

type vectorPutChunkResult struct {
	HandledChunkID  uint64 `json:"handled_chunk_id"`
	ReceivedRecords int64  `json:"received_records"`
	Duplicate       bool   `json:"duplicate"`
	Canceled        bool   `json:"canceled"`
	Finished        bool   `json:"finished"`
	Error           string `json:"error"`
}

// FlushResult is json data that returned by remote server POST API.
type FlushResult struct {
	FlushedChunkIDs map[uint64]uint64 `json:"flushed-chunk-ids"`
	Canceled        bool              `json:"canceled"`
	Finished        bool              `json:"finished"`
	Error           string            `json:"error"`
}

type vectorFlushResult struct {
	WriterID        uint64 `json:"writer_id"`
	ReceivedChunkID uint64 `json:"received_chunk_id"`
	IngestedChunkID uint64 `json:"ingested_chunk_id"`
	InsertedRecords int64  `json:"inserted_records"`
	Canceled        bool   `json:"canceled"`
	Finished        bool   `json:"finished"`
	Error           string `json:"error"`
}

// chunk is a unit of data that send to remote worker.
type chunk struct {
	id   uint64
	data []byte
}

type chunksState struct {
	FlushedChunkID uint64 `json:"flushed-chunk-id"`
	HandledChunkID uint64 `json:"handled-chunk-id"`
}

type chunkTask struct {
	// For flush, resp is used to notify the completion of the flush.
	// For put chunk, resp is used to receive chunkID allocated by chunkSenderLoop.
	resp  chan uint64
	flush bool
	data  []byte
}

func newChunkTask(data []byte, flush bool) *chunkTask {
	return &chunkTask{
		resp:  make(chan uint64, 1),
		flush: flush,
		data:  data,
	}
}

func (t *chunkTask) done(chunkID uint64) {
	t.resp <- chunkID
	close(t.resp)
}

type chunkSender struct {
	id uint64
	wg sync.WaitGroup

	e           *engine
	httpClient  *http.Client
	chunksCache *chunkCache

	// state is used to record the state of the chunks in remote worker.
	state       atomic.Value
	nextChunkID atomic.Uint64

	// the following channels should be closed by chunkSenderLoop
	loopDoneChan chan struct{} // notify chunkSenderLoop is done

	// the following channels should be closed by closed method
	taskChan     chan *chunkTask
	loopQuitChan chan struct{} // notify chunkSenderLoop to quit

	err error
}

func newChunkSender(ctx context.Context, id uint64, engine *engine, chunksCache *chunkCache) *chunkSender {
	c := &chunkSender{
		id: id,
		wg: sync.WaitGroup{},

		e:           engine,
		httpClient:  engine.httpClient,
		chunksCache: chunksCache,

		state:       atomic.Value{},
		nextChunkID: atomic.Uint64{},

		loopDoneChan: make(chan struct{}),
		loopQuitChan: make(chan struct{}),
		taskChan:     make(chan *chunkTask),
	}
	c.state.Store(&chunksState{})

	c.wg.Add(1)
	go c.chunkSenderLoop(ctx)

	engine.logger.Info("chunk sender started", zap.Uint64("sender", c.id))
	return c
}

func (c *chunkSender) putChunk(ctx context.Context, data []byte) (uint64, error) {
	data0 := make([]byte, len(data))
	copy(data0, data)

	task := newChunkTask(data0, false)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case c.taskChan <- task:
	case <-c.loopDoneChan:
		return 0, c.err
	}

	newChunkID := uint64(0)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case newChunkID = <-task.resp:
	case <-c.loopDoneChan:
		return 0, c.err
	}

	return newChunkID, nil
}

func (c *chunkSender) flush(ctx context.Context) error {
	if c.getFlushedChunkID() == c.getLastChunkID() {
		return nil
	}

	task := newChunkTask(nil, true)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.taskChan <- task:
	case <-c.loopDoneChan:
		return c.err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-task.resp:
	case <-c.loopDoneChan:
		return c.err
	}

	return nil
}

func (c *chunkSender) getNextChunkID() uint64 {
	return c.nextChunkID.Add(1)
}

func (c *chunkSender) getLastChunkID() uint64 {
	return c.nextChunkID.Load()
}

func (c *chunkSender) getFlushedChunkID() uint64 {
	return c.state.Load().(*chunksState).FlushedChunkID
}

func (c *chunkSender) chunkSenderLoop(ctx context.Context) {
	defer func() {
		close(c.loopDoneChan)
		c.chunksCache.close()
		c.wg.Done()
	}()

	ticker := time.NewTicker(updateFlushedChunkDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.loopQuitChan:
			return
		case task := <-c.taskChan:
			// Reset the ticker when a new task is received. Avoid sending empty chunks too frequently.
			ticker.Reset(updateFlushedChunkDuration)
			if !task.flush {
				newChunkID := c.getNextChunkID()
				task.done(newChunkID)

				chunk := &chunk{
					id:   newChunkID,
					data: task.data,
				}
				c.err = c.putChunkToRemote(ctx, chunk)
				if c.err != nil {
					c.e.logger.Error("put chunk error",
						zap.Uint64("sender", c.id),
						zap.Error(c.err))
					return
				}
			} else {
				c.err = c.sendFlushToRemote(ctx)
				if c.err != nil {
					c.e.logger.Error("flush chunk error",
						zap.Uint64("sender", c.id),
						zap.Error(c.err))
					return
				}
				task.done(0)
			}
		case <-ticker.C:
			// Periodically send empty chunks to update the flushed chunkID.
			if c.e.taskType != taskTypeLoadData {
				continue
			}
			if c.getFlushedChunkID() == c.getLastChunkID() {
				continue
			}
			c.err = c.putEmptyChunk(ctx)
			if c.err != nil {
				c.e.logger.Error("flush empty chunk error",
					zap.Uint64("sender", c.id),
					zap.Error(c.err))
				return
			}
		}
	}
}

func (c *chunkSender) putEmptyChunk(ctx context.Context) error {
	if c.e.taskType != taskTypeLoadData {
		return fmt.Errorf("putEmptyChunk is not supported for task type %v", c.e.taskType)
	}
	chunk := &chunk{id: c.getLastChunkID(), data: nil}
	return c.putChunkToRemote(ctx, chunk)
}

func (c *chunkSender) putChunkToRemote(ctx context.Context, chunk *chunk) error {
	if c.e.taskType == taskTypeVectorLoad {
		return c.putVectorChunkToRemote(ctx, chunk)
	}
	// cache chunk data
	if len(chunk.data) != 0 {
		err := c.chunksCache.put(chunk.id, chunk.data)
		if err != nil {
			return err
		}
	}

	url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, chunk.id)

	data, err := sendRequest(ctx, c.httpClient, "PUT", url, chunk.data, c.e.retryCounter)
	if err != nil {
		return err
	}
	result := new(PutChunkResult)
	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}

	err = c.handlePutChunkResult(ctx, result, chunk.id)
	if err != nil {
		return err
	}

	return nil
}

func (c *chunkSender) handlePutChunkResult(ctx context.Context, result *PutChunkResult, expectChunkID uint64) error {
	if result.Canceled {
		c.e.logger.Error("failed to put chunk, task is canceled",
			zap.String("error", result.Error))
		return ErrTaskCanceled
	}

	if result.Finished {
		c.e.logger.Error("failed to put chunk, task is finished",
			zap.String("error", result.Error))
		return nil
	}

	if result.HandledChunkID != expectChunkID {
		c.e.logger.Info("remote worker may restart, retry to put chunk",
			zap.Uint64("expectChunkID", expectChunkID),
			zap.Uint64("handledChunkID", result.HandledChunkID),
			zap.Uint64("flushedChunkID", result.FlushedChunkID),
			zap.String("error", result.Error))

		nextChunkID := result.HandledChunkID + 1
		for nextChunkID <= expectChunkID {
			url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
				c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, nextChunkID)

			buf, err := c.chunksCache.get(nextChunkID)
			if err != nil {
				return err
			}
			c.e.logger.Info("retry to put chunk",
				zap.Uint64("sender", c.id),
				zap.Uint64("chunkID", nextChunkID))

			data, err := sendRequest(ctx, c.httpClient, "PUT", url, buf, c.e.retryCounter)
			if err != nil {
				return err
			}
			result = new(PutChunkResult)
			err = json.Unmarshal(data, result)
			if err != nil {
				return err
			}
			if result.Canceled {
				c.e.logger.Error("failed to put chunk, task is canceled",
					zap.String("error", result.Error))
				return ErrTaskCanceled
			}
			if result.Finished {
				c.e.logger.Error("failed to put chunk, task is finished",
					zap.String("error", result.Error))
				return nil
			}
			nextChunkID = result.HandledChunkID + 1
		}
	}

	state := c.state.Load().(*chunksState)
	lastFlushedChunkID := state.FlushedChunkID + 1
	for lastFlushedChunkID <= result.FlushedChunkID {
		err := c.chunksCache.clean(lastFlushedChunkID)
		if err != nil {
			c.e.logger.Info("failed to clean chunk cache",
				zap.Uint64("chunkID", lastFlushedChunkID),
				zap.Error(err))
		}
		lastFlushedChunkID += 1
	}

	c.state.Store(&chunksState{
		FlushedChunkID: result.FlushedChunkID,
		HandledChunkID: result.HandledChunkID,
	})
	return nil
}

func (c *chunkSender) putVectorChunkToRemote(ctx context.Context, chunk *chunk) error {
	// cache chunk data
	if len(chunk.data) != 0 {
		if err := c.chunksCache.put(chunk.id, chunk.data); err != nil {
			return err
		}
	}

	url := fmt.Sprintf("%s/vector_load?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
		c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, chunk.id)

	resp, err := sendRequestWithRetry(ctx, c.httpClient, "PUT", url, chunk.data, c.e.retryCounter)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		result := new(vectorPutChunkResult)
		if err := json.Unmarshal(body, result); err != nil {
			return err
		}
		return c.handleVectorPutChunkResult(ctx, result, chunk.id)
	case http.StatusConflict:
		expected, ok := parseExpectedChunkIDFromConflictMsg(body)
		if !ok {
			return fmt.Errorf("vector load put chunk conflict: %s", strings.TrimSpace(string(body)))
		}
		if expected == 0 {
			expected = 1
		}
		if expected > chunk.id {
			return fmt.Errorf("vector load put chunk conflict, expected %d > got %d", expected, chunk.id)
		}
		c.e.logger.Info("vector load out-of-order chunk, retry from expected chunk id",
			zap.Uint64("expectChunkID", chunk.id),
			zap.Uint64("expectedChunkID", expected))
		return c.resendVectorChunks(ctx, expected, chunk.id)
	default:
		return fmt.Errorf("failed to send request to remote worker, url: %s, status code: %s, msg: %s",
			url, resp.Status, strings.TrimSpace(string(body)))
	}
}

func (c *chunkSender) handleVectorPutChunkResult(ctx context.Context, result *vectorPutChunkResult, expectChunkID uint64) error {
	if result.Canceled {
		c.e.logger.Error("failed to put vector chunk, task is canceled",
			zap.String("error", result.Error))
		return ErrTaskCanceled
	}
	if result.Finished {
		c.e.logger.Error("failed to put vector chunk, task is finished",
			zap.String("error", result.Error))
		return nil
	}
	if result.Error != "" {
		c.e.logger.Error("failed to put vector chunk",
			zap.String("error", result.Error))
		return fmt.Errorf("failed to put vector chunk: %s", result.Error)
	}

	if result.HandledChunkID != expectChunkID {
		c.e.logger.Info("vector load worker may restart, retry to put chunk",
			zap.Uint64("expectChunkID", expectChunkID),
			zap.Uint64("handledChunkID", result.HandledChunkID),
			zap.String("error", result.Error))

		nextChunkID := result.HandledChunkID + 1
		for nextChunkID <= expectChunkID {
			buf, err := c.chunksCache.get(nextChunkID)
			if err != nil {
				return err
			}
			c.e.logger.Info("retry to put vector chunk",
				zap.Uint64("sender", c.id),
				zap.Uint64("chunkID", nextChunkID))
			if err := c.putVectorChunkToRemote(ctx, &chunk{id: nextChunkID, data: buf}); err != nil {
				return err
			}
			nextChunkID++
		}
		return nil
	}

	state := c.state.Load().(*chunksState)
	c.state.Store(&chunksState{
		FlushedChunkID: state.FlushedChunkID,
		HandledChunkID: result.HandledChunkID,
	})
	return nil
}

func parseExpectedChunkIDFromConflictMsg(body []byte) (uint64, bool) {
	msg := strings.TrimSpace(string(body))
	// Expected error message format: "out-of-order chunk, got X, expected Y"
	idx := strings.LastIndex(msg, "expected ")
	if idx < 0 {
		return 0, false
	}
	s := strings.TrimSpace(msg[idx+len("expected "):])
	// Strip trailing punctuation just in case.
	s = strings.TrimRight(s, ",.")
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (c *chunkSender) resendVectorChunks(ctx context.Context, startChunkID, endChunkID uint64) error {
	if startChunkID == 0 {
		startChunkID = 1
	}
	chunkID := startChunkID
	for chunkID <= endChunkID {
		buf, err := c.chunksCache.get(chunkID)
		if err != nil {
			return err
		}
		url := fmt.Sprintf("%s/vector_load?cluster_id=%d&task_id=%s&writer_id=%d&chunk_id=%d",
			c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, chunkID)
		resp, err := sendRequestWithRetry(ctx, c.httpClient, "PUT", url, buf, c.e.retryCounter)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		switch resp.StatusCode {
		case http.StatusOK:
			result := new(vectorPutChunkResult)
			if err := json.Unmarshal(body, result); err != nil {
				return err
			}
			if err := c.handleVectorPutChunkResult(ctx, result, chunkID); err != nil {
				return err
			}
			chunkID++
		case http.StatusConflict:
			expected, ok := parseExpectedChunkIDFromConflictMsg(body)
			if !ok {
				return fmt.Errorf("vector load put chunk conflict: %s", strings.TrimSpace(string(body)))
			}
			if expected == 0 {
				expected = 1
			}
			if expected > endChunkID {
				// The remote expects a chunk beyond what we want to resend. Stop here and let the
				// caller retry with a newer state.
				return fmt.Errorf("vector load put chunk conflict, expected %d > end %d", expected, endChunkID)
			}
			chunkID = expected
		default:
			return fmt.Errorf("failed to resend vector chunk, url: %s, status code: %s, msg: %s",
				url, resp.Status, strings.TrimSpace(string(body)))
		}
	}
	return nil
}

func (c *chunkSender) sendFlushToRemote(ctx context.Context) error {
	if c.e.taskType == taskTypeVectorLoad {
		return c.sendVectorFlushToRemote(ctx)
	}
	for {
		url := fmt.Sprintf("%s/load_data?cluster_id=%d&task_id=%s&flush=true&writer_id=%d",
			c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id)

		data, err := sendRequest(ctx, c.httpClient, "POST", url, nil, c.e.retryCounter)
		if err != nil {
			c.e.logger.Error("failed to flush", zap.Error(err))
			return err
		}

		result := new(FlushResult)
		err = json.Unmarshal(data, result)
		if err != nil {
			return err
		}

		if result.Canceled || result.Error != "" {
			c.e.logger.Error("failed to flush",
				zap.Bool("canceled", result.Canceled),
				zap.String("error", result.Error))
			return ErrFlushRemoteWorker
		}

		if result.Finished {
			c.e.logger.Info("loadDataTask finished", zap.String("taskID", c.e.loadDataTaskID))
			return nil
		}

		// make sure all chunks are flushed
		flushedChunkID := result.FlushedChunkIDs[c.id]
		if flushedChunkID == c.getLastChunkID() {
			// all chunks are flushed, clean cache and update state
			state := c.state.Load().(*chunksState)
			lastFlushedChunkID := state.FlushedChunkID + 1
			for lastFlushedChunkID <= flushedChunkID {
				c.chunksCache.clean(lastFlushedChunkID)
				lastFlushedChunkID += 1
			}
			state.FlushedChunkID = flushedChunkID

			c.state.Store(state)
			return nil
		}

		// the flushed chunkID is not equal to the last sent chunkID,
		// because the remote worker may restart, retry to put chunk.
		err = c.putEmptyChunk(ctx)
		if err != nil {
			return err
		}
	}
}

func (c *chunkSender) sendVectorFlushToRemote(ctx context.Context) error {
	for {
		lastChunkID := c.getLastChunkID()
		url := fmt.Sprintf("%s/vector_load?cluster_id=%d&task_id=%s&flush=true&writer_id=%d&target_chunk_id=%d",
			c.e.addr, c.e.clusterID, c.e.loadDataTaskID, c.id, lastChunkID)

		data, err := sendRequest(ctx, c.httpClient, "POST", url, nil, c.e.retryCounter)
		if err != nil {
			c.e.logger.Error("failed to flush vector load", zap.Error(err))
			return err
		}

		result := new(vectorFlushResult)
		if err := json.Unmarshal(data, result); err != nil {
			return err
		}

		if result.Canceled {
			c.e.logger.Error("failed to flush vector load, task is canceled",
				zap.String("error", result.Error))
			return ErrTaskCanceled
		}
		if result.Error != "" {
			c.e.logger.Error("failed to flush vector load",
				zap.String("error", result.Error))
			return ErrFlushRemoteWorker
		}
		if result.Finished {
			c.e.logger.Info("vectorLoad task finished", zap.String("taskID", c.e.loadDataTaskID))
			return nil
		}

		// Remote worker may have restarted and lost some received chunks. Resend missing chunks first.
		if result.ReceivedChunkID < lastChunkID {
			if err := c.resendVectorChunks(ctx, result.ReceivedChunkID+1, lastChunkID); err != nil {
				return err
			}
			continue
		}

		// make sure all chunks are ingested
		ingestedChunkID := result.IngestedChunkID
		if ingestedChunkID >= lastChunkID {
			// all chunks are ingested, clean cache and update state
			state := c.state.Load().(*chunksState)
			lastFlushedChunkID := state.FlushedChunkID + 1
			for lastFlushedChunkID <= ingestedChunkID {
				c.chunksCache.clean(lastFlushedChunkID)
				lastFlushedChunkID += 1
			}
			handledChunkID := max(result.ReceivedChunkID, state.HandledChunkID)
			c.state.Store(&chunksState{FlushedChunkID: ingestedChunkID, HandledChunkID: handledChunkID})
			return nil
		}
	}
}

func (c *chunkSender) close() error {
	// notify chunkSenderLoop to quit
	close(c.loopQuitChan)
	c.wg.Wait()

	return c.err
}

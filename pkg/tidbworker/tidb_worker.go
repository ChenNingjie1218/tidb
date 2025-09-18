// Copyright 2023 PingCAP, Inc.
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

package tidbworker

import (
	"context"
	"sync"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

var (
	// GlobalTiDBWorkerManager is the global TiDB worker manager
	GlobalTiDBWorkerManager Manager
	// once is here to make sure the initialization of GlobalTiDBWorkerManager is done only once.
	once sync.Once
)

// Manager is used to manage TiDB worker.
type Manager interface {
	// Role returns the role of the TiDB worker.
	Role() string
	// Meta returns the keyspace meta.
	Meta() *keyspacepb.KeyspaceMeta

	// InitializeGC registers all existing GC tasks to TiDB worker service.
	InitializeGC(ctx context.Context, sctx sessionctx.Context) error
	// RegisterGC notifies the manager that a gc task is registered at ts.
	// Should only be used by user tidb.
	RegisterGC(ctx context.Context, deletionTs uint64) error
	// RecycleGC notifies the manager that all tasks at or before safePoint are finished.
	// Should only be called by worker.
	RecycleGC(ctx context.Context, safePoint uint64) error

	// InitializeGCV2 registers the initial GCV2 task to TiDB worker service, this is used to make sure
	// at least one GCV2 task exists in TiDB worker service.
	InitializeGCV2(ctx context.Context) error
	// AbortGCV2 aborts all the GCV2 tasks in TiDB worker service.
	AbortGCV2(ctx context.Context) error

	// RegisterGCV2 notifies the manager that a round of gc has been performed at gcLastRunTime,
	// with logical timestamp ts.
	RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error
	// RecycleGCV2 notifies the manager that all tasks at or before safePoint are finished.
	RecycleGCV2(ctx context.Context, safePoint uint64) error
	// UpdateGCLifeTime updates the gc life time.
	UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error

	// GetBgTaskConfig gets the background task config.
	GetBgTaskConfig(ctx context.Context, workerType string) (workerCount int, autoScaleEnabled bool, err error)
	// RegisterBgTask notifies the manager that a background sub-task has been created.
	RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error
	// RecycleBgTask notifies the manager that a background task is finished.
	RecycleBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64) error
	// UpdateBgTaskExecID updates the execIDs of the background task.
	UpdateBgTaskExecID(ctx context.Context, gTaskID int64, subtaskIDs []int64, execIDs []string) error

	// RegisterRemoteQuery registers a remote query job to TiDB worker service.
	RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error

	// RegisterTTLTask notifies the manager that a table with TTL information has been created.
	RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error
	// DeleteTTLTableInfo notifies the manager that a table no longer has TTL settings or has been dropped.
	DeleteTTLTableInfo(ctx context.Context, tableID int64) error
	// RecycleTTLTask notifies the manager that a TTL task has been completed.
	RecycleTTLTask(ctx context.Context, finishTime uint64) error
	// UpdateTTLJobEnable notifies the manager that the user's 'tidb_ttl_job_enable' variable has been updated.
	UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error

	// RegisterAutoAnalyze notifies the manager that an auto-analyze task has been registered.
	RegisterAutoAnalyze(ctx context.Context, taskID uint64) error
	// RecycleAutoAnalyze notifies the manager that an auto-analyze task has been completed.
	RecycleAutoAnalyze(ctx context.Context, taskID uint64) error
}

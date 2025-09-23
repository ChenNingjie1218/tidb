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
	"crypto/tls"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	workercliV2 "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/clientv2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ Manager = &manager{}

type manager struct {
	clientV2 workercliV2.Client
	role     string
	meta     *keyspacepb.KeyspaceMeta

	// TODO: Remove after clientv2 merge
	gcClientV2          bool
	gcV2ClientV2        bool
	bgTaskClientV2      bool
	ttlClientV2         bool
	autoAnalyzeClientV2 bool
}

const (
	// WorkerTypeDDL is the type of ddl background task.
	WorkerTypeDDL = "ddl"
	// WorkerTypeImportInto is the type of import into background task.
	WorkerTypeImportInto = "import-into"
	// WorkerTypeBatch is the type of batch background task.
	WorkerTypeBatch = "batch"
	// WorkerTypeGCV2 is the type of GCV2 background task.
	WorkerTypeGCV2 = "gcv2"
	// WorkerTypeGC is the type of GC background task.
	WorkerTypeGC = "gc"
	// WorkerTypeRemoteQuery is the type of remote query task.
	WorkerTypeRemoteQuery = "remote-query"
	// WorkerTypeTTL is the type of TTL background task.
	WorkerTypeTTL = "ttl"
	// WorkerTypeAutoAnalyze is the type of auto analyze background task.
	WorkerTypeAutoAnalyze = "auto-analyze"

	defGCLifeTimeSec = 600
)

// TaskWorkerType converts the task type in global to the type of TiDB worker.
func TaskWorkerType(taskType string) string {
	switch taskType {
	case proto.Backfill.String():
		return WorkerTypeDDL
	case proto.Batch.String():
		return WorkerTypeBatch
	case proto.ImportInto.String():
		return WorkerTypeImportInto
	default:
		return ""
	}
}

// InitManager initialize the global TiDB worker manager with the given backend.
func InitManager(ctx context.Context, keyspaceMeta *keyspacepb.KeyspaceMeta, cfg config.TiDBWorker) (err error) {
	once.Do(func() {
		if cfg.LocalMode.Enable {
			GlobalTiDBWorkerManager = &manager{
				clientV2: &localClient{},
				role:     cfg.Role,
				meta:     keyspaceMeta,
			}
			return
		}
		// Setup TLS if necessary.
		var tlsConfig *tls.Config
		security := config.GetGlobalConfig().Security
		if len(security.ClusterSSLCA) > 0 {
			clusterSecurity := security.ClusterSecurity()
			tlsConfig, err = clusterSecurity.ToTLSConfig()
			if err != nil {
				log.Error("[tidb-worker] failed to create tls config", zap.Error(err))
				return
			}
		}
		var clientV2 workercliV2.Client
		clientOption := &workercliV2.Option{
			KeyspaceName: keyspaceMeta.GetName(),
			KeyspaceID:   keyspaceMeta.GetId(),
			TiDBPool:     cfg.TidbPool,
			TLSConfig:    tlsConfig,
			ScalerAddr:   cfg.APIServerAddr,
		}
		// GRPC requires a transport credentials, even if it's insecure.
		// If the tls config is correctly setup, it will overwrite the default inside NewClientWithContext.
		dialOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
		dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		clientV2, err = workercliV2.NewClientWithContext(dialCtx, clientOption, dialOpt)
		if err != nil {
			log.Error("[tidb-worker] failed to create tidb worker client", zap.Error(err))
			return
		}
		err = clientV2.Ping(dialCtx)
		if err != nil {
			log.Error("[tidb-worker] failed to connect to tidb worker service", zap.Error(err))
			return
		}
		GlobalTiDBWorkerManager = &manager{
			clientV2: clientV2,
			role:     cfg.Role,
			meta:     keyspaceMeta,
		}
	})

	return err
}

func (m *manager) Role() string {
	return m.role
}

func (m *manager) Meta() *keyspacepb.KeyspaceMeta {
	return m.meta
}

func (m *manager) InitializeGC(ctx context.Context, sctx sessionctx.Context) error {
	log.Info("[tidb-worker] initialize GC tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.InitializeWorkerTasks, "").Inc()
	tasks, err := ddlutil.LoadDeleteRanges(ctx, sctx, math.MaxUint64)
	if err != nil {
		return errors.Trace(err)
	}
	for _, task := range tasks {
		err := m.RegisterGC(ctx, task.Ts)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *manager) RegisterGC(ctx context.Context, deletionTs uint64) error {
	log.Info("[tidb-worker] register a GC task to worker service", zap.Uint64("ts", deletionTs))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.RegisterWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RegisterGC(ctx, deletionTs))
}

func (m *manager) RecycleGC(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] notify worker service to recycle a GC task", zap.Uint64("safe-point", safePoint))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGC, metrics.RecycleWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RecycleGC(ctx, safePoint))
}

func (m *manager) InitializeGCV2(ctx context.Context) error {
	log.Info("[tidb-worker] initialize GCV2 tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.InitializeWorkerTasks, "").Inc()
	// Use 0 as the timestamp to make sure this task can be cleaned by the completion of any other GCV2 task.
	return errors.Trace(m.RegisterGCV2(ctx, 0, defGCLifeTimeSec))
}

func (m *manager) AbortGCV2(ctx context.Context) error {
	log.Info("[tidb-worker] abort all GCV2 tasks")
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.AbortWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RecycleGCV2(ctx, math.MaxUint64))
}

func (m *manager) RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error {
	log.Info("[tidb-worker] register a GCV2 task to worker service",
		zap.Uint64("safe-point", safePoint),
		zap.Int64("gc-life-time", gcLifeTime),
	)
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.RegisterWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RegisterGCV2(ctx, safePoint, gcLifeTime))
}

func (m *manager) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	log.Info("[tidb-worker] recycle a GCV2 task to worker service", zap.Uint64("safe-point", safePoint))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeGCV2, metrics.RecycleWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RecycleGCV2(ctx, safePoint))
}

func (m *manager) UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error {
	log.Info("[tidb-worker] update GC life time", zap.Int64("gc-life-time", gcLifeTime))
	return errors.Trace(m.clientV2.UpdateGCLifeTime(ctx, gcLifeTime))
}

func (m *manager) GetBgTaskConfig(ctx context.Context, workerType string) (workerCount int, autoScaleEnabled bool, err error) {
	log.Info("[tidb-worker] get background task config", zap.String("worker-type", workerType))
	workerCount, autoScaleEnabled, err = m.clientV2.GetBgTaskConfig(ctx, workerType)
	return workerCount, autoScaleEnabled, errors.Trace(err)
}

func (m *manager) RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error {
	log.Info("[tidb-worker] register a background task to worker service",
		zap.String("task-key", taskKey),
		zap.String("task-type", taskType),
		zap.Int64("global-task-id", gTaskID),
		zap.Int64("subtask-id", subTaskID),
		zap.String("exec-id", execID),
	)
	metrics.WorkerTaskCounter.WithLabelValues(taskType, metrics.RegisterWorkerTask, taskKey).Inc()
	return errors.Trace(m.clientV2.RegisterBgTask(ctx, taskType, taskKey, gTaskID, subTaskID, execID))
}

func (m *manager) RecycleBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64) error {
	log.Info("[tidb-worker] recycle a background task from worker service", zap.Int64("global-task-id", gTaskID), zap.Int64("subtask-id", subTaskID))
	if taskKey != "" { // only record global task.
		metrics.WorkerTaskCounter.WithLabelValues(taskType, metrics.RecycleWorkerTask, taskKey).Inc()
	}
	return errors.Trace(m.clientV2.RecycleBgTask(ctx, gTaskID, subTaskID))
}

func (m *manager) UpdateBgTaskExecID(ctx context.Context, gTaskID int64, subtaskIDs []int64, execIDs []string) error {
	log.Info("[tidb-worker] update background task exec IDs", zap.Int64("global-task-id", gTaskID), zap.Strings("exec-ids", execIDs))
	return errors.Trace(m.clientV2.UpdateBgTaskExecID(ctx, gTaskID, subtaskIDs, execIDs))
}

func (m *manager) RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error {
	log.Info("[tidb-worker] register a remote query to worker service", zap.String("query-id", queryID))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeRemoteQuery, metrics.RegisterWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RegisterRemoteQuery(ctx, queryID, queryAddr))
}

func (m *manager) RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error {
	log.Info("[tidb-worker] register a TTL task to worker service",
		zap.Int64("table-id", tableID),
		zap.Bool("ttl-job-enable", ttlJobEnable),
	)
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeTTL, metrics.RegisterWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RegisterTTLTask(ctx, tableID, ttlJobEnable))
}

func (m *manager) DeleteTTLTableInfo(ctx context.Context, tableID int64) error {
	log.Info("[tidb-worker] delete TTL table info from worker service", zap.Int64("table-id", tableID))
	return errors.Trace(m.clientV2.DeleteTTLTableInfo(ctx, tableID))
}

func (m *manager) RecycleTTLTask(ctx context.Context, finishTime uint64) error {
	log.Info("[tidb-worker] recycle a TTL task from worker service", zap.Uint64("finish-time", finishTime))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeTTL, metrics.RecycleWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RecycleTTLTask(ctx, finishTime))
}

func (m *manager) UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error {
	log.Info("[tidb-worker] update TTL job enable", zap.Bool("ttl-job-enable", ttlJobEnable))
	return errors.Trace(m.clientV2.UpdateTTLJobEnable(ctx, ttlJobEnable))
}

func (m *manager) RegisterAutoAnalyze(ctx context.Context, taskID uint64) error {
	log.Info("[tidb-worker] register an auto analyze task to worker service", zap.Uint64("task-id", taskID))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeAutoAnalyze, metrics.RegisterWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RegisterAutoAnalyze(ctx, taskID))
}

func (m *manager) RecycleAutoAnalyze(ctx context.Context, taskID uint64) error {
	log.Info("[tidb-worker] recycle an auto analyze task from worker service", zap.Uint64("task-id", taskID))
	metrics.WorkerTaskCounter.WithLabelValues(WorkerTypeAutoAnalyze, metrics.RecycleWorkerTask, "").Inc()
	return errors.Trace(m.clientV2.RecycleAutoAnalyze(ctx, taskID))
}

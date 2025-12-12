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
	"errors"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/util/logutil"
	workercliV2 "github.com/tidbcloud/aws-shared-provider/pkg/tidbworker/clientv2"
	"go.uber.org/zap"
)

const (
	workerIDPrefix = "tidb-worker-"
)

// IsMaster returns whether the current TiDB is of role master.
func IsMaster() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleMaster
}

// IsBgTaskEnabled returns whether the current TiDB has the specified bg task enabled.
func IsBgTaskEnabled(ctx context.Context, taskType string) bool {
	if GlobalTiDBWorkerManager == nil {
		return false
	}
	_, _, err := loadBgTaskConfig(ctx, TaskWorkerType(taskType))
	if err != nil && !errors.Is(err, workercliV2.ErrHandlerPaused) {
		logutil.BgLogger().Warn("[tidb-worker] failed to load worker config", zap.Error(err))
		return false
	}
	return err == nil
}

// IsGCWorker returns whether the current TiDB is a GC worker.
func IsGCWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCWorker
}

// IsBatchWorker returns whether the current TiDB is a batch worker.
func IsBatchWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleBatchWorker
}

// IsSharedWorker returns whether the current TiDB is a shared worker.
func IsSharedWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleSharedWorker
}

// IsDDLWorker returns whether the current TiDB is a DDL worker.
func IsDDLWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleDDLWorker
}

// IsImportIntoWorker returns whether the current TiDB is an import into worker.
func IsImportIntoWorker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleImportIntoWorker
}

// IsGCV2Worker returns whether the current TiDB is a GCV2 worker.
func IsGCV2Worker() bool {
	return GlobalTiDBWorkerManager != nil && GlobalTiDBWorkerManager.Role() == config.RoleGCV2Worker
}

// UseKeyspaceLevelGC returns whether the current TiDB uses keyspace level GC.
func UseKeyspaceLevelGC() bool {
	return GlobalTiDBWorkerManager != nil && keyspace.IsKeyspaceUseKeyspaceLevelGC(GlobalTiDBWorkerManager.Meta())
}

// IsTTLTaskWorker returns whether the current TiDB is a TTL worker.
func IsTTLTaskWorker() bool {
	if GlobalTiDBWorkerManager == nil {
		return false
	}
	if GlobalTiDBWorkerManager.Role() == config.RoleTTLTaskWorker {
		return true
	}
	if GlobalTiDBWorkerManager.Role() == config.RoleSharedWorker {
		if _, enabled, err := loadBgTaskConfig(context.TODO(), WorkerTypeTTL); enabled && err == nil {
			return true
		}
	}
	return false
}

// IsAutoAnalyzeWorker returns whether the current TiDB is an auto analyze worker.
func IsAutoAnalyzeWorker() bool {
	if GlobalTiDBWorkerManager == nil {
		return false
	}
	if GlobalTiDBWorkerManager.Role() == config.RoleAutoAnalyzeWorker {
		return true
	}
	if GlobalTiDBWorkerManager.Role() == config.RoleSharedWorker {
		if _, enabled, err := loadBgTaskConfig(context.TODO(), WorkerTypeAutoAnalyze); enabled && err == nil {
			return true
		}
	}
	return false
}

// IsUseTiDBWorker returns whether the current TiDB use TiDB worker.
func IsUseTiDBWorker() bool {
	return GlobalTiDBWorkerManager != nil
}

// SchedulerNodes generate scheduler nodes according to tidb worker config instead of current
// cluster topology.
func SchedulerNodes(ctx context.Context, workerType string, gTaskID int64) []*infosync.ServerInfo {
	if GlobalTiDBWorkerManager == nil {
		return nil
	}
	workerCount, autoScalerEnabled, err := loadBgTaskConfig(ctx, TaskWorkerType(workerType))
	if err != nil {
		logutil.BgLogger().Warn("[tidb-worker] failed to load worker config", zap.Error(err))
		return nil
	}
	if workerCount == 0 {
		return nil
	}
	nodeIP := workerIDPrefix + workerType
	// If not in local mode, append the global task ID to the node IP.
	if !config.GetGlobalConfig().TiDBWorker.LocalMode.Enable || !config.GetGlobalConfig().TiDBWorker.LocalMode.StaticExecID {
		nodeIP += "-" + strconv.FormatInt(gTaskID, 10)
	}
	if autoScalerEnabled {
		nodeIP = workerIDPrefix + "shared" // if auto scale is enabled, use shared worker as the node IP.
	}
	nodes := make([]*infosync.ServerInfo, workerCount)
	for i := range workerCount {
		nodes[i] = &infosync.ServerInfo{
			StaticServerInfo: infosync.StaticServerInfo{
				IP:   nodeIP,
				Port: uint(i),
			},
		}
	}
	return nodes
}

// IsWorkerExecID checks whether the execID belongs to a tidb worker.
func IsWorkerExecID(execID, workerType string) bool {
	return strings.HasPrefix(execID, workerIDPrefix+workerType+"-") ||
		strings.HasPrefix(execID, workerIDPrefix+"shared-")
}

func loadBgTaskConfig(ctx context.Context, workerType string) (int, bool, error) {
	if config.GetGlobalConfig().TiDBWorker.LocalMode.Enable {
		cfg, ok := config.GetGlobalConfig().TiDBWorker.LocalMode.BgTaskConfig[workerType]
		if !ok || cfg.Paused {
			return 0, false, workercliV2.ErrHandlerPaused
		}
		return cfg.WorkerCount, cfg.EnableAutoScale, nil
	}
	return GlobalTiDBWorkerManager.GetBgTaskConfig(ctx, workerType)
}

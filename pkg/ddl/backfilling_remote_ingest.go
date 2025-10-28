// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tidbworker"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type remoteIngestExecutor struct {
	taskexecutor.EmptyStepExecutor

	indexes []*model.IndexInfo
	ptbl    table.PhysicalTable
	ddlObj  *ddl
	job     *model.Job

	bc ingest.BackendCtx
}

func newRemoteIngestExecutor(
	ctx context.Context,
	ddlObj *ddl,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	bcGetter func(context.Context) (ingest.BackendCtx, error),
) (r *remoteIngestExecutor, err error) {
	r = &remoteIngestExecutor{
		ddlObj:  ddlObj,
		job:     job,
		indexes: indexes,
		ptbl:    ptbl,
	}

	r.bc, err = bcGetter(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *remoteIngestExecutor) Init(_ context.Context) error {
	logutil.BgLogger().Info("remote ingest executor init subtask exec env",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
	return nil
}

func (r *remoteIngestExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.BgLogger().Info("remote ingest executor run subtask",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))

	indexIDs := make([]int64, 0, len(r.indexes))
	uniques := make([]bool, 0, len(r.indexes))
	for _, index := range r.indexes {
		indexIDs = append(indexIDs, index.ID)
		uniques = append(uniques, index.Unique)
	}

	keyspaceID := r.ddlObj.store.GetCodec().GetKeyspaceID()
	_, err := r.bc.Register(indexIDs, uniques, r.job.ID, r.job.EstimatedTableDataSize, uint32(keyspaceID), r.ptbl)
	if err != nil {
		logutil.BgLogger().Error("cannot register engines",
			zap.Error(err),
			zap.Int64("job ID", r.job.ID),
			zap.Int64s("index IDs", indexIDs))
		return err
	}

	return r.bc.FinishAndImport(ingest.OptCheckDup)
}

func (r *remoteIngestExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("remote ingest executor cleanup subtask exec env",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
	select {
	case <-ctx.Done():
		if context.Cause(ctx) != taskexecutor.ErrCancelSubtask {
			return ctx.Err()
		}
	default:
	}
	err := r.bc.Cleanup()
	if err != nil {
		logutil.Logger(ctx).Info("failed to clean up remote backend engine",
			zap.String("category", "ddl"),
			zap.Int64("jobID", r.job.ID))
	}
	return nil
}

func (r *remoteIngestExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("remote ingest executor cleanup subtask exec env",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))

	if variable.EnableDistTask.Load() && tidbworker.IsBgTaskEnabled(ctx, string(subtask.Type)) {
		err := tidbworker.GlobalTiDBWorkerManager.RecycleBgTask(
			ctx, tidbworker.TaskWorkerType(string(subtask.Type)),
			"",
			subtask.TaskID,
			subtask.ID,
		)
		if err != nil {
			logutil.Logger(ctx).Error("tidb worker manager failed to recycle subtask", zap.Error(err))
		}
	}
	return nil
}

func (r *remoteIngestExecutor) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("remote ingest executor rollback backfill add index task",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
	return nil
}

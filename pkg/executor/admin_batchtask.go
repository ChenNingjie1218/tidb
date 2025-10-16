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

package executor

import (
	"context"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// AdminShowBatchTaskExec is an executor for ADMIN SHOW BATCHTASK.
type AdminShowBatchTaskExec struct {
	exec.BaseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminShowBatchTaskExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	exec := e.Ctx().(sqlexec.RestrictedSQLExecutor)

	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT id,task_key,state,start_time,state_update_time,error FROM mysql.tidb_global_task WHERE type="batch"`)
	if err != nil {
		return errors.Trace(err)
	}
	req.AppendRows(rows)
	return nil
}

// AdminCancelBatchTaskExec is an executor for ADMIN CANCEL BATCHTASK.
type AdminCancelBatchTaskExec struct {
	taskIDs []int64
	exec.BaseExecutor
	done bool
}

// Next implements the Executor Next interface.
func (e *AdminCancelBatchTaskExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	if len(e.taskIDs) == 0 {
		return errors.New("No task id is specified")
	}
	restrictedCtx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	defer e.ReleaseSysSession(internalCtx, restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.SQLExecutor)

	var sb strings.Builder
	sb.WriteString(`UPDATE mysql.tidb_global_task SET state="cancelling" WHERE state IN ("waiting","pending","running") and id IN (`)
	for i, id := range e.taskIDs {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(strconv.FormatInt(id, 10))
	}
	sb.WriteString(")")

	if _, err := sqlExecutor.ExecuteInternal(internalCtx, sb.String()); err != nil {
		return err
	}
	return nil
}

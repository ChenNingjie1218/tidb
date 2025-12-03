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

package autoanalyze

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/tidbworker"
	"go.uber.org/zap"
)

// autoAnalyzeRegInterval is the minimum interval between two auto analyze registrations.
const (
	autoAnalyzeRegInterval = 15 * time.Minute
	insertJob              = "INSERT INTO mysql.auto_analyze_tasks(table_id, start_time) VALUES(%?, %?)"
	getJobID               = "SELECT LAST_INSERT_ID()"
	getJob                 = "SELECT id, table_id, start_time FROM mysql.auto_analyze_tasks ORDER BY RAND() LIMIT 1"
	getJobWithTableID      = "SELECT id, table_id, start_time FROM mysql.auto_analyze_tasks WHERE table_id = %?"
	deleteJob              = "DELETE FROM mysql.auto_analyze_tasks where id = %?"
	insertJobHistory       = "INSERT INTO mysql.auto_analyze_tasks_history(id, table_id, analyzed, " +
		"statement, err, start_time, end_time) VALUES (%?, %?, %?, %?, %?, %?, %?)"
)

// statsAutoAnalyze is used to handle auto-analyze-worker
type statAutoAnalyzeWorker struct {
	sync.Mutex
	statsHandle statstypes.StatsHandle

	autoAnalyzeTaskRegistration sync.Map
	lastExecutedAutoAnalyzeSQL  string
	lastAutoAnalyzeSuccessful   bool
}

// NewStatAutoAnalyzeWorker creates a new statsAutoAnalyzeWorker.
func NewStatAutoAnalyzeWorker(statsHandle statstypes.StatsHandle) statstypes.StatsAutoAnalyzeWorker {
	return &statAutoAnalyzeWorker{statsHandle: statsHandle}
}

func (sa *statAutoAnalyzeWorker) RegisterAutoAnalyzeTask(tableID int64, reason string) (err error) {
	now := time.Now()
	value, ok := sa.autoAnalyzeTaskRegistration.Load(tableID)
	if ok {
		lastRegistered, ok := value.(time.Time)
		if ok && now.Sub(lastRegistered) < autoAnalyzeRegInterval {
			statslogutil.StatsLogger().Debug("skip registering an auto analyze",
				zap.Int64("table-id", tableID),
				zap.Duration("min-registration-interval", autoAnalyzeRegInterval),
				zap.Time("last-registered", lastRegistered),
				zap.Time("now", now),
			)
			return nil
		}
	}
	var (
		taskID             uint64
		registeredToWorker bool
		existingTask       *statistics.AutoAnalyzeTask
	)
	err = statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		// Check if task with target table ID already exists.
		existingTask, err = getAutoAnalyzeTaskWithTableID(sctx, tableID)
		if err != nil {
			return err
		}
		if existingTask != nil {
			return nil
		}
		// Register task to system table.
		taskID, err = insertAutoAnalyzeJob(sctx, tableID, now.Unix())
		if err != nil {
			return err
		}
		// Register task to tidb worker.
		err = tidbworker.GlobalTiDBWorkerManager.RegisterAutoAnalyze(context.Background(), taskID)
		if err != nil {
			return errors.Trace(err)
		}
		registeredToWorker = true
		return nil
	}, statsutil.FlagWrapTxn)
	if err != nil {
		if registeredToWorker {
			// If the task was registered to the worker but failed to insert into the job table,
			// need to recycle the task registration.
			rollbackErr := tidbworker.GlobalTiDBWorkerManager.RecycleAutoAnalyze(context.Background(), taskID)
			if rollbackErr != nil {
				statslogutil.StatsLogger().Warn("failed to rollback auto analyze task registration",
					zap.Uint64("task-id", taskID),
					zap.Int64("table-id", tableID),
					zap.Error(rollbackErr),
				)
			}
		}
		return errors.Trace(err)
	}
	if existingTask != nil {
		statslogutil.StatsLogger().Info("skip registering an auto analyze task, task already exists",
			zap.Int64("table-id", tableID),
			zap.Uint64("task-id", existingTask.ID),
			zap.Int64("start-time", existingTask.StartTime),
			zap.String("reason", reason),
		)
		sa.autoAnalyzeTaskRegistration.Store(tableID, now)
		return nil
	}
	sa.autoAnalyzeTaskRegistration.Store(tableID, now)
	statslogutil.StatsLogger().Info("registered an auto analyze task",
		zap.Int64("table-id", tableID),
		zap.Uint64("task-id", taskID),
		zap.String("reason", reason),
	)
	return nil
}

func getAutoAnalyzeTaskWithTableID(sctx sessionctx.Context, tableID int64) (*statistics.AutoAnalyzeTask, error) {
	rows, _, err := statsutil.ExecRows(sctx, getJobWithTableID, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &statistics.AutoAnalyzeTask{
		ID:        rows[0].GetUint64(0),
		TableID:   rows[0].GetInt64(1),
		StartTime: rows[0].GetInt64(2),
	}, nil
}

func insertAutoAnalyzeJob(sctx sessionctx.Context, tableID int64, startTime int64) (uint64, error) {
	_, _, err := statsutil.ExecRows(sctx, insertJob, tableID, startTime)
	if err != nil {
		return 0, errors.Trace(err)
	}
	rows, _, err := statsutil.ExecRows(sctx, getJobID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return rows[0].GetUint64(0), nil
}

func (sa *statAutoAnalyzeWorker) GetAutoAnalyzeTask() (*statistics.AutoAnalyzeTask, error) {
	var (
		task *statistics.AutoAnalyzeTask
		err  error
	)
	err = statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		task, err = getAutoAnalyzeTask(sctx)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return task, nil
}

func getAutoAnalyzeTask(sctx sessionctx.Context) (*statistics.AutoAnalyzeTask, error) {
	rows, _, err := statsutil.ExecRows(sctx, getJob)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &statistics.AutoAnalyzeTask{
		ID:        rows[0].GetUint64(0),
		TableID:   rows[0].GetInt64(1),
		StartTime: rows[0].GetInt64(2),
	}, nil
}

func (sa *statAutoAnalyzeWorker) FinishAutoAnalyzeTask(task *statistics.AutoAnalyzeTask) error {

	err := tidbworker.GlobalTiDBWorkerManager.RecycleAutoAnalyze(context.Background(), task.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = statsutil.CallWithSCtx(sa.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return finishAutoAnalyzeTask(sctx, task)
	})
	if err != nil {
		return errors.Trace(err)
	}
	statslogutil.StatsLogger().Info("finished an auto analyze task",
		zap.Uint64("task-id", task.ID),
		zap.Int64("table-id", task.TableID),
	)
	return nil
}

func finishAutoAnalyzeTask(sctx sessionctx.Context, task *statistics.AutoAnalyzeTask) error {
	_, _, err := statsutil.ExecRows(sctx, deleteJob, task.ID)
	if err != nil {
		return errors.Trace(err)
	}
	_, _, err = statsutil.ExecRows(sctx, insertJobHistory,
		task.ID, task.TableID, task.Analyzed, task.Statement, task.Err, task.StartTime, time.Now().Unix())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// UpdateLastExecution record the last execution status of auto analyze
func (sa *statAutoAnalyzeWorker) UpdateLastExecution(success bool, sql string) {
	sa.Lock()
	defer sa.Unlock()
	sa.lastAutoAnalyzeSuccessful = success
	sa.lastExecutedAutoAnalyzeSQL = sql
}

// GetLastExecution returns the last execution status of auto analyze
func (sa *statAutoAnalyzeWorker) GetLastExecution() (success bool, sql string) {
	sa.Lock()
	defer sa.Unlock()
	return sa.lastAutoAnalyzeSuccessful, sa.lastExecutedAutoAnalyzeSQL
}

// loadAutoAnalyzeTaskAndExecute loads one auto analyze task from mysql.auto_analyze_tasks and executes it.
func loadAutoAnalyzeTaskAndExecute(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	sysProcTracker sysproctrack.Tracker,
	is infoschema.InfoSchema,
	autoAnalyzeRatio float64,
	pruneMode variable.PartitionPruneMode,
	lockedTables map[int64]struct{},
) bool {
	statsHandle.UpdateLastExecution(false, "")

	task, err := statsHandle.GetAutoAnalyzeTask()
	if err != nil {
		statslogutil.StatsLogger().Error("[stats] get auto analyze task failed", zap.Error(err))
		return false
	}
	// No more tasks, simply return and wait for next tick.
	if task == nil {
		return false
	}
	tbl, exist := is.TableByID(context.Background(), task.TableID)
	// Table not exist, record error, cleanup and return.
	if !exist {
		statslogutil.StatsLogger().Error("[stats] auto analyze table not found", zap.Int64("tableID", task.TableID))
		task.Err = "table not found"
		err = statsHandle.FinishAutoAnalyzeTask(task)
		if err != nil {
			statslogutil.StatsLogger().Error("[stats] finish auto analyze task failed", zap.Error(err))
		}
		return false
	}
	tblInfo := tbl.Meta()
	dbInfo, exist := is.SchemaByID(tblInfo.DBID)
	if !exist {
		statslogutil.StatsLogger().Error("auto analyze database not found", zap.Int64("tableID", task.TableID))
		task.Err = "database not found"
		err = statsHandle.FinishAutoAnalyzeTask(task)
		if err != nil {
			statslogutil.StatsLogger().Error("finish auto analyze task failed", zap.Error(err))
		}
		return false
	}

	// If table locked, wait for next tick.
	if _, ok := lockedTables[tblInfo.ID]; ok {
		statslogutil.StatsLogger().Warn("table is locked, wait for next tick", zap.Int64("tableID", tblInfo.ID))
		return false
	}

	var analyzed bool
	if checkAndRunAnalyze(sctx, statsHandle, sysProcTracker, tblInfo, dbInfo.Name.O, pruneMode, lockedTables, autoAnalyzeRatio) {
		task.Analyzed, task.Statement = statsHandle.GetLastExecution()
		analyzed = true
	}
	if err = statsHandle.FinishAutoAnalyzeTask(task); err != nil {
		statslogutil.StatsLogger().Error("finish auto analyze task failed", zap.Error(err))
	}
	return analyzed
}

// checkAndRegisterAutoAnalyze checks if the table needs to be analyzed and register the auto analyze task.
func checkAndRegisterAutoAnalyze(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	autoAnalyzeRatio float64,
) (bool, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		statsTbl := statsHandle.GetTableStats(tblInfo)
		if needAnalyze, reason := checkNeedAnalyze(tblInfo, statsTbl, autoAnalyzeRatio); needAnalyze {
			return true, statsHandle.RegisterAutoAnalyzeTask(tblInfo.ID, reason)
		}
		return false, nil
	}
	for _, def := range pi.Definitions {
		statsTbl := statsHandle.GetPartitionStats(tblInfo, def.ID)
		if needAnalyze, reason := checkNeedAnalyze(tblInfo, statsTbl, autoAnalyzeRatio); needAnalyze {
			return true, statsHandle.RegisterAutoAnalyzeTask(tblInfo.ID, reason)
		}
	}
	return false, nil
}

// checkNeedAnalyze checks if analyze is needed for the target table.
func checkNeedAnalyze(
	tblInfo *model.TableInfo,
	statsTbl *statistics.Table,
	autoAnalyzeRatio float64,
) (bool, string) {
	if statsTbl.Pseudo || statsTbl.RealtimeCount < statistics.AutoAnalyzeMinCnt {
		return false, ""
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, autoAnalyzeRatio); needAnalyze {
		return true, reason
	}
	for _, idx := range tblInfo.Indices {
		if idxStats := statsTbl.GetIdx(idx.ID); idxStats != nil && !idxStats.IsAnalyzed() {
			return true, "new index"
		}
	}
	return false, ""
}

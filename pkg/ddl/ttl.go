// Copyright 2022 PingCAP, Inc.
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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tidbworker"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// DefaultTTLJobInterval is the default value for ttl job interval.
const DefaultTTLJobInterval = "1h"

func onTTLInfoRemove(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo.TTLInfo = nil
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	//-------------- Serverless TiDB Worker ------------
	if tidbworker.IsMaster() {
		// tidb worker:alter table remove ttl
		tidbworker.GlobalTiDBWorkerManager.DeleteTTLTableInfo(jobCtx.ctx, tblInfo.ID)
	}
	//-------------- Serverless TiDB Worker end------------
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onTTLInfoChange(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// at least one for them is not nil
	args, err := model.GetAlterTTLInfoArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ttlInfo, ttlInfoEnable, ttlInfoJobInterval := args.TTLInfo, args.TTLEnable, args.TTLCronJobSchedule

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if ttlInfo != nil {
		// if the TTL_ENABLE is not set explicitly, use the original value
		if ttlInfoEnable == nil && tblInfo.TTLInfo != nil {
			ttlInfo.Enable = tblInfo.TTLInfo.Enable
		}
		if ttlInfoJobInterval == nil && tblInfo.TTLInfo != nil {
			ttlInfo.JobInterval = tblInfo.TTLInfo.JobInterval
		}
		tblInfo.TTLInfo = ttlInfo
	}
	if ttlInfoEnable != nil {
		if tblInfo.TTLInfo == nil {
			return ver, errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_ENABLE"))
		}

		tblInfo.TTLInfo.Enable = *ttlInfoEnable
	}
	if ttlInfoJobInterval != nil {
		if tblInfo.TTLInfo == nil {
			return ver, errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_JOB_INTERVAL"))
		}

		tblInfo.TTLInfo.JobInterval = *ttlInfoJobInterval
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Serverless TiDB worker
	// Register or Unregister the TTL task according to the new TTL settings.
	if tidbworker.IsMaster() && tblInfo != nil {
		ttlInfo := tblInfo.TTLInfo
		if ttlInfo != nil && ttlInfo.Enable {
			err := tidbworker.GlobalTiDBWorkerManager.RegisterTTLTask(
				jobCtx.ctx,
				tblInfo.ID,
				variable.EnableTTLJob.Load(),
			)
			if err != nil {
				logutil.DDLLogger().Error("Failed to register TTL task",
					zap.Int64("tableID", tblInfo.ID),
					zap.Error(err))
			}
		}
	}
	//--------- Serverless TiDB worker end ---------

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

// checkTTLInfoValid checks the TTL settings for a table.
// The argument `isForForeignKeyCheck` is used to check the table should not be referenced by foreign key.
// If `isForForeignKeyCheck` is `nil`, it will skip the foreign key check.
func checkTTLInfoValid(schema pmodel.CIStr, tblInfo *model.TableInfo, foreignKeyCheckIs infoschemactx.MetaOnlyInfoSchema) error {
	if !intest.InTest && !config.GetGlobalConfig().EnableSetTableTTL {
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("TTL")
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrTempTableNotAllowedWithTTL
	}

	if err := checkTTLIntervalExpr(tblInfo.TTLInfo); err != nil {
		return err
	}

	if err := checkPrimaryKeyForTTLTable(tblInfo); err != nil {
		return err
	}

	if foreignKeyCheckIs != nil {
		// checks even when the foreign key check is not enabled, to keep safe
		if referredFK := checkTableHasForeignKeyReferred(foreignKeyCheckIs, schema.L, tblInfo.Name.L, nil, true); referredFK != nil {
			return dbterror.ErrUnsupportedTTLReferencedByFK
		}
	}

	return checkTTLInfoColumnType(tblInfo)
}

func checkTTLJobInterval(_ *string) error {
	// todo(ystaticy): fix test case
	//if jobInterval != nil && *jobInterval != DefaultTTLJobInterval && !intest.InTest && config.GetGlobalConfig().EnableSetTableTTL {
	//	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("update ttl job interval")
	//}
	return nil
}

func checkTTLIntervalExpr(ttlInfo *model.TTLInfo) error {
	_, err := cache.EvalExpireTime(time.Now(), ttlInfo.IntervalExprStr, ast.TimeUnitType(ttlInfo.IntervalTimeUnit))
	return errors.Trace(err)
}

func checkTTLInfoColumnType(tblInfo *model.TableInfo) error {
	colInfo := findColumnByName(tblInfo.TTLInfo.ColumnName.L, tblInfo)
	if colInfo == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(tblInfo.TTLInfo.ColumnName.O, "TTL config")
	}
	if !types.IsTypeTime(colInfo.FieldType.GetType()) {
		return dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(tblInfo.TTLInfo.ColumnName.O)
	}

	return nil
}

func checkDropColumnWithTTLConfig(tblInfo *model.TableInfo, colName string) error {
	if tblInfo.TTLInfo != nil {
		if tblInfo.TTLInfo.ColumnName.L == colName {
			return dbterror.ErrTTLColumnCannotDrop.GenWithStackByArgs(colName)
		}
	}

	return nil
}

// We should forbid creating a TTL table with clustered primary key that contains a column with type float/double.
// This is because currently we are using SQL to delete expired rows and when the primary key contains float/double column,
// it is hard to use condition `WHERE PK in (...)` to delete specified rows because some precision will be lost when comparing.
func checkPrimaryKeyForTTLTable(tblInfo *model.TableInfo) error {
	if !tblInfo.IsCommonHandle {
		// only check the primary keys when it is common handle
		return nil
	}

	pk := tblInfo.GetPrimaryKey()
	if pk == nil {
		return nil
	}

	for _, colDef := range pk.Columns {
		col := tblInfo.Columns[colDef.Offset]
		switch col.GetType() {
		case mysql.TypeFloat, mysql.TypeDouble:
			return dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL
		}
	}

	return nil
}

// getTTLInfoInOptions returns the aggregated ttlInfo, the ttlEnable, or an error.
// if TTL, TTL_ENABLE or TTL_JOB_INTERVAL is not set in the config, the corresponding return value will be nil.
// if both of TTL and TTL_ENABLE are set, the `ttlInfo.Enable` will be equal with `ttlEnable`.
// if both of TTL and TTL_JOB_INTERVAL are set, the `ttlInfo.JobInterval` will be equal with `ttlCronJobSchedule`.
func getTTLInfoInOptions(options []*ast.TableOption) (ttlInfo *model.TTLInfo, ttlEnable *bool, ttlCronJobSchedule *string, err error) {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionTTL:
			var sb strings.Builder
			restoreFlags := format.RestoreStringSingleQuotes | format.RestoreNameBackQuotes
			restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
			err := op.Value.Restore(restoreCtx)
			if err != nil {
				return nil, nil, nil, err
			}

			intervalExpr := sb.String()
			ttlInfo = &model.TTLInfo{
				ColumnName:       op.ColumnName.Name,
				IntervalExprStr:  intervalExpr,
				IntervalTimeUnit: int(op.TimeUnitValue.Unit),
				Enable:           true,
				JobInterval:      DefaultTTLJobInterval,
			}
		case ast.TableOptionTTLEnable:
			ttlEnable = &op.BoolValue
		case ast.TableOptionTTLJobInterval:
			ttlCronJobSchedule = &op.StrValue
		}
	}

	if ttlInfo != nil {
		if ttlEnable != nil {
			ttlInfo.Enable = *ttlEnable
		}
		if ttlCronJobSchedule != nil {
			ttlInfo.JobInterval = *ttlCronJobSchedule
		}
	}

	if err = checkTTLJobInterval(ttlCronJobSchedule); err != nil {
		return nil, nil, nil, err
	}

	return ttlInfo, ttlEnable, ttlCronJobSchedule, nil
}

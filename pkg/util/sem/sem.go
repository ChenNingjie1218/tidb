// Copyright 2021 PingCAP, Inc.
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

package sem

import (
	"os"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	metricsSchema         = "metrics_schema"
	exprPushdownBlacklist = "expr_pushdown_blacklist"
	gcDeleteRange         = "gc_delete_range"
	gcDeleteRangeDone     = "gc_delete_range_done"
	optRuleBlacklist      = "opt_rule_blacklist"
	tidb                  = "tidb"
	globalVariables       = "global_variables"
	informationSchema     = "information_schema"
	clusterConfig         = "cluster_config"
	clusterHardware       = "cluster_hardware"
	clusterLoad           = "cluster_load"
	clusterLog            = "cluster_log"
	clusterSystemInfo     = "cluster_systeminfo"
	inspectionResult      = "inspection_result"
	inspectionRules       = "inspection_rules"
	inspectionSummary     = "inspection_summary"
	metricsSummary        = "metrics_summary"
	metricsSummaryByLabel = "metrics_summary_by_label"
	metricsTables         = "metrics_tables"
	tidbHotRegions        = "tidb_hot_regions"
	performanceSchema     = "performance_schema"
	pdProfileAllocs       = "pd_profile_allocs"
	pdProfileBlock        = "pd_profile_block"
	pdProfileCPU          = "pd_profile_cpu"
	pdProfileGoroutines   = "pd_profile_goroutines"
	pdProfileMemory       = "pd_profile_memory"
	pdProfileMutex        = "pd_profile_mutex"
	tidbProfileAllocs     = "tidb_profile_allocs"
	tidbProfileBlock      = "tidb_profile_block"
	tidbProfileCPU        = "tidb_profile_cpu"
	tidbProfileGoroutines = "tidb_profile_goroutines"
	tidbProfileMemory     = "tidb_profile_memory"
	tidbProfileMutex      = "tidb_profile_mutex"
	tikvProfileCPU        = "tikv_profile_cpu"
	tidbGCLeaderDesc      = "tidb_gc_leader_desc"
	restrictedPriv        = "RESTRICTED_"
	tidbAuditRetractLog   = "tidb_audit_redact_log" // sysvar installed by a plugin

	placementAdmin     = "PLACEMENT_ADMIN"
	backupAdmin        = "BACKUP_ADMIN"
	restoreAdmin       = "RESTORE_ADMIN"
	resourceGroupAdmin = "RESOURCE_GROUP_ADMIN"

	// Additional tables for serverless tier.
	attributes            = "attributes"
	clusterInfo           = "cluster_info"
	tikvRegionStatus      = "tikv_region_status"
	tikvStoreStatus       = "tikv_store_status"
	resourceGroups        = "resource_groups"
	tidbHotRegionsHistory = "tidb_hot_regions_history"
	tidbServersInfo       = "tidb_servers_info"
	placementPolicies     = "placement_policies"
	tikvRegionPeers       = "tikv_region_peers"
	tidbTTLJobHistory     = "tidb_ttl_job_history"
	tidbTTLTableStatus    = "tidb_ttl_table_status"
	tidbTTLTask           = "tidb_ttl_task"

	// Serverless tier slow query related tables.
	slowQuery                       = "slow_query"
	clusterSlowQuery                = "cluster_slow_query"
	statementsSummary               = "statements_summary"
	statementsSummaryEvicted        = "statements_summary_evicted"
	statementsSummaryHistory        = "statements_summary_history"
	clusterStatementsSummary        = "cluster_statements_summary"
	clusterStatementsSummaryEvicted = "cluster_statements_summary_evicted"
	clusterStatementsSummaryHistory = "cluster_statements_summary_history"
)

var (
	semEnabled int32
	semLevel   int32
)

const (
	levelBasicVal  int32 = 1
	levelStrictVal       = 2
	levelConfigVal       = 3
)

// Enable enables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Enable(level string) error {
	failpoint.Inject("skipSEM", func() {
		logutil.BgLogger().Info("skip enabling SEM")
		failpoint.Return(nil)
	})

	if !atomic.CompareAndSwapInt32(&semEnabled, 0, 1) {
		logutil.BgLogger().Info("SEM enable operation was skipped because it is already enabled")
		return nil
	}

	switch level {
	case config.SEMLevelBasic:
		atomic.StoreInt32(&semLevel, levelBasicVal)
		variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.On)
		variable.SetSysVar(variable.Hostname, variable.DefHostname)
	case config.SEMLevelStrict:
		atomic.StoreInt32(&semLevel, levelStrictVal)
		enableStrictMode()
	case config.SEMLevelConfig:
		atomic.StoreInt32(&semLevel, levelConfigVal)
		enableConfigMode()
	default:
		return errors.Errorf("invalid level option for sem: %s", level)
	}
	// write to log so users understand why some operations are weird.
	logutil.BgLogger().Info("tidb-server is operating with security enhanced mode (SEM) enabled",
		zap.String("level", level),
	)
	return nil
}

// Disable disables SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func Disable() {
	if !atomic.CompareAndSwapInt32(&semEnabled, 1, 0) {
		logutil.BgLogger().Info("SEM disable operation was skipped because it is already disabled")
		return
	}

	switch atomic.LoadInt32(&semLevel) {
	case levelBasicVal:
		variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.Off)
		if hostname, err := os.Hostname(); err == nil {
			variable.SetSysVar(variable.Hostname, hostname)
		}
	case levelStrictVal:
		disableStrictMode()
	case levelConfigVal:
		disableConfigMode()
	}
}

// IsEnabled checks if Security Enhanced Mode (SEM) is enabled
func IsEnabled() bool {
	return atomic.LoadInt32(&semEnabled) == 1
}

// IsInvisibleSchema returns true if the dbName needs to be hidden
// when sem is enabled.
func IsInvisibleSchema(dbName string) bool {
	if isConfigMode() {
		return configModeInvisibleSchema(dbName)
	}
	return strings.EqualFold(dbName, metricsSchema)
}

// IsInvisibleTable returns true if the table needs to be hidden
// when sem is enabled.
func IsInvisibleTable(dbLowerName, tblLowerName string) bool {
	if isConfigMode() {
		return configModeInvisibleTable(dbLowerName, tblLowerName)
	}

	switch dbLowerName {
	case mysql.SystemDB:
		switch tblLowerName {
		case exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables:
			return true
		}
	case informationSchema:
		switch tblLowerName {
		case clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
			inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions,
			clusterInfo, tikvRegionStatus, tikvStoreStatus, clusterSlowQuery,
			slowQuery, statementsSummary, statementsSummaryEvicted, statementsSummaryHistory, clusterStatementsSummary,
			clusterStatementsSummaryEvicted, clusterStatementsSummaryHistory, resourceGroups, tidbHotRegionsHistory,
			tidbServersInfo:
			return true
		}
	case performanceSchema:
		switch tblLowerName {
		case pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
			pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
			tidbProfileMemory, tidbProfileMutex, tikvProfileCPU:
			return true
		}
	case metricsSchema:
		return true
	}
	return isStrictMode() && strictModeInvisibleTable(dbLowerName, tblLowerName)
}

// IsRestrictedStatus return the actual restricted status of the status variable.
// false indicates no restriction.
func IsRestrictedStatus(varName string) (bool, *config.RestrictedStatus) {
	if isConfigMode() {
		return configModeRestrictedStatus(varName)
	}

	if varName == tidbGCLeaderDesc {
		return true, &config.RestrictedStatus{
			Name:            tidbGCLeaderDesc,
			RestrictionType: "hidden",
		}
	}
	return false, nil
}

// IsInvisibleSysVar returns true if the sysvar needs to be hidden
func IsInvisibleSysVar(varNameInLower string) bool {
	if isConfigMode() {
		return configModeInvisibleSysVar(varNameInLower)
	}

	switch varNameInLower {
	case variable.TiDBDDLSlowOprThreshold, // ddl_slow_threshold
		variable.TiDBCheckMb4ValueInUTF8,
		variable.TiDBConfig,
		variable.TiDBEnableSlowLog,
		variable.TiDBEnableTelemetry,
		variable.TiDBExpensiveQueryTimeThreshold,
		variable.TiDBForcePriority,
		variable.TiDBGeneralLog,
		variable.TiDBMetricSchemaRangeDuration,
		variable.TiDBMetricSchemaStep,
		variable.TiDBOptWriteRowID,
		variable.TiDBPProfSQLCPU,
		variable.TiDBRecordPlanInSlowLog,
		variable.TiDBRowFormatVersion,
		variable.TiDBSlowQueryFile,
		variable.TiDBSlowLogThreshold,
		variable.TiDBSlowTxnLogThreshold,
		variable.TiDBEnableCollectExecutionInfo,
		variable.TiDBMemoryUsageAlarmRatio,
		variable.TiDBRedactLog,
		variable.TiDBRestrictedReadOnly,
		variable.TiDBTopSQLMaxTimeSeriesCount,
		variable.TiDBTopSQLMaxMetaCount,
		// TODO: cp-7.5 variable.TiDBEnableVectorType,
		tidbAuditRetractLog:
		return true
	}
	return isStrictMode() && strictModeInvisibleSysVar(varNameInLower)
}

// IsReadOnlySysVar returns true if the sysvar is read-only
func IsReadOnlySysVar(varNameInLower string) bool {
	if isConfigMode() {
		return configModeReadOnlySysVar(varNameInLower)
	}

	return isStrictMode() && strictModeReadOnlySysVar(varNameInLower)
}

// IsReplacedSysVar returns true if the sysVar is replaced.
func IsReplacedSysVar(varNameInLower string) bool {
	return isConfigMode() && configModeReplacedSysVar(varNameInLower)
}

// IsReadOnlyGlobalSysVar returns true if global sysVar is read only.
func IsReadOnlyGlobalSysVar(varNameInLower string) bool {
	return isConfigMode() && configModeReadOnlyGlobalSysVar(varNameInLower)
}

// IsStaticPermissionRestricted returns true if the static privilege should be restricted in config mode.
func IsStaticPermissionRestricted(privType mysql.PrivilegeType) bool {
	return isConfigMode() && configModeStaticPermissionRestricted(privType)
}

// IsRestrictedPrivilege returns true if the privilege shuld not be satisfied by SUPER
// As most dynamic privileges are.
func IsRestrictedPrivilege(privNameInUpper string) bool {
	if isConfigMode() {
		return configModeRestrictedPrivilege(privNameInUpper)
	}

	switch privNameInUpper {
	case
		placementAdmin,
		backupAdmin,
		restoreAdmin,
		resourceGroupAdmin:
		return true
	}
	if len(privNameInUpper) >= 12 && privNameInUpper[:11] == restrictedPriv {
		return true
	}
	return isStrictMode() && strictModeRestrictedPrivilege(privNameInUpper)
}

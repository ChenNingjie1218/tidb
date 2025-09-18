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

package session

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tidbworker"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	serverlessVersionVar = "serverless_version"
	// serverlessVersion2 added support for Disaggregated TiFlash, as a result, we can no safely enable mpp
	// and two previously forbidden executor push-down.
	serverlessVersion2 = 2
	// serverlessVersion3 adds json contains to tikv push-down-blacklist and adds reference_priv to cloud_admin.
	serverlessVersion3 = 3
	// serverlessVersion4 adds 6.4-6.6 newly added push-down-blacklist items.
	serverlessVersion4 = 4
	// serverlessVersion5 changes variable `tidb_stmt_summary_refresh_interval` and `tidb_stmt_summary_max_stmt_count`.
	serverlessVersion5 = 5
	// serverlessVersion6 fixes few push down executor name.
	serverlessVersion6 = 6
	// serverlessVersion7 disable async commit.
	serverlessVersion7 = 7
	// serverlessVersion8 adjusts push-down blacklist for CSE and TiFlash 6.5
	serverlessVersion8 = 8
	// serverlessVersion9 change variable `max_execution_time` to `30m`.
	serverlessVersion9 = 9
	// serverlessVersion10 disable 1pc.
	serverlessVersion10 = 10
	// serverlessVersion11 grants `cloud_admin` with the privilege that `grant 'role_admin' to <user>`
	serverlessVersion11 = 11
	// serverlessVersion12 creates missing 'role_admin' user.
	serverlessVersion12 = 12
	// serverlessVersion13 is marked as a no-op.
	serverlessVersion13 = 13
	// serverlessVersion14 reverts the change of serverlessVersion11.
	serverlessVersion14 = 14
	// serverlessVersion15 rename user cloud_admin to prefix.cloud_admin`.
	serverlessVersion15 = 15
	// serverlessVersion16 is noop.
	serverlessVersion16 = 16
	// serverlessVersion17 disable tidb_pessimistic_txn_fair_locking.
	serverlessVersion17 = 17
	// ...
	// [serverlessVersion18, serverlessVersion29] is the version range reserved for serverless TiDB 6.6.
	// ...
	// serverlessVersion30 adds 7.1 executors that's incompatible with tiflash 6.5 to pushdown_blacklist.
	serverlessVersion30 = 30
	// serverlessVersion31 change `tidb_replica_read` to `leader`.
	serverlessVersion31 = 31
	// serverlessVersion32 modify column `token` of table `mysql.plan_replayer_status` from `VARCHAR(128)` to `TEXT`.
	serverlessVersion32 = 32
	// serverlessVersion33 adds table mysql.auto_analyze_tasks and mysql.auto_analyze_tasks_history.
	serverlessVersion33 = 33
	// ...
	// [serverlessVersion34, serverlessVersion39] is the version range reserved for serverless TiDB 7.1.
	// ...
	serverlessVersion40 = 40
	// serverlessVersion41 adds AUDIT_ADMIN and RESTRICTED_AUDIT_ADMIN privileges to cloud_admin.
	serverlessVersion41 = 41
	// serverlessVersion42 add `mediumtext,longtext` to variable `tidb_analyze_skip_column_types`.
	serverlessVersion42 = 42
	// serverlessVersion43 remove all existing grants that use cloud_admin as a role
	serverlessVersion43 = 43
	// ...
	// [serverlessVersion44, serverlessVersion50] is the version range reserved for serverless TiDB 7.5.
	// ...
)

const (
	// defaultMaxExecutionTime is the max execution time for serverless.
	defaultMaxExecutionTime = int(30 * time.Minute / time.Millisecond)
	// defaultReplicaRead is the default value of replica read.
	defaultReplicaRead = "leader"

	branchBootstrapStateVar = "branch_bootstrap_state"
)

const (
	// CreateAutoAnalyzeTasks is the SQL statement to create the auto analyze tasks table.
	CreateAutoAnalyzeTasks = `CREATE TABLE IF NOT EXISTS mysql.auto_analyze_tasks (
		id BIGINT(64) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		table_id BIGINT(64) NOT NULL,
		start_time BIGINT(64)
	);`

	// CreateAutoAnalyzeTasksHistory is the SQL statement to create the auto analyze tasks history table.
	CreateAutoAnalyzeTasksHistory = `CREATE TABLE IF NOT EXISTS mysql.auto_analyze_tasks_history (
		id BIGINT(64) UNSIGNED PRIMARY KEY,
		table_id BIGINT(64) NOT NULL,
		analyzed BOOLEAN NOT NULL DEFAULT FALSE,
		statement TEXT,
		err TEXT,
		start_time BIGINT(64),
		end_time BIGINT(64)
	);`
)

// currentServerlessVersion is defined as a variable, so we can modify its value for testing.
// please make sure this is the largest version
var currentServerlessVersion int64 = serverlessVersion43

var bootstrapServerlessVersion = []func(sessiontypes.Session, int64){
	upgradeToServerlessVer2,
	upgradeToServerlessVer3,
	upgradeToServerlessVer4,
	upgradeToServerlessVer5,
	upgradeToServerlessVer6,
	upgradeToServerlessVer7,
	upgradeToServerlessVer8,
	upgradeToServerlessVer9,
	upgradeToServerlessVer10,
	upgradeToServerlessVer11,
	upgradeToServerlessVer12,
	upgradeToServerlessVer13,
	upgradeToServerlessVer14,
	upgradeToServerlessVer15,
	upgradeToServerlessVer17,
	upgradeToServerlessVer30,
	upgradeToServerlessVer31,
	upgradeToServerlessVer32,
	upgradeToServerlessVer33,
	upgradeToServerlessVer40,
	upgradeToServerlessVer41,
	upgradeToServerlessVer42,
	upgradeToServerlessVer43,
}

// updateServerlessVersion updates serverless version variable in mysql.TiDB table.
func updateServerlessVersion(s sessiontypes.Session) {
	// Update serverless version.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "Serverless bootstrap version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, serverlessVersionVar, currentServerlessVersion, currentServerlessVersion,
	)
}

// getServerlessVersion gets serverless version from mysql.tidb table.
func getServerlessVersion(s sessiontypes.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, serverlessVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// serverlessBootstrapped is used in dist task framework to ensure serverless upgrade is only executed once.
var serverlessBootstrapped = &atomic.Bool{}

// runServerlessUpgrade check and runs upgradeServerless functions on given store if necessary.
func runServerlessUpgrade(store kv.Storage) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

	s.SetValue(sessionctx.Initing, true)
	upgradeServerless(s)
	s.ClearValue(sessionctx.Initing)

	// execute in test and execute only once.
	if intest.InTest && serverlessBootstrapped.CompareAndSwap(false, true) {
		dom := domain.GetDomain(s)
		dom.Close()
		domap.Delete(store)
	}
}

// abortGCV2 aborts the GCV2 worker if it's running.
func abortGCV2() {
	if tidbworker.IsGCV2Worker() {
		err := tidbworker.GlobalTiDBWorkerManager.AbortGCV2(context.Background())
		if err != nil {
			logutil.BgLogger().Fatal("abort gc worker failed", zap.Error(err))
		}
		logutil.BgLogger().Info("gcv2 worker aborted")
		os.Exit(0)
	}
}

// upgradeServerless execute some upgrade work if system is bootstrapped by tidb with lower serverless version.
func upgradeServerless(s sessiontypes.Session) {
	ver, err := getServerlessVersion(s)
	terror.MustNil(err)

	if ver >= currentServerlessVersion {
		logutil.BgLogger().Info("[upgrade] The current serverless version is greater than or equal to the target version, skip upgrade",
			zap.Int64("current-serverless-version", ver),
			zap.Int64("target-serverless-version", currentServerlessVersion),
		)
		return
	}

	// Abort upgrade if TiDB is running as gcv2 worker.
	abortGCV2()

	// Do upgrade works then update serverless version.
	for _, upgradeFunc := range bootstrapServerlessVersion {
		logutil.BgLogger().Info("[upgrade] before upgrade serverless version", zap.Int64("serverless-version", ver))
		upgradeFunc(s, ver)
		logutil.BgLogger().Info("[upgrade] upgrade the serverless version succeed", zap.Int64("serverless-version", ver))
	}
	updateServerlessVersion(s)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err = s.ExecuteInternal(ctx, "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("upgrade serverless version failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if serverless version is already upgraded.
		v, err1 := getServerlessVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade serverless version failed", zap.Error(err1))
		}
		if v >= currentServerlessVersion {
			// It is already bootstrapped/upgraded by a TiDB server with higher serverless version.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade serverless version failed",
			zap.Int64("from", ver),
			zap.Int64("to", currentServerlessVersion),
			zap.Error(err))
	}
	logutil.BgLogger().Info("[upgrade] upgrade all serverless version succeed", zap.Int64("currentServerlessVersion", currentServerlessVersion))
}

// Serverless upgrade functions.
// NOTE: Upgrade functions below will only be executed on cluster that's already bootstrapped by a TiDB with lower serverless version,
// When applying changes, add more upgrade function instead of modifying them.
// Addition of them requires changes to serverless bootstrap procedures further below.

func upgradeToServerlessVer2(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion2 {
		return
	}

	// Enable mpp.
	mustExecute(s, "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?;",
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAllowMPPExecution, variable.On, variable.On)

	// Remove lead/lag from pushdown_blacklist.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist where name in "+
		"(\"Lead\", \"Lag\") and store_type = \"tiflash\"")
}

func upgradeToServerlessVer3(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion3 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('json_contains','tikv', 'Compatibility with tikv 6.1')")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y' WHERE User='cloud_admin' AND Host='%'")
}

func upgradeToServerlessVer4(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion4 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('json_valid','tikv', 'Compatibility with tikv 6.1'),"+
		"('json_unquote','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('json_extract','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_like','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_substr','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_instr','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('regexp_replace','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('Cast.CastJsonAsString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('Extract.ExtractDuration','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('least.LeastString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('greatest.GreatestString','tiflash', 'Compatibility with tiflash 6.1'),"+
		"('unhex','tiflash', 'Compatibility with tiflash 6.1')",
	)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='Cast.CastTimeAsDuration' WHERE name='Cast.ScalarFuncSig_CastTimeAsDuration' and store_type = 'tiflash'")
}

func upgradeToServerlessVer5(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion5 {
		return
	}
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryRefreshInterval, 60, 60,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryMaxStmtCount, 1000, 1000,
	)
}

func upgradeToServerlessVer6(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion6 {
		return
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_like' WHERE name='RegexpLike' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_substr' WHERE name='RegexpSubstr' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_instr' WHERE name='RegexpInStr' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='regexp_replace' WHERE name='RegexpReplace' and store_type = 'tikv'")

	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.expr_pushdown_blacklist"+
		" SET name='get_format' WHERE name='GetFormat' and store_type = 'tiflash'")
}

func upgradeToServerlessVer7(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion7 {
		return
	}
	mustExecute(s, "set @@global.tidb_enable_async_commit=OFF;")
}

func upgradeToServerlessVer8(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion8 {
		return
	}

	// Remove executors from pushdown_blacklist that tiflash 6.5 supports.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist WHERE LOWER(name) IN "+
		"('hex',"+
		"'get_format',"+
		"'space',"+
		"'cast.casttimeasduration',"+
		"'reverse',"+
		"'elt',"+
		"'repeat',"+
		"'rightshift',"+
		"'leftshift',"+
		"'json_unquote',"+
		"'json_extract',"+
		"'regexp',"+
		"'regexp_like',"+
		"'regexp_substr',"+
		"'regexp_instr',"+
		"'cast.castjsonasstring',"+
		"'extract.extractduration') "+
		"AND LOWER(store_type) = 'tiflash'")

	// Remove executors from pushdown_blacklist that cse 6.5 supports.
	mustExecute(s, "DELETE FROM mysql.expr_pushdown_blacklist WHERE LOWER(name) IN "+
		"('regexp',"+
		"'regexp_like',"+
		"'regexp_substr',"+
		"'regexp_instr',"+
		"'regexp_replace',"+
		"'json_contains',"+
		"'json_valid') "+
		"AND LOWER(store_type) = 'tikv'")
}

func upgradeToServerlessVer9(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion9 {
		return
	}
	mustExecute(s, "set @@global.max_execution_time=%?", defaultMaxExecutionTime)
}

func upgradeToServerlessVer10(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion10 {
		return
	}

	mustExecute(s, "set @@global.tidb_enable_1pc=OFF")
}

func upgradeToServerlessVer11(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion11 {
		return
	}
	insertGlobalGrants(s, "cloud_admin", "ROLE_ADMIN", "Y")
}

func upgradeToServerlessVer12(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion12 {
		return
	}

	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "Y", `+
		`Shutdown_priv = "N", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "";`,
	)

	// GRANT ROLE_ADMIN ON *.* to 'role_admin';
	insertGlobalGrants(s, "role_admin", "ROLE_ADMIN", "N")

	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`Priv = "{}";`,
	)
}

func upgradeToServerlessVer13(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion13 {
		return
	}
	// no-op
}

func upgradeToServerlessVer14(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion14 {
		return
	}
	mustExecute(s, `DELETE FROM mysql.global_grants where `+
		`User = %? `+
		`AND Host = "%" `+
		`AND Priv = %?`,
		"cloud_admin",
		"ROLE_ADMIN",
	)
}

func upgradeToServerlessVer15(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion15 {
		return
	}

	if variants := keyspace.GetUsernamePolicy().GetUsernameVariants("cloud_admin"); len(variants) > 0 {
		cloudAdminName := variants[0]
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.global_priv SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
		mustExecute(s, "UPDATE HIGH_PRIORITY mysql.global_grants SET User=%? WHERE User='cloud_admin' AND Host='%'", cloudAdminName)
	}
}

func upgradeToServerlessVer17(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion17 {
		return
	}

	// TODO: remove this after the next cse upgrade to 7.1
	mustExecute(s, "set @@global.tidb_pessimistic_txn_fair_locking=OFF")
}

func upgradeToServerlessVer30(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion30 {
		return
	}

	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('ilike','tiflash', 'Compatibility with tiflash 6.5'),"+
		"('is_ipv4','tiflash', 'Compatibility with tiflash 6.5'),"+
		"('is_ipv6','tiflash', 'Compatibility with tiflash 6.5')",
	)
}

func upgradeToServerlessVer31(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion31 {
		return
	}
	mustExecute(s, "set @@global.tidb_replica_read=%?", defaultReplicaRead)
}

func upgradeToServerlessVer32(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion32 {
		return
	}
	mustExecute(s, "ALTER TABLE mysql.plan_replayer_status MODIFY COLUMN `token` TEXT")
}

func upgradeToServerlessVer33(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion33 {
		return
	}
	mustExecute(s, CreateAutoAnalyzeTasks)
	mustExecute(s, CreateAutoAnalyzeTasksHistory)
}

func upgradeToServerlessVer40(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion40 {
		return
	}
	mustExecute(s, `
		DELETE FROM mysql.expr_pushdown_blacklist
		WHERE name IN (
			'regexp_replace',
			'least.LeastString',
			'greatest.GreatestString',
			'unhex',
			'ilike',
			'is_ipv4',
			'is_ipv6'
		)
		AND store_type = 'tiflash';`)
}

func upgradeToServerlessVer41(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion41 {
		return
	}
	if variants := keyspace.GetUsernamePolicy().GetUsernameVariants("cloud_admin"); len(variants) > 0 {
		cloudAdminName := variants[0]
		insertGlobalGrants(s, cloudAdminName, "RESTRICTED_AUDIT_ADMIN", "N")
	}
}

func upgradeToServerlessVer42(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion42 {
		return
	}
	newVal, err := updateGlobalVariable(s, variable.TiDBAnalyzeSkipColumnTypes, appendMediumAndLongTextToSkipTypes)
	if err != nil {
		logutil.BgLogger().Error("[upgrade] update @@global.tidb_analyze_skip_column_types failed",
			zap.Error(err))
	} else if newVal != nil {
		logutil.BgLogger().Info("[upgrade] update @@global.tidb_analyze_skip_column_types",
			zap.String("newVal", *newVal))
	}
}

// Ref: `variable.analyzeSkipAllowedTypes`
const (
	mediumText = "mediumtext"
	longText   = "longtext"
)

func appendMediumAndLongTextToSkipTypes(skipTypesStr string) (string, error) {
	skipTypes := variable.ParseAnalyzeSkipColumnTypes(skipTypesStr)
	if _, ok := skipTypes[mediumText]; !ok {
		skipTypesStr += "," + mediumText
	}
	if _, ok := skipTypes[longText]; !ok {
		skipTypesStr += "," + longText
	}
	skipTypesStr = strings.TrimPrefix(skipTypesStr, ",")

	return skipTypesStr, nil
}

func upgradeToServerlessVer43(s sessiontypes.Session, ver int64) {
	if ver >= serverlessVersion43 {
		return
	}
	cloudAdminName := "cloud_admin"
	if variants := keyspace.GetUsernamePolicy().GetUsernameVariants("cloud_admin"); len(variants) > 0 {
		cloudAdminName = variants[0]
	}
	mustExecute(s, `DELETE FROM mysql.role_edges WHERE `+
		`FROM_HOST = "%" AND `+
		`FROM_USER = %?`,
		cloudAdminName)
}

// Serverless bootstrap procedures.
// NOTE: The following methods will only be executed once at doDMLWorks during TiDB Bootstrap,
// therefore any modification of it requires addition to the serverless version upgrade function above
// in order to cover those already bootstrapped.

// bootstrapServerlessPushdownBlacklist writes serverless expr pushdown blacklist into mysql.expr_pushdown_blacklist.
// This is required when using CSE and TiFlash at a lower version than TiDB.
func bootstrapServerlessPushdownBlacklist(_ sessiontypes.Session) {
	return
}

// bootstrapServerlessVariables writes serverless global variables into mysql.GLOBAL_VARIABLES.
func bootstrapServerlessVariables(s sessiontypes.Session) {
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBAnalyzeVersion, 2, 2,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBRedactLog, variable.On, variable.On,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryRefreshInterval, 60, 60,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBStmtSummaryMaxStmtCount, 1000, 1000,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnableAsyncCommit, variable.Off, variable.Off,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBEnable1PC, variable.Off, variable.Off,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable,
		variable.MaxExecutionTime,
		defaultMaxExecutionTime,
		defaultMaxExecutionTime,
	)
	// TODO: remove this after the next cse upgrade to 7.1
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBPessimisticTransactionFairLocking, variable.Off, variable.Off,
	)
	mustExecute(s, `INSERT HIGH_PRIORITY INTO %n.%n VALUES(%?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.GlobalVariablesTable, variable.TiDBReplicaRead, defaultReplicaRead, defaultReplicaRead,
	)
}

// bootstrapServerlessRoot writes root user's privilege into mysql.user.
// Not using grant/revoke statement because this is executed prior to privilege module load.
// Root username are passed as argument since it's prefixed by keyspace token.
func bootstrapServerlessRoot(s sessiontypes.Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = %?, `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "N", `+
		`Shutdown_priv = "N", `+ // REVOKE SHUTDOWN ON *.* FROM root;
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+ // REVOKE CONFIG ON *.* FROM root;
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "" `,
		userName,
	)
}

// bootstrapCloudAdmin creates user cloud_admin and configure its privilege into mysql.user.
func bootstrapCloudAdmin(s sessiontypes.Session, userName string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = %?, `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "N", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "N", `+
		`Create_tmp_table_priv = "N", `+
		`Lock_tables_priv = "N", `+
		`Execute_priv = "N", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "N", `+
		`Create_routine_priv = "N", `+
		`Alter_routine_priv = "N", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "N", `+
		`Repl_slave_priv = "N", `+
		`Repl_client_priv = "N", `+
		`Trigger_priv = "N", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "N", `+
		`Account_locked = "N", `+
		`Shutdown_priv = "Y", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "N", `+
		`Config_priv = "Y", `+
		`Create_Tablespace_Priv = "N", `+
		`User_attributes = NULL, `+
		`Token_issuer = "" `,
		userName,
	)

	insertGlobalGrants(s, userName, "DASHBOARD_CLIENT", "N")
	insertGlobalGrants(s, userName, "SYSTEM_VARIABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "CONNECTION_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_VARIABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_STATUS_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_CONNECTION_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_USER_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_TABLES_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTRICTED_REPLICA_WRITER_ADMIN", "N")
	insertGlobalGrants(s, userName, "BACKUP_ADMIN", "N")
	insertGlobalGrants(s, userName, "RESTORE_ADMIN", "N")
	insertGlobalGrants(s, userName, "SYSTEM_USER", "Y")
	insertGlobalGrants(s, userName, "RESTRICTED_AUDIT_ADMIN", "N")

	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = %?, `+
		`Priv = "{}"`,
		userName,
	)
}

// bootstrapRoleAdmin creates user role_admin and configure its privileges.
func bootstrapRoleAdmin(s sessiontypes.Session) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.user SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`authentication_string = "", `+
		`plugin = "mysql_native_password", `+
		`Select_priv = "Y", `+
		`Insert_priv = "Y", `+
		`Update_priv = "Y", `+
		`Delete_priv = "Y", `+
		`Create_priv = "Y", `+
		`Drop_priv = "Y", `+
		`Process_priv = "Y", `+
		`Grant_priv = "Y", `+
		`References_priv = "Y", `+
		`Alter_priv = "Y", `+
		`Show_db_priv = "Y", `+
		`Super_priv = "Y", `+
		`Create_tmp_table_priv = "Y", `+
		`Lock_tables_priv = "Y", `+
		`Execute_priv = "Y", `+
		`Create_view_priv = "Y", `+
		`Show_view_priv = "Y", `+
		`Create_routine_priv = "Y", `+
		`Alter_routine_priv = "Y", `+
		`Index_priv = "Y", `+
		`Create_user_priv = "Y", `+
		`Event_priv = "Y", `+
		`Repl_slave_priv = "Y", `+
		`Repl_client_priv = "Y", `+
		`Trigger_priv = "Y", `+
		`Create_role_priv = "Y", `+
		`Drop_role_priv = "Y", `+
		`Account_locked = "Y", `+
		`Shutdown_priv = "N", `+
		`Reload_priv = "Y", `+
		`FILE_priv = "Y", `+
		`Config_priv = "N", `+
		`Create_Tablespace_Priv = "Y", `+
		`User_attributes = NULL, `+
		`Token_issuer = "";`,
	)

	// GRANT ROLE_ADMIN ON *.* to 'role_admin';
	insertGlobalGrants(s, "role_admin", "ROLE_ADMIN", "N")

	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.global_priv SET `+
		`Host = "%", `+
		`User = "role_admin", `+
		`Priv = "{}";`,
	)
}

// insertGlobalGrants inserts user's privilege into mysql.global_grants.
func insertGlobalGrants(s sessiontypes.Session, userName, priv, grant string) {
	mustExecute(s, `REPLACE HIGH_PRIORITY INTO mysql.global_grants SET `+
		`USER = %?, `+
		`HOST = "%", `+
		`PRIV = %?, `+
		`WITH_GRANT_OPTION = %?`,
		userName,
		priv,
		grant,
	)
}

func isBranchBootstrappedOld(s sessiontypes.Session) (bool, error) {
	sVal, isNull, err := getTiDBVar(s, branchBootstrapStateVar)
	if err != nil {
		return false, errors.Trace(err)
	}
	if isNull {
		return false, nil
	}
	return sVal == "True", nil
}

func isBranchBootstrapped(s sessiontypes.Session) (bool, error) {
	if config.GetGlobalConfig().IsBranchBootstrapped == "" {
		// Fall back to the old logic to determine if the branch is bootstrapped
		return isBranchBootstrappedOld(s)
	} else if config.GetGlobalConfig().IsBranchBootstrapped == "True" {
		return true, nil
	}
	return false, nil
}

func isClusterBootstrapped() bool {
	isClusterBootstrapped, _ := strconv.ParseBool(config.GetGlobalConfig().IsBootstrappedForRestore)
	return isClusterBootstrapped

}

func runBranchDBUsersAmendment(store kv.Storage) {
	if !config.GetGlobalConfig().IsBranch {
		return
	}

	logutil.BgLogger().Info("start to execute runBranchDBUsersAmendment", zap.String("IsBranchBootstrapped", config.GetGlobalConfig().IsBranchBootstrapped))

	s, err := createSession(store)
	if err != nil {
		logutil.BgLogger().Fatal("runBranchDBUsersAmendment createSession error", zap.Error(err))
	}

	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	defer s.ClearValue(sessionctx.Initing)

	bootstrapped, err := isBranchBootstrapped(s)
	if err != nil {
		logutil.BgLogger().Fatal("runBranchDBUsersAmendment isBranchBootstrapped error", zap.Error(err))
	}
	if bootstrapped {
		logutil.BgLogger().Info("already executed runBranchDBUsersAmendment", zap.String("IsBranchBootstrapped", config.GetGlobalConfig().IsBranchBootstrapped))
		return
	}

	amendDBUsers(s)
}

func runClusterDBUsersAmendment(store kv.Storage) {
	IsBootstrappedForRestore := config.GetGlobalConfig().IsBootstrappedForRestore
	// IsBootstrappedForRestore will not be empty string when create from restore
	if IsBootstrappedForRestore == "" {
		return
	}

	logutil.BgLogger().Info("start to execute runClusterDBUsersAmendment", zap.String("IsBootstrappedForRestore", IsBootstrappedForRestore))

	s, err := createSession(store)
	if err != nil {
		logutil.BgLogger().Fatal("runClusterDBUsersAmendment createSession error", zap.Error(err))
	}

	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	defer s.ClearValue(sessionctx.Initing)

	if isClusterBootstrapped() {
		logutil.BgLogger().Info("already executed runClusterDBUsersAmendment")
		return
	}
	amendDBUsers(s)
}

func amendDBUsers(s sessiontypes.Session) {
	// clear all user and priv tables
	mustExecute(s, "DELETE FROM mysql.db")
	mustExecute(s, "DELETE FROM mysql.default_roles")
	mustExecute(s, "DELETE FROM mysql.global_grants")
	mustExecute(s, "DELETE FROM mysql.global_priv")
	mustExecute(s, "DELETE FROM mysql.role_edges")
	mustExecute(s, "DELETE FROM mysql.user")

	// reinit root/role_admin/cloud_admin
	rootUserName, cloudAdminName := "root", "cloud_admin"
	if variants := keyspace.GetUsernamePolicy().GetUsernameVariants(rootUserName); len(variants) > 0 {
		rootUserName = variants[0]
	}
	if variants := keyspace.GetUsernamePolicy().GetUsernameVariants(cloudAdminName); len(variants) > 0 {
		cloudAdminName = variants[0]
	}

	bootstrapServerlessRoot(s, rootUserName)
	bootstrapRoleAdmin(s)
	bootstrapCloudAdmin(s, cloudAdminName)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
	if err != nil {
		logutil.BgLogger().Fatal("runDBUsersAmendment reinit db user error", zap.Error(err))
	}
	err = setKeyspaceBootstrapped()
	if err != nil {
		logutil.BgLogger().Fatal("runDBUsersAmendment setKeyspaceIsBranchBootstrapped error", zap.Error(err))
	}
	logutil.BgLogger().Info("successfully executed runDBUsersAmendment")
}

// KeyspaceBootstrapped represents parameters needed to modify target keyspace's configs.
type KeyspaceBootstrapped struct {
	Config struct {
		IsBranchBootstrapped     string `json:"serverless_is_branch_bootstrapped,omitempty"`
		IsBootstrappedForRestore string `json:"serverless_is_bootstrapped_for_restore,omitempty"`
	} `json:"config"`
}

func setKeyspaceBootstrapped() error {
	cfg := config.GetGlobalConfig()
	input := KeyspaceBootstrapped{}
	if config.GetGlobalConfig().IsBranch {
		input.Config.IsBranchBootstrapped = "True"
	} else {
		input.Config.IsBootstrappedForRestore = "True"
	}
	return infosync.SetKeyspaceConfig(context.Background(), cfg.KeyspaceName, input)
}

// getGlobalVar gets variable value from mysql.global_variables table.
func getGlobalVar(ctx context.Context, s sessiontypes.Session, name string) (sVal string, isNull bool, e error) {
	rs, err := s.ExecuteInternal(ctx, `SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME= %?`,
		mysql.SystemDB,
		mysql.GlobalVariablesTable,
		name,
	)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if rs == nil {
		return "", true, errors.New("Wrong number of Recordset")
	}
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

// updateGlobalVariable update a global variable.
func updateGlobalVariable(s sessiontypes.Session, name string, updateFn func(string) (string, error)) (*string /* newVal */, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return nil, errors.Trace(err)
	}

	committed := false
	defer func() {
		if !committed {
			s.ExecuteInternal(ctx, "ROLLBACK")
		}
	}()

	oldVal, _, err := getGlobalVar(ctx, s, name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newVal, err := updateFn(oldVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if newVal == oldVal {
		return nil, nil // no need to update
	}

	_, err = s.ExecuteInternal(ctx, "UPDATE HIGH_PRIORITY %n.%n SET VARIABLE_VALUE = %? WHERE VARIABLE_NAME = %?",
		mysql.SystemDB,
		mysql.GlobalVariablesTable,
		newVal,
		name,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = s.ExecuteInternal(ctx, "COMMIT")
	if err != nil {
		return nil, errors.Trace(err)
	}
	committed = true
	return &newVal, nil
}

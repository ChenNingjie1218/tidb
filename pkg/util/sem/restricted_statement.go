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

package sem

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var (
	restrictedUsers = []string{"cloud_admin", "root"}
	restrictedRoles = []string{"cloud_admin"}
)

// isRestrictedUser checks if the username is forbidden from rename or drop.
func isRestrictedUser(userName, hostname string) bool {
	originalName := keyspace.GetUsernamePolicy().GetOriginalUsername(userName)
	if originalName == "" || hostname != "%" {
		return false
	}
	for _, restrictedUser := range restrictedUsers {
		if originalName == restrictedUser {
			return true
		}
	}
	return false
}

// isRestrictedRole check if the role is forbidden from grant and revoke.
func isRestrictedRole(userName, hostname string) bool {
	originalName := keyspace.GetUsernamePolicy().GetOriginalUsername(userName)
	if originalName == "" || hostname != "%" {
		return false
	}
	for _, restrictedRole := range restrictedRoles {
		if originalName == restrictedRole {
			return true
		}
	}
	return false
}

// IsRestrictedStatement checks if the statement is a restricted under enhanced sem,
// and returns an error if it is.
func IsRestrictedStatement(stmt ast.Node) error {
	// Skip checking sem in test
	if intest.InTest {
		return nil
	}
	switch x := stmt.(type) {
	case *ast.DeallocateStmt,
		*ast.DeleteStmt,
		*ast.ExecuteStmt,
		*ast.ExplainStmt,
		*ast.ExplainForStmt,
		*ast.TraceStmt,
		*ast.InsertStmt,
		*ast.LockStatsStmt,
		*ast.UnlockStatsStmt,
		// TODO: *ast.IndexAdviseStmt,
		*ast.PlanReplayerStmt,
		*ast.PrepareStmt,
		*ast.SelectStmt,
		*ast.SetOprStmt,
		*ast.UpdateStmt,
		*ast.DoStmt,
		*ast.SetStmt,
		*ast.AnalyzeTableStmt,
		*ast.CreateBindingStmt,
		*ast.DropBindingStmt,
		*ast.SetBindingStmt,
		*ast.CompactTableStmt:
		return nil
	case *ast.LoadDataStmt:
		return verifyLoadData(x)
	case *ast.AdminStmt:
		return verifyAdmin(x)
	case *ast.LoadStatsStmt:
		return verifyLoadStats(x)
	case *ast.ShowStmt:
		return verifyShow(x)
	case *ast.SetConfigStmt:
		return verifySetConfig(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt, *ast.BRIEStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt,
		*ast.RenameUserStmt, *ast.NonTransactionalDMLStmt, *ast.SetSessionStatesStmt:
		return verifySimple(x)
	case ast.DDLNode:
		return verifyDDL(x)
	case *ast.ChangeStmt:
		return verifyChange(x)
	case *ast.SplitRegionStmt:
		return verifySplitRegion(x)
	}

	return nil
}
func verifyDDL(stmt ast.DDLNode) error {
	switch s := stmt.(type) {
	case
		*ast.CreateDatabaseStmt,
		*ast.AlterDatabaseStmt,
		*ast.DropDatabaseStmt,
		*ast.DropTableStmt,
		*ast.DropSequenceStmt,
		*ast.RenameTableStmt,
		*ast.CreateViewStmt,
		*ast.CreateSequenceStmt,
		*ast.CreateIndexStmt,
		*ast.DropIndexStmt,
		*ast.LockTablesStmt,
		*ast.UnlockTablesStmt,
		*ast.CleanupTableLockStmt,
		*ast.RepairTableStmt,
		*ast.TruncateTableStmt,
		*ast.AlterSequenceStmt,
		*ast.RecoverTableStmt,
		*ast.FlashBackDatabaseStmt,
		*ast.FlashBackTableStmt:
		return nil
	case *ast.CreateTableStmt:
		if !config.GetGlobalConfig().EnableSetTableTTL {
			for _, option := range s.Options {
				if option.Tp == ast.TableOptionTTL ||
					option.Tp == ast.TableOptionTTLEnable ||
					option.Tp == ast.TableOptionTTLJobInterval {
					return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("TTL")
				}
			}
		}
		return nil
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			if !config.GetGlobalConfig().EnableSetTableTTL && spec.Tp == ast.AlterTableRemoveTTL {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("TTL")
			}
			if spec.Tp == ast.AlterTableAttributes || spec.Tp == ast.AlterTablePartitionAttributes {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER TABLE ATTRIBUTES")
			}
		}
		return nil
	case *ast.AlterPlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER PLACEMENT POLICY")
	case *ast.CreatePlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CREATE PLACEMENT POLICY")
	case *ast.DropPlacementPolicyStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("DROP PLACEMENT POLICY")
	case *ast.DropResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("DROP RESOURCE GROUP")
	case *ast.CreateResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CREATE RESOURCE GROUP")
	case *ast.FlashBackToTimestampStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("FLASHBACK CLUSTER")
	case *ast.AlterResourceGroupStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER RESOURCE GROUP")

	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported DDL %T", stmt))
}

func verifySimple(stmt ast.Node) error {
	switch s := stmt.(type) {
	case
		*ast.FlushStmt,
		*ast.BeginStmt,
		*ast.CommitStmt,
		*ast.SavepointStmt,
		*ast.ReleaseSavepointStmt,
		*ast.RollbackStmt,
		*ast.CreateUserStmt,
		*ast.AlterUserStmt,
		*ast.SetPwdStmt,
		*ast.SetSessionStatesStmt,
		*ast.KillStmt,
		*ast.BinlogStmt,
		*ast.DropStatsStmt,
		*ast.SetDefaultRoleStmt,
		*ast.AdminStmt,
		*ast.GrantStmt,
		*ast.RevokeStmt,
		*ast.NonTransactionalDMLStmt,
		*ast.UseStmt:
		return nil
	// Dropping restricted user is not allowed.
	case *ast.DropUserStmt:
		for _, user := range s.UserList {
			if isRestrictedUser(user.Username, user.Hostname) {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("DROP USER %s", user))
			}
		}
		return nil
	// Renaming restricted user is not allowed.
	case *ast.RenameUserStmt:
		for _, userToUser := range s.UserToUsers {
			if isRestrictedUser(userToUser.OldUser.Username, userToUser.OldUser.Hostname) {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("RENAME USER %s", userToUser.OldUser))
			}
		}
		return nil
	case *ast.GrantRoleStmt:
		for _, role := range s.Roles {
			if isRestrictedRole(role.Username, role.Hostname) {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("GRANT ROLE %s", role))
			}
		}
		return nil
	case *ast.RevokeRoleStmt:
		for _, role := range s.Roles {
			if isRestrictedRole(role.Username, role.Hostname) {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("REVOKE %s", role))
			}
		}
		return nil
	case *ast.SetRoleStmt:
		for _, role := range s.RoleList {
			if isRestrictedRole(role.Username, role.Hostname) {
				return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("SET ROLE %s", role))
			}
		}
		return nil
	case *ast.AlterInstanceStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ALTER INSTANCE")
	case *ast.ShutdownStmt:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHUTDOWN")
	case *ast.BRIEStmt:
		return verifyBRIE(s)
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported Executor %T", stmt))
}

func verifyBRIE(stmt *ast.BRIEStmt) error {
	switch stmt.Kind {
	case ast.BRIEKindBackup:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("BACKUP")
	case ast.BRIEKindRestore:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("RESTORE")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("BRIE")
}

func verifyShow(stmt *ast.ShowStmt) error {
	switch stmt.Tp {
	case
		ast.ShowNone,
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowTableStatus,
		ast.ShowColumns,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowCollation,
		ast.ShowCreateTable,
		ast.ShowCreateView,
		ast.ShowCreateUser,
		ast.ShowCreateSequence,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowFunctionStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowStatsExtended,
		ast.ShowStatsMeta,
		ast.ShowStatsHistograms,
		ast.ShowStatsTopN,
		ast.ShowStatsBuckets,
		ast.ShowStatsHealthy,
		ast.ShowStatsLocked,
		ast.ShowHistogramsInFlight,
		ast.ShowColumnStatsUsage,
		ast.ShowProfile,
		ast.ShowProfiles,
		ast.ShowMasterStatus,
		ast.ShowPrivileges,
		ast.ShowErrors,
		ast.ShowBindings,
		ast.ShowBindingCacheStatus,
		ast.ShowOpenTables,
		ast.ShowAnalyzeStatus,
		ast.ShowBuiltins,
		ast.ShowTableNextRowId,
		ast.ShowImports,
		ast.ShowImportJobs,
		ast.ShowCreateImport,
		ast.ShowSessionStates,
		// "SHOW CONFIG" command is necessary for lightning to function,
		// therefore, it's access is restricted via privileges.
		ast.ShowConfig,
		// "SHOW PLUGINS" command is necessary for mysql workbench,
		// hence we return an empty result in fetchShowPlugins instead of returning an error here.
		ast.ShowPlugins:
		return nil
	case ast.ShowCreateResourceGroup:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW CREATE RESOURCE GROUP")
	case ast.ShowCreatePlacementPolicy:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW CREATE PLACEMENT POLICY")
	case ast.ShowBackups:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW BACKUPS")
	case ast.ShowRestores:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW RESTORES")
	case ast.ShowPlacement:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT POLICY")
	case ast.ShowPlacementForDatabase:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR DATABASE")
	case ast.ShowPlacementForTable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR TABLE")
	case ast.ShowPlacementForPartition:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT FOR PARTITION")
	case ast.ShowPlacementLabels:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW PLACEMENT LABELS")
	case ast.ShowRegions:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SHOW TABLE REGIONS")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("Unsupported SHOW type")
}

func verifyChange(stmt *ast.ChangeStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("CHANGE NODE STATE")
}

func verifyLoadStats(stmt *ast.LoadStatsStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("LOAD STATS")
}

func verifySplitRegion(stmt *ast.SplitRegionStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SPLIT REGION")
}

func verifyLoadData(stmt *ast.LoadDataStmt) error {
	if stmt.FileLocRef == ast.FileLocClient {
		return nil
	}
	// Only support load remote data from trusted sources.
	whiteList := config.GetGlobalConfig().Security.RemoteDataWhiteList
	for _, whiteListedPrefix := range whiteList {
		if strings.HasPrefix(stmt.Path, whiteListedPrefix) {
			return nil
		}
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("LOAD DATA INFILE")
}

func verifyAdmin(stmt *ast.AdminStmt) error {
	switch stmt.Tp {
	case
		ast.AdminShowDDL,
		ast.AdminCheckTable,
		ast.AdminShowDDLJobs,
		ast.AdminCancelDDLJobs,
		ast.AdminCheckIndex,
		ast.AdminRecoverIndex,
		ast.AdminCleanupIndex,
		ast.AdminCheckIndexRange,
		ast.AdminShowDDLJobQueries,
		ast.AdminShowDDLJobQueriesWithRange,
		ast.AdminChecksumTable,
		ast.AdminShowNextRowID,
		ast.AdminReloadExprPushdownBlacklist,
		ast.AdminReloadOptRuleBlacklist,
		ast.AdminFlushBindings,
		ast.AdminCaptureBindings,
		ast.AdminEvolveBindings,
		ast.AdminReloadBindings,
		ast.AdminReloadStatistics,
		ast.AdminFlushPlanCache:
		// TODO: Batch Task
		// ast.AdminShowBatchTasks,
		// ast.AdminCancelBatchTasks:
		return nil
	case ast.AdminPluginDisable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN PLUGIN DISABLE")
	case ast.AdminPluginEnable:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN PLUGIN ENABLE")
	case ast.AdminShowSlow:
		return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("ADMIN SHOW SLOW")
	}
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause(fmt.Sprintf("Unsupported statement: %T", stmt))
}

func verifySetConfig(_ *ast.SetConfigStmt) error {
	return dbterror.ErrNotSupportedOnServerless.GenWithStackByCause("SET CONFIG")
}

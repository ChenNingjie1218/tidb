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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

var (
	// sysMap record original value of sysvar
	sysMap      map[string]string
	mapMutex    sync.Mutex
	sysMapMutex sync.RWMutex
)

// GetOrigVar Get original system variables
func GetOrigVar(name string) string {
	sysMapMutex.RLock()
	defer sysMapMutex.RUnlock()
	return sysMap[name]
}

// enableConfigMode enables Config Based SEM. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func enableConfigMode() {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	sysMap = make(map[string]string)
	variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.On)
	// Set password validation rules.
	variable.SetSysVarMin(variable.ValidatePasswordLength, 8)
	variable.SetSysVarMin(variable.ValidatePasswordMixedCaseCount, 1)
	variable.SetSysVarMin(variable.ValidatePasswordNumberCount, 1)
	variable.SetSysVarPossibleValues(variable.ValidatePasswordPolicy, []string{"MEDIUM", "STRONG"})

	cfg := config.GetGlobalConfig()
	for _, resVar := range cfg.Security.SEM.RestrictedVariables {
		if resVar.RestrictionType == "replace" {
			if variable.IsVarExists(resVar.Name) {
				originalValue := variable.GetSysVar(resVar.Name).Value
				if _, ok := sysMap[resVar.Name]; !ok {
					sysMap[resVar.Name] = originalValue
					variable.SetSysVar(resVar.Name, resVar.Value)
				}
			}
		}
	}
}

// disableConfigMode disables SEM config mode. This is intended to be used by the test-suite.
// Dynamic configuration by users may be a security risk.
func disableConfigMode() {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	variable.SetSysVar(variable.TiDBEnableEnhancedSecurity, variable.Off)
	for varName, varValue := range sysMap {
		variable.SetSysVar(varName, varValue)
	}
	sysMap = nil
	logutil.BgLogger().Info("tidb-server is operating with security enhanced mode (SEM) disabled")
}

// isConfigMode checks if the server is running in Config Based SEM mode.
func isConfigMode() bool {
	return atomic.LoadInt32(&semLevel) == levelConfigVal
}

// configModeInvisibleSchema returns true if the dbName needs to be hidden
// when sem is enabled.
func configModeInvisibleSchema(dbName string) bool {
	cfg := config.GetGlobalConfig()
	for _, dbn := range cfg.Security.SEM.RestrictedDatabases {
		if strings.EqualFold(dbName, dbn) {
			return true
		}
	}
	return false
}

// configModeInvisibleTable returns true if the table needs to be hidden
// when sem is enabled.
func configModeInvisibleTable(dbLowerName, tblLowerName string) bool {
	cfg := config.GetGlobalConfig()
	if IsInvisibleSchema(dbLowerName) {
		return true
	}

	for _, tbl := range cfg.Security.SEM.RestrictedTables {
		if strings.EqualFold(dbLowerName, tbl.Schema) && strings.EqualFold(tblLowerName, tbl.Name) {
			return true
		}
	}
	return false
}

// configModeInvisibleSysVar returns true if the sysvar needs to be hidden
func configModeInvisibleSysVar(varNameInLower string) bool {
	cfg := config.GetGlobalConfig()
	for _, resvarName := range cfg.Security.SEM.RestrictedVariables {
		if strings.EqualFold(varNameInLower, resvarName.Name) {
			if resvarName.RestrictionType == "hidden" {
				return true
			}
		}
	}
	return false
}

// configModeReadOnlySysVar returns true if the sysvar is read-only
func configModeReadOnlySysVar(varNameInLower string) bool {
	cfg := config.GetGlobalConfig()
	for _, resvarName := range cfg.Security.SEM.RestrictedVariables {
		if strings.EqualFold(varNameInLower, resvarName.Name) {
			return resvarName.Readonly
		}
	}
	return false
}

// configModeReadOnlyGlobalSysVar returns true if the sysvar is read-only
func configModeReadOnlyGlobalSysVar(varNameInLower string) bool {
	if !IsReadOnlySysVar(varNameInLower) {
		return false
	}
	cfg := config.GetGlobalConfig()
	for _, resvarName := range cfg.Security.SEM.RestrictedVariables {
		if strings.EqualFold(varNameInLower, resvarName.Name) && strings.EqualFold(resvarName.Scope, "global") {
			return resvarName.Readonly
		}
	}
	return false
}

// configModeReplacedSysVar returns true if the sys var need to be replaced
func configModeReplacedSysVar(varNameInLower string) bool {
	cfg := config.GetGlobalConfig()
	for _, resvarName := range cfg.Security.SEM.RestrictedVariables {
		if varNameInLower == resvarName.Name && resvarName.RestrictionType == "replace" {
			return true
		}
	}
	return false
}

// configModeRestrictedPrivilege returns true if the privilege shuld not be satisfied by SUPER
// As most dynamic privileges are.
func configModeRestrictedPrivilege(privNameInUpper string) bool {
	switch privNameInUpper {
	case
		placementAdmin,
		backupAdmin,
		restoreAdmin,
		resourceGroupAdmin:
		return true
	}
	return len(privNameInUpper) >= 12 && privNameInUpper[:11] == restrictedPriv
}

// configModeStaticPermissionRestricted Returning true when statically permissions are hit first in the list.
func configModeStaticPermissionRestricted(privType mysql.PrivilegeType) bool {
	cfg := config.GetGlobalConfig()
	restrictedPrivileges := cfg.Security.SEM.RestrictedStaticPrivileges
	_, ok := restrictedPrivileges[privType]
	return ok
}

// configModeRestrictedStatus return the actual restricted status of the status variable.
// false indicates no restriction.
func configModeRestrictedStatus(varName string) (bool, *config.RestrictedStatus) {
	cfg := config.GetGlobalConfig()
	status := cfg.Security.SEM.RestrictedStatus
	for _, state := range status {
		if varName == state.Name {
			return true, &state
		}
	}
	return false, &config.RestrictedStatus{}
}

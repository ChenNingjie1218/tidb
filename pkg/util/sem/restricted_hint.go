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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// IsRestrictedHint checks if the hint is allowed.
func IsRestrictedHint(hintNameLower string) error {
	if intest.InTest {
		return nil
	}
	if !IsEnabled() {
		return nil
	}

	switch hintNameLower {
	case "memory_quota":
		if IsInvisibleSysVar(variable.TiDBMemQuotaQuery) || IsReadOnlySysVar(variable.TiDBMemQuotaQuery) {
			return errors.New("MEMORY_QUOTA() is not supported on TiDB Serverless")
		}
	case "resource_group":
		return errors.New("RESOURCE_GROUP() is not supported on TiDB Serverless")
	case "read_consistent_replica":
		if IsInvisibleSysVar(variable.TiDBReplicaRead) || IsReadOnlySysVar(variable.TiDBReplicaRead) {
			return errors.New("READ_CONSISTENT_REPLICA() is not supported on TiDB Serverless")
		}
	case "max_execution_time":
		if IsInvisibleSysVar(variable.MaxExecutionTime) || IsReadOnlySysVar(variable.MaxExecutionTime) {
			return errors.New("MAX_EXECUTION_TIME() is not supported TiDB Serverless")
		}
	}
	return nil
}

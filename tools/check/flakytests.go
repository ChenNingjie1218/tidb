// Copyright 2025 PingCAP, Inc.
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

package main

import (
	"slices"
)

var flakyTests = map[string][]string{
	"./pkg/executor/test/splittest": {
		"TestShowTableRegion",
		"TestClusterIndexShowTableRegion",
	},
	"./pkg/statistics/handle/cache/internal/lfu": {
		"TestMemoryControlWithUpdate",
	},
	"./pkg/executor/test/analyzetest": {
		"TestKillAutoAnalyze",
		"TestKillAutoAnalyzeIndex",
	},
	"./pkg/infoschema/test/clustertablestest": {
		"TestMDLViewIDConflict",
	},
	"./pkg/statistics/handle/storage": {
		"TestGCExtendedStats",
	},
	"./pkg/expression/integration_test": {
		"TestGetLock",
	},
	"./pkg/ttl/ttlworker": {
		"TestFinishJob",
		"TestJobHeartBeatFailNotBlockOthers",
		"TestHeartBeatErrorNotBlockOthers",
		"TestTaskCancelledAfterHeartbeatTimeout",
	},
	"./pkg/ddl/tests/partition": {
		"TestMultiSchemaPartitionByGlobalIndex",
		"TestPartitionErrorCode",
		"TestBackfillConcurrentDML",
		"TestMultiSchemaModifyColumn",
	},
	"./pkg/executor/staticrecordset": {
		"TestFinishStmtError",
	},
	"./pkg/server": {
		"TestMemoryLeak",
	},
	"./pkg/executor/test/aggregate": {
		"TestRandomPanicConsume",
	},
	"./pkg/planner/core/casetest/instanceplancache": {
		// https://github.com/pingcap/tidb/issues/57514
		"TestInstancePlanCacheConcurrencySysbench",
	},
	"./pkg/executor": {
		"TestContextCancelWhenReadFromCopIterator",
	},
}

func isFlakyTest(task task) bool {
	if tests, ok := flakyTests[task.pkg]; ok {
		return slices.Contains(tests, task.test)
	}
	return false
}

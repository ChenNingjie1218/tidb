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

package serverless

const (
	// LabelClusterID is the label key for cluster id.
	LabelClusterID = "serverless_cluster_id"
	// LabelProjectID is the label key for project id.
	LabelProjectID = "serverless_project_id"
	// LabelTenantID is the label key for tenant id.
	LabelTenantID = "serverless_tenant_id"
	// LabelIsBranch is the label key to check if the cluster is a branch of another cluster.
	LabelIsBranch = "serverless_is_branch"
	// LabelIsBranchBootstrapped is the label key to check if the branch is bootstrapped.
	LabelIsBranchBootstrapped = "serverless_is_branch_bootstrapped"
	// LabelTsoKeyspaceGroupID is the label key for tso keyspace group id.
	LabelTsoKeyspaceGroupID = "tso_keyspace_group_id"
	// LabelIsBootstrappedForRestore is the label key to check if the restored cluster/branch is bootstrapped.
	LabelIsBootstrappedForRestore = "serverless_is_bootstrapped_for_restore"

	// SlowLogServerlessTenantIDKey is slow log field name.
	SlowLogServerlessTenantIDKey = "Serverless_tenant_ID"
	// SlowLogServerlessProjectIDKey is slow log field name.
	SlowLogServerlessProjectIDKey = "Serverless_project_ID"
	// SlowLogServerlessClusterIDKey is slow log field name.
	SlowLogServerlessClusterIDKey = "Serverless_cluster_ID"
)

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

package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	// EnvVarExecID is the overwritten execID of the ddl worker.
	EnvVarExecID = "EXEC_ID"
	// EnvVarTiDBPool is the overwritten tidb pool of the ddl worker.
	EnvVarTiDBPool = "TIDB_POOL"
)

// TiDBWorker is the config for TiDB worker.
type TiDBWorker struct {
	// Enable indicates whether to start the TiDB worker manager.
	Enable bool `toml:"enable" json:"enable"`
	// Role indicates the role of the TiDB worker.
	Role string `toml:"role" json:"role"`
	// TidbPool specifies pool of the tidb worker.
	TidbPool string `toml:"tidb-pool" json:"tidb-pool"`
	// APIServerAddr specifies the address of the TiDB worker API server.
	APIServerAddr string `toml:"api-server" json:"api-server"`
	// ExecID specifies execID when TiDB is running as ddl worker.
	ExecID string `toml:"exec-id" json:"exec-id"`

	// LocalMode allows tidb to skip the tidb-worker registry and return a local bg task config.
	LocalMode LocalMode `toml:"local-mode" json:"local-mode"`
}

// LocalMode is the config for local mode.
type LocalMode struct {
	// Enable indicates whether to start the TiDB worker manager in local mode.
	Enable bool `toml:"enable" json:"enable"`
	// BgTaskConfig specifies the response of the TiDB worker API server in local mode.
	BgTaskConfig map[string]BgTaskConfig `toml:"bg-task-config" json:"bg-task-config"`
	// StaticExecID fixes the execID of the task generated in local mode.
	StaticExecID bool `toml:"static-exec-id" json:"static-exec-id"`
}

// BgTaskConfig is the config for background task.
type BgTaskConfig struct {
	// Paused indicates whether the specific background task is paused.
	Paused bool `toml:"paused" json:"paused"`
	// WorkerCount indicates the number of workers for the specific background task.
	WorkerCount int `toml:"worker-count" json:"worker-count"`
	// EnableAutoScale indicates whether to enable auto scale for the specific background task.
	EnableAutoScale bool `toml:"enable-auto-scale" json:"enable-auto-scale"`
}

const (
	// RoleMaster is the role for user tidb.
	RoleMaster = "master"
	// RoleGCWorker is the role for GC worker.
	RoleGCWorker = "gc"
	// RoleGCV2Worker is the role for GCV2 worker.
	RoleGCV2Worker = "gcv2"
	// RoleDDLWorker is the role for DDL worker.
	RoleDDLWorker = "ddl"
	// RoleBatchWorker is the role for batch worker.
	RoleBatchWorker = "batch"
	// RoleImportIntoWorker is the role for import-into worker.
	RoleImportIntoWorker = "import-into"
	// RoleSharedWorker is the role for batch, DDL and import-into worker.
	RoleSharedWorker = "shared"
	// RoleRemoteQueryWorker is the role for remote query worker.
	RoleRemoteQueryWorker = "remote-query"
	// RoleTTLTaskWorker is the role for batch worker.
	RoleTTLTaskWorker = "ttl"
	// RoleAutoAnalyzeWorker is the role for auto analyze worker.
	RoleAutoAnalyzeWorker = "auto-analyze"
)

// defaultTiDBWorker creates a new TiDBWorker.
func defaultTiDBWorker() TiDBWorker {
	return TiDBWorker{
		Enable:        false,
		Role:          RoleMaster,
		TidbPool:      "tidb-pool",
		APIServerAddr: "http://scaler-svc.tidb-worker:9080",
		LocalMode:     defaultLocalMode(),
	}
}

// defaultLocalMode creates a new LocalMode.
func defaultLocalMode() LocalMode {
	return LocalMode{
		Enable:       false,
		BgTaskConfig: make(map[string]BgTaskConfig),
		StaticExecID: false,
	}
}

// Valid validates the TiDBWorker config.
func (w *TiDBWorker) Valid(c *Config) error {
	// Skip validation if TiDB worker is disabled.
	if !w.Enable {
		return nil
	}
	w.Role = strings.ToLower(w.Role)
	switch w.Role {
	case RoleMaster:
	case RoleGCWorker:
	case RoleGCV2Worker:
	case RoleDDLWorker, RoleBatchWorker, RoleImportIntoWorker, RoleSharedWorker:
		// Skip running GC worker on DDL worker.
		c.SkipGCWorker = true
		// Overwrite execID if its set in env.
		if execID := os.Getenv(EnvVarExecID); execID != "" {
			w.ExecID = execID
		}
		// Overwrite tidb pool if it's set in env.
		if tidbPool := os.Getenv(EnvVarTiDBPool); tidbPool != "" {
			w.TidbPool = tidbPool
		}
	case RoleRemoteQueryWorker:
		c.SkipGCWorker = true
		// Overwrite execID if its set in env.
		if execID := os.Getenv(EnvVarExecID); execID != "" {
			w.ExecID = execID
		}
	case RoleTTLTaskWorker:
	case RoleAutoAnalyzeWorker:
		c.SkipGCWorker = true
		// Enable auto analyze system table when running as auto analyze worker.
		c.EnableAutoAnalyzeSysTable = true
	default:
		return fmt.Errorf("invalid tidb worker role %s", w.Role)
	}
	return nil
}

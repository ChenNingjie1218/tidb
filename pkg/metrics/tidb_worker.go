// Copyright 2024 PingCAP, Inc.
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// WorkerTaskCounter records when tasks are initialized, registered, recycled, or aborted.
	WorkerTaskCounter *prometheus.CounterVec
	// InitializeWorkerTasks represents when worker tasks are initialized when tidb starts,
	// this applies to gc and gcv2 tasks.
	InitializeWorkerTasks = "init"
	// RegisterWorkerTask represents when tidb registers a task and request a worker from the manager.
	RegisterWorkerTask = "register"
	// RecycleWorkerTask represents when a recycle request is sent to the manager.
	RecycleWorkerTask = "recycle"
	// AbortWorkerTask represents when a specific type of task is aborted.
	AbortWorkerTask = "abort"
)

// InitTiDBWorkerMetrics initializes metrics for TiDB worker.
func InitTiDBWorkerMetrics() {
	WorkerTaskCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb_worker",
			Subsystem: "worker",
			Name:      "task_counter",
			Help:      "Counter of worker tasks.",
		}, []string{LblType, LblAction, LblTaskID},
	)
}

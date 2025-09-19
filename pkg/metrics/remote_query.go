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
	// RemoteQuerySessionGauge is the gauge of remote query sessions.
	RemoteQuerySessionGauge prometheus.Gauge
	// RemoteQuerySessionCounter is the counter of remote query sessions.
	RemoteQuerySessionCounter *prometheus.CounterVec
	// RemoteQueryServerCounter is the counter of remote query servers.
	RemoteQueryServerCounter *prometheus.CounterVec
	// RemoteQueryRecordSetCounter is the counter of remote query record sets.
	RemoteQueryRecordSetCounter *prometheus.CounterVec
	// RemoteQueryRecordSetDuration is the histogram of processing time in remote query record set.
	RemoteQueryRecordSetDuration *prometheus.HistogramVec
	// RemoteQueryWorkerCounter is the counter of remote query workers.
	RemoteQueryWorkerCounter *prometheus.CounterVec
	// RemoteQueryWorkerDuration is the histogram of processing time in remote query worker.
	RemoteQueryWorkerDuration *prometheus.HistogramVec
)

// InitRemoteQueryMetrics initializes metrics for remote query.
func InitRemoteQueryMetrics() {
	RemoteQuerySessionGauge = NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "remote_query",
			Name:      "session_gauge",
			Help:      "Gauge of remote query sessions.",
		},
	)

	RemoteQuerySessionCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_query",
			Name:      "session_counter",
			Help:      "Counter of remote query sessions.",
		}, []string{LblType},
	)

	RemoteQueryServerCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_query",
			Name:      "server_counter",
			Help:      "Counter of remote query servers.",
		}, []string{LblType, LblResult},
	)

	RemoteQueryRecordSetCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "remote_query",
			Name:      "recordset_counter",
			Help:      "Counter of remote query record sets.",
		}, []string{LblType, LblResult},
	)

	RemoteQueryRecordSetDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "remote_query",
			Name:      "recordset_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in remote query record set.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 25), // 1ms-1h
		}, []string{LblType},
	)

	RemoteQueryWorkerCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb_worker",
			Subsystem: "remote_query",
			Name:      "worker_counter",
			Help:      "Counter of remote query workers.",
		}, []string{LblType, LblResult},
	)

	RemoteQueryWorkerDuration = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb_worker",
			Subsystem: "remote_query",
			Name:      "worker_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in remote query worker.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 25), // 1ms-1h
		}, []string{LblType},
	)
}

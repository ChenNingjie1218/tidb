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

import "github.com/prometheus/client_golang/prometheus"

var (
	// VPAReportCounter records the counter of VPA reports.
	VPAReportCounter *prometheus.CounterVec
	// VPAScaleMemoryCounter records the counter of VPA scale memory.
	VPAScaleMemoryCounter *prometheus.CounterVec
	// VPAMemoryGauge records the gauge of VPA memory.
	VPAMemoryGauge *prometheus.GaugeVec
)

// InitVPAMetrics initializes metrics for VPA.
func InitVPAMetrics() {
	VPAReportCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb_vpa",
			Subsystem: "vpa",
			Name:      "report_counter",
			Help:      "Counter of VPA reports.",
		}, []string{"result"},
	)

	VPAScaleMemoryCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb_vpa",
			Subsystem: "vpa",
			Name:      "scale_memory_counter",
			Help:      "Counter of VPA scale memory.",
		}, []string{"result"},
	)

	VPAMemoryGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb_vpa",
			Subsystem: "vpa",
			Name:      "memory_gauge",
			Help:      "Gauge of VPA memory.",
		}, []string{"type"},
	)
}

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

package telemetryv2

import (
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

func applyUpdatesVectorSearch(sctx sessionctx.Context) {
	isOwner := sctx.IsDDLOwner()
	// The following metrics are identical for all TiDB Servers in the cluster,
	// so no need to collect them when instance is not a DDL owner.
	metrics.VectorSearchVectorColumnDimension.Enabled.Store(isOwner)
	metrics.VectorSearchTableNumsWithVectorData.Enabled.Store(isOwner)
	metrics.VectorSearchTableNumsWithVectorIndex.Enabled.Store(isOwner)
	if !isOwner {
		// All telemetry metrics in Vector Search only need to be collected by
		// one TiDB server in the cluster, no need to continue.
		return
	}

	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)

	tablesWithVectorData := 0
	tablesWithVectorIndex := 0

	for _, db := range is.AllSchemas() {
		tblInfos, err := is.SchemaTableInfos(context.Background(), db.Name)
		if err != nil {
			return
		}
		for _, table := range tblInfos {

			hasVectorData := false
			hasVectorIndex := false
			for _, col := range table.Columns {
				if col.FieldType.GetType() == mysql.TypeTiDBVectorFloat32 {
					hasVectorData = true

					if col.FieldType.GetFlen() != types.UnspecifiedLength {
						metrics.VectorSearchVectorColumnDimension.Collector.
							Observe(float64(col.FieldType.GetFlen()))
					}
				}
			}

			for _, idx := range table.Indices {
				if idx.VectorInfo != nil {
					hasVectorIndex = true
					break
				}
			}

			if hasVectorData {
				tablesWithVectorData++
			}
			if hasVectorIndex {
				tablesWithVectorIndex++
			}
		}
	}

	metrics.VectorSearchTableNumsWithVectorData.Collector.
		Set(float64(tablesWithVectorData))

	metrics.VectorSearchTableNumsWithVectorIndex.Collector.
		Set(float64(tablesWithVectorIndex))
}

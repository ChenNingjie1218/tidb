package core

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestConvertToIndexScanBlocksSPFreshIndexPath(t *testing.T) {
	ctx := mock.NewContext()
	ds := logicalop.DataSource{}.Init(ctx, 0)

	vecColInfo := &model.ColumnInfo{
		ID:   1,
		Name: pmodel.NewCIStr("vec"),
	}
	ds.TableInfo = &model.TableInfo{
		ID:      100,
		Name:    pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{vecColInfo},
	}
	ds.Columns = []*model.ColumnInfo{vecColInfo}
	ds.DBName = pmodel.NewCIStr("test")
	ds.PhysicalTableID = ds.TableInfo.ID
	ds.PushedDownConds = nil
	ds.SetSchema(expression.NewSchema(&expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}))
	ds.SetStats(&property.StatsInfo{RowCount: 100, ColNDVs: map[int64]float64{}})

	idx := &model.IndexInfo{
		ID:   1,
		Name: pmodel.NewCIStr("idx_vec"),
		VectorInfo: &model.VectorIndexInfo{
			Kind:           model.VectorIndexKindSPFresh,
			Dimension:      3,
			DistanceMetric: model.DistanceMetricCosine,
		},
		Columns: []*model.IndexColumn{{
			Name:   pmodel.NewCIStr("vec"),
			Offset: 0,
		}},
	}

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	cases := []struct {
		name         string
		prop         func() *property.PhysicalProperty
		isMatchProp  bool
		enableRemote bool

		expectInvalid bool
		expectScan    bool
		expectNilErr  bool
	}{
		{
			name: "non_vector_query",
			prop: func() *property.PhysicalProperty {
				return &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
			},
			isMatchProp:   false,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_mismatch",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   false,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_non_root_task",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.CopSingleReadTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   true,
			enableRemote:  true,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_root_task_match_remote_disabled",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:   true,
			enableRemote:  false,
			expectInvalid: true,
			expectNilErr:  true,
		},
		{
			name: "vector_query_root_task_match_remote_enabled",
			prop: func() *property.PhysicalProperty {
				prop := &property.PhysicalProperty{
					TaskTp:      property.RootTaskType,
					ExpectedCnt: math.MaxFloat64,
				}
				prop.VectorProp.VectorHelper = &expression.VectorHelper{}
				prop.VectorProp.TopK = 10
				prop.VectorProp.Vec = vec
				return prop
			},
			isMatchProp:  true,
			enableRemote: true,
			expectScan:   true,
			expectNilErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				if tc.enableRemote {
					conf.TiKVAPIServiceAddr = "http://127.0.0.1:1234"
				} else {
					conf.TiKVAPIServiceAddr = ""
				}
			})

			candidate := &candidatePath{
				path: &util.AccessPath{
					Index:        idx,
					IsSingleScan: true,
				},
				isMatchProp: tc.isMatchProp,
			}
			task, err := convertToIndexScan(ds, tc.prop(), candidate, nil)
			if tc.expectNilErr {
				require.NoError(t, err)
			}
			if tc.expectInvalid {
				require.Same(t, base.InvalidTask, task)
			}
			if tc.expectScan {
				require.False(t, task.Invalid())
				scan, ok := task.Plan().(*PhysicalSPFreshVectorScan)
				require.True(t, ok)
				require.Equal(t, uint32(10), scan.TopK)
				require.Equal(t, vec.Len(), scan.QueryVec.Len())
				_, ok = scan.CloneForPlanCache(ctx)
				require.False(t, ok)
			}
		})
	}
}

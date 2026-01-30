package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestGetPhysTopNRootVectorPropCandidate(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{TableCacheStatusType: model.TableCacheStatusDisable}
	ds.PossibleAccessPaths = []*util.AccessPath{{StoreType: kv.TiKV}}
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.PushedDownConds = nil

	lt := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: distFn}},
		Count:   1,
	}.Init(ctx, 0)
	lt.SetSchema(expression.NewSchema(vecCol))
	lt.SetChildren(ds)

	// Ensure we exercise the "forcibly push down TopN/LIMIT" branch.
	require.True(t, pushLimitOrTopNForcibly(lt))

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	found := false
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			require.Equal(t, uint32(1), childProp.VectorProp.TopK)
			found = true
		}
	}
	require.True(t, found, "expect RootTaskType + VectorProp candidate")
}

func TestGetPhysTopNRootVectorPropCandidateRespectsLimitToCop(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().LimitPushDownThreshold = 100

	vec, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)

	vecCol := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vecConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(vec),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	distFn := expression.NewFunctionInternal(ctx, ast.VecCosineDistance, types.NewFieldType(mysql.TypeFloat), vecCol, vecConst)

	ds := logicalop.DataSource{}.Init(ctx, 0)
	ds.TableInfo = &model.TableInfo{TableCacheStatusType: model.TableCacheStatusDisable}
	ds.PossibleAccessPaths = []*util.AccessPath{{StoreType: kv.TiKV}}
	ds.SetSchema(expression.NewSchema(vecCol))
	ds.PushedDownConds = nil

	lt := logicalop.LogicalTopN{
		ByItems:          []*util.ByItems{{Expr: distFn}},
		Count:            1,
		PreferLimitToCop: true,
	}.Init(ctx, 0)
	lt.SetSchema(expression.NewSchema(vecCol))
	lt.SetChildren(ds)

	require.True(t, pushLimitOrTopNForcibly(lt))

	plans := getPhysTopN(lt, &property.PhysicalProperty{})
	for _, p := range plans {
		topN, ok := p.(*PhysicalTopN)
		if !ok {
			continue
		}
		childProp := topN.GetChildReqProps(0)
		if childProp.TaskTp == property.RootTaskType && childProp.VectorProp.VectorHelper != nil {
			t.Fatalf("unexpected RootTaskType + VectorProp candidate when LIMIT_TO_COP is effective")
		}
	}
}

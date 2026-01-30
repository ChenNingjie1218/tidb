package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

// tryEnableSPFreshVectorSearchDistanceProjection enables a SPFresh-only distance projection rewrite:
//   - Always: ensure SPFreshScan outputs a virtual distance column and rewrite the ordering to use it,
//     so TiDB won't re-calculate vec_*_distance in execution.
//   - Optional: if the vector column becomes unused after the rewrite, remove it from SPFreshScan output,
//     so `/vector_search.required_columns` won't contain it.
//
// This is intentionally isolated from the existing TiFlash/HNSW logic (task.go:tryEnableVectorSearchDistanceProjection).
func tryEnableSPFreshVectorSearchDistanceProjection(plan base.PhysicalPlan) {
	spfreshDistanceProjTraverse(plan, nil)
}

func spfreshDistanceProjTraverse(p base.PhysicalPlan, parent base.PhysicalPlan) {
	if topN, ok := p.(*PhysicalTopN); ok {
		tryEnableSPFreshDistanceProjectionOnTopN(parent, topN)
	}
	for _, child := range p.Children() {
		spfreshDistanceProjTraverse(child, p)
	}
}

func tryEnableSPFreshDistanceProjectionOnTopN(parent base.PhysicalPlan, topN *PhysicalTopN) {
	if topN == nil || len(topN.Children()) != 1 {
		return
	}
	if len(topN.ByItems) != 1 || topN.ByItems[0].Desc {
		// Vector search only supports single-item ascending order for now.
		return
	}
	var (
		scan       *PhysicalSPFreshVectorScan
		bottomProj *PhysicalProjection
		orderIdx   int
		vs         *expression.VectorHelper
	)

	switch child := topN.Children()[0].(type) {
	case *PhysicalSPFreshVectorScan:
		scan = child
		// TopN(order by vec_*_distance(vec_col, const)) -> SPFreshScan
		vs = expression.ExtractVectorHelper(topN.ByItems[0].Expr)
	case *PhysicalProjection:
		// TopN(order by Column(distance)) -> Projection(vec_*_distance(vec_col, const)) -> SPFreshScan
		bottomProj = child
		if len(bottomProj.Children()) != 1 {
			return
		}
		var ok bool
		scan, ok = bottomProj.Children()[0].(*PhysicalSPFreshVectorScan)
		if !ok {
			return
		}
		orderByCol, ok := topN.ByItems[0].Expr.(*expression.Column)
		if !ok {
			return
		}
		orderIdx = orderByCol.Index
		if orderIdx < 0 || orderIdx >= len(bottomProj.Exprs) {
			return
		}
		vs = expression.ExtractVectorHelper(bottomProj.Exprs[orderIdx])
	default:
		return
	}

	if scan == nil || scan.Index == nil || scan.Index.VectorInfo == nil || scan.Index.VectorInfo.Kind != model.VectorIndexKindSPFresh {
		return
	}
	if vs == nil || vs.Column == nil {
		return
	}
	vecColID := vs.Column.ID

	distanceIdx := ensureSPFreshDistanceColumn(scan)
	if distanceIdx < 0 {
		return
	}
	scan.EnableDistanceProj = true

	// Rewrite the ordering to use worker-returned distance, avoiding TiDB-side re-calculation.
	distCol := scan.Schema().Columns[distanceIdx].Clone().(*expression.Column)
	distCol.Index = distanceIdx
	if bottomProj == nil {
		topN.ByItems[0].Expr = distCol
	} else {
		// Replace the heavy distance function in the projection with a direct distance column reference.
		bottomProj.Exprs[orderIdx] = distCol
	}

	// Optional: remove the vector column from scan output when it becomes unused after the rewrite.
	if bottomProj != nil {
		// The TopN reads from the projection output. If the projection doesn't need vec in its expressions
		// after the rewrite, and nothing above TopN reads vec, we can remove vec from scan output.
		vecOutIdx := findSchemaColumnIdxByID(bottomProj.Schema(), vecColID)
		for i, expr := range bottomProj.Exprs {
			// Ignore the passthrough output column itself. We'll remove it if it's unused above.
			if i == vecOutIdx {
				continue
			}
			if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
				return col.ID == vecColID
			}) {
				return
			}
		}
		// Generated columns may still depend on vec.
		if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
			return
		}
		if vecOutIdx >= 0 {
			// Only remove vec from the projection output if nothing above TopN uses it.
			proj, ok := parent.(*PhysicalProjection)
			if !ok || proj == nil {
				return
			}
			for _, expr := range proj.Exprs {
				if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
					return col.ID == vecColID
				}) {
					return
				}
			}
			bottomProj.Exprs = removeExprByIdx(bottomProj.Exprs, vecOutIdx)
			removeSchemaColumnByIdx(bottomProj.Schema(), vecOutIdx)
			// Keep TopN schema aligned with its child output.
			if topNSchemaIdx := findSchemaColumnIdxByID(topN.Schema(), vecColID); topNSchemaIdx >= 0 {
				removeSchemaColumnByIdx(topN.Schema(), topNSchemaIdx)
				for _, expr := range proj.Exprs {
					for _, col := range expression.ExtractColumns(expr) {
						col.Index = topN.Schema().ColumnIndex(col)
					}
				}
			}
			// Refresh TopN's order-by column index if it's a direct column reference.
			if orderByCol, ok := topN.ByItems[0].Expr.(*expression.Column); ok {
				orderByCol.Index = bottomProj.Schema().ColumnIndex(orderByCol)
			}
		}
		scanColIdx := findColumnInfoIdxByID(scan.Columns, vecColID)
		scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), vecColID)
		if scanColIdx < 0 || scanSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
			return
		}
		scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
		removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)

		// Rewire column indexes in projection expressions based on the updated child schema.
		for _, expr := range bottomProj.Exprs {
			for _, col := range expression.ExtractColumns(expr) {
				col.Index = scan.Schema().ColumnIndex(col)
			}
		}
		return
	}

	// Simple removal case without a bottom projection: use parent Projection to decide vec is unused.
	proj, ok := parent.(*PhysicalProjection)
	if !ok || proj == nil {
		return
	}
	for _, expr := range proj.Exprs {
		if expression.HasColumnWithCondition(expr, func(col *expression.Column) bool {
			return col.ID == vecColID
		}) {
			return
		}
	}
	// Generated columns may still depend on vec.
	if virtualGeneratedDependsOnColumnID(scan.Columns, vecColID) {
		return
	}
	scanColIdx := findColumnInfoIdxByID(scan.Columns, vecColID)
	scanSchemaIdx := findSchemaColumnIdxByID(scan.Schema(), vecColID)
	topNSchemaIdx := findSchemaColumnIdxByID(topN.Schema(), vecColID)
	if scanColIdx < 0 || scanSchemaIdx < 0 || topNSchemaIdx < 0 || scanColIdx != scanSchemaIdx {
		return
	}
	scan.Columns = removeColumnInfoByIdx(scan.Columns, scanColIdx)
	removeSchemaColumnByIdx(scan.Schema(), scanSchemaIdx)
	removeSchemaColumnByIdx(topN.Schema(), topNSchemaIdx)

	// Rewire projection expr column indexes based on the updated child schema.
	for _, expr := range proj.Exprs {
		for _, col := range expression.ExtractColumns(expr) {
			col.Index = topN.Schema().ColumnIndex(col)
		}
	}

	// The scan schema was changed. Refresh the final distance column index for TopN's order-by column.
	distanceIdx = scan.Schema().Len() - 1
	if distanceIdx >= 0 {
		topN.ByItems[0].Expr.(*expression.Column).Index = distanceIdx
	}
}

func ensureSPFreshDistanceColumn(scan *PhysicalSPFreshVectorScan) int {
	if scan.Schema() == nil {
		return -1
	}
	for _, c := range scan.Columns {
		if c != nil && c.ID == model.VirtualColVecSearchDistanceID {
			// Already appended.
			return scan.Schema().Len() - 1
		}
	}

	scan.Columns = append(scan.Columns, &model.ColumnInfo{
		ID:        model.VirtualColVecSearchDistanceID,
		FieldType: *types.NewFieldType(mysql.TypeDouble),
		Offset:    len(scan.Columns),
	})
	distCol := &expression.Column{
		UniqueID: scan.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeDouble),
		ID:       model.VirtualColVecSearchDistanceID,
	}
	scan.Schema().Append(distCol)
	return scan.Schema().Len() - 1
}

func findColumnInfoIdxByID(cols []*model.ColumnInfo, colID int64) int {
	for i := range cols {
		if cols[i] != nil && cols[i].ID == colID {
			return i
		}
	}
	return -1
}

func findSchemaColumnIdxByID(schema *expression.Schema, colID int64) int {
	if schema == nil {
		return -1
	}
	for i := range schema.Columns {
		if schema.Columns[i] != nil && schema.Columns[i].ID == colID {
			return i
		}
	}
	return -1
}

func removeColumnInfoByIdx(cols []*model.ColumnInfo, idx int) []*model.ColumnInfo {
	copy(cols[idx:], cols[idx+1:])
	cols[len(cols)-1] = nil
	return cols[:len(cols)-1]
}

func removeSchemaColumnByIdx(schema *expression.Schema, idx int) {
	copy(schema.Columns[idx:], schema.Columns[idx+1:])
	schema.Columns[len(schema.Columns)-1] = nil
	schema.Columns = schema.Columns[:len(schema.Columns)-1]
}

func removeExprByIdx(exprs []expression.Expression, idx int) []expression.Expression {
	copy(exprs[idx:], exprs[idx+1:])
	exprs[len(exprs)-1] = nil
	return exprs[:len(exprs)-1]
}

func virtualGeneratedDependsOnColumnID(cols []*model.ColumnInfo, colID int64) bool {
	var colName string
	for _, c := range cols {
		if c != nil && c.ID == colID {
			colName = c.Name.L
			break
		}
	}
	if colName == "" {
		return false
	}
	for _, c := range cols {
		if c == nil || !c.IsVirtualGenerated() || len(c.Dependences) == 0 {
			continue
		}
		if _, ok := c.Dependences[colName]; ok {
			return true
		}
	}
	return false
}

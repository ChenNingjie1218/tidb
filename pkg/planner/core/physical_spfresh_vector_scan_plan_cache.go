package core

import "github.com/pingcap/tidb/pkg/planner/core/base"

// CloneForPlanCache implements the base.Plan interface.
//
// Note: SPFresh vector search is not cacheable by default because query vectors are embedded in the plan.
func (p *PhysicalSPFreshVectorScan) CloneForPlanCache(_ base.PlanContext) (base.Plan, bool) {
	return nil, false
}

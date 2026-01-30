package vectorsearch

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func TestSPFreshVectorScanExplainNormalized(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = "http://127.0.0.1:1234"
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendMgr(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh")
	tk.MustExec("create table t_spfresh(vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "spfresh:COSINE(vec..[?], limit:?)"), strings.Join(rows, "\n"))
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "->Column#"), strings.Join(rows, "\n"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = ""
	})
	tk.MustExec("explain select * from t_spfresh order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows = getNormalizedPlanRows()
	require.False(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

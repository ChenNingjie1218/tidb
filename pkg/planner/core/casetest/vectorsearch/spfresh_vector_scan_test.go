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

func TestSPFreshVectorScanExplainNormalizedIdOnly(t *testing.T) {
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
	tk.MustExec("drop table if exists t_spfresh_id")
	tk.MustExec("create table t_spfresh_id(id int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_id ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_id values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]')")

	tk.MustExec("explain select id from t_spfresh_id order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanDefaultVectorIndexWhenRemoteBackendEnabled(t *testing.T) {
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

	tk.MustExec("drop table if exists t_spfresh_default")
	tk.MustExec("create table t_spfresh_default(vec vector(3))")
	// Without USING clause, VECTOR INDEX defaults to SPFresh when remote backend is configured.
	tk.MustExec("create vector index idx_vec on t_spfresh_default ((vec_cosine_distance(vec)))")
	tk.MustExec("insert into t_spfresh_default values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh_default order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanDefaultVectorIndexInlineWhenRemoteBackendEnabled(t *testing.T) {
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

	tk.MustExec("drop table if exists t_spfresh_default_inline")
	// Without USING clause, inline VECTOR INDEX defaults to SPFresh when remote backend is configured.
	tk.MustExec("create table t_spfresh_default_inline(vec vector(3), vector index idx_vec((vec_cosine_distance(vec))))")
	tk.MustExec("insert into t_spfresh_default_inline values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh_default_inline order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

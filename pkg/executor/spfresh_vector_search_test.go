package executor_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/spfresh/spfreshpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestSPFreshVectorSearchExec_RequestAndDecode(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[spfreshpb.VectorSearchRequest]
	var gotClusterID atomic.Value // string

	// Distances based on vec_l2_distance(vec, '[1,2,3]').
	rows := []struct {
		handle int64
		vec    string
		dist   float32
	}{
		{handle: 1, vec: `[1,2,3]`, dist: 0},
		{handle: 2, vec: `[1,2,4]`, dist: 1},
		{handle: 3, vec: `[1,2,5]`, dist: 2},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This test server is only for /vector_search; ignore other DDL-related requests if any.
		if r.URL.Path != "/vector_search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotClusterID.Store(r.URL.Query().Get("cluster_id"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &spfreshpb.VectorSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))
		gotReq.Store(reqPB)

		respPB := &spfreshpb.VectorSearchResponse{}
		for _, row := range rows {
			vec, err := types.ParseVectorFloat32(row.vec)
			require.NoError(t, err)
			vecBytes, err := codec.EncodeValue(nil, nil, types.NewVectorFloat32Datum(vec))
			require.NoError(t, err)

			var colVals [][]byte
			for _, col := range reqPB.RequiredColumns {
				switch col.Tp {
				case int32(mysql.TypeTiDBVectorFloat32):
					colVals = append(colVals, vecBytes)
				case int32(mysql.TypeLong):
					// Return empty bytes to force TiDB side to use origin default.
					colVals = append(colVals, nil)
				default:
					t.Fatalf("unexpected required column tp=%v", col.Tp)
				}
			}

			respPB.Rows = append(respPB.Rows, &spfreshpb.VectorSearchRow{
				HandleBytes:  kv.IntHandle(row.handle).Encoded(),
				Distance:     row.dist,
				ColumnValues: colVals,
			})
		}

		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = srv.URL
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_exec")
	tk.MustExec("create table t_spfresh_exec(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_exec ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_exec(id, vec) values (1,'[1,2,3]'), (2,'[1,2,4]'), (3,'[1,2,5]')")
	tk.MustExec("alter table t_spfresh_exec add column a int default 42")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_exec, idx_vec) */ id, a from t_spfresh_exec order by vec_l2_distance(vec, '[1,2,3]') limit 1 offset 2").
		Check(testkit.Rows("3 42"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)

	// LIMIT 1 OFFSET 2 => TopK = 3
	require.Equal(t, uint32(3), reqPB.TopK)
	require.True(t, tk.Session().GetSessionVars().StmtCtx.VectorSearchIsANNQuery)
	require.Equal(t, uint32(3), tk.Session().GetSessionVars().StmtCtx.VectorSearchTopK)

	clusterIDStr, _ := gotClusterID.Load().(string)
	require.NotEmpty(t, clusterIDStr)

	// Query vector bytes should be serialized without dims prefix.
	qv, err := types.ParseVectorFloat32(`[1,2,3]`)
	require.NoError(t, err)
	expected := qv.ZeroCopySerialize()[4:]
	require.True(t, bytes.Equal(expected, reqPB.QueryVectorF32Le), fmt.Sprintf("query_vector_f32_le mismatch: got=%v want=%v", reqPB.QueryVectorF32Le, expected))

	// id (PK handle) should be skipped from required_columns.
	// vec should also be skipped after distance projection rewrite.
	require.Len(t, reqPB.RequiredColumns, 1)
	require.Equal(t, int32(mysql.TypeLong), reqPB.RequiredColumns[0].Tp)
}

func TestSPFreshVectorSearchExec_ExplainAnalyzeStats(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	const (
		partitionsScanned uint64 = 7
		vectorsScanned    uint64 = 123
		tableLookupKeys   uint64 = 45
		tableLookupBytes  uint64 = 6789
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/vector_search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &spfreshpb.VectorSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))

		respPB := &spfreshpb.VectorSearchResponse{
			Rows: []*spfreshpb.VectorSearchRow{
				{
					HandleBytes:  kv.IntHandle(1).Encoded(),
					Distance:     0,
					ColumnValues: make([][]byte, len(reqPB.RequiredColumns)),
				},
			},
			Stats: &spfreshpb.VectorSearchStats{
				PartitionsScanned: partitionsScanned,
				VectorsScanned:    vectorsScanned,
				TableLookupKeys:   tableLookupKeys,
				TableLookupBytes:  tableLookupBytes,
			},
		}
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = srv.URL
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_stats")
	tk.MustExec("create table t_spfresh_stats(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_stats ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_stats values (1,'[0,0,0]')")

	rows := tk.MustQuery("explain analyze select /*+ USE_INDEX(t_spfresh_stats, idx_vec) */ id from t_spfresh_stats order by vec_l2_distance(vec, '[0,0,0]') limit 1").Rows()
	found := false
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		id, ok := row[0].(string)
		require.True(t, ok)
		if !strings.Contains(id, "SPFreshVectorScan") {
			continue
		}

		found = true
		var cols []string
		for _, c := range row {
			cols = append(cols, c.(string))
		}
		all := strings.Join(cols, " | ")
		require.Contains(t, all, "vector_search: {")
		require.Contains(t, all, fmt.Sprintf("partitions_scanned: %d", partitionsScanned))
		require.Contains(t, all, fmt.Sprintf("vectors_scanned: %d", vectorsScanned))
		require.Contains(t, all, fmt.Sprintf("table_lookup_keys: %d", tableLookupKeys))
		require.Contains(t, all, fmt.Sprintf("table_lookup_bytes: %d", tableLookupBytes))
	}
	require.True(t, found)
}

func TestSPFreshVectorSearchExec_Redirect(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var frontHits atomic.Int32
	var workerHits atomic.Int32

	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerHits.Add(1)
		reqBody, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &spfreshpb.VectorSearchRequest{}
		require.NoError(t, proto.Unmarshal(reqBody, reqPB))

		// Return an empty response to make the query return empty.
		respPB := &spfreshpb.VectorSearchResponse{
			Rows: []*spfreshpb.VectorSearchRow{},
		}
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(worker.Close)

	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		frontHits.Add(1)
		w.Header().Set("Location", worker.URL+"/vector_search")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(front.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = front.URL
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_redirect")
	tk.MustExec("create table t_spfresh_redirect(id int primary key, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_redirect ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_redirect values (1,'[0,0,0]')")

	// First query should go through the front server and then the worker.
	_ = tk.QueryToErr("select /*+ USE_INDEX(t_spfresh_redirect, idx_vec) */ id from t_spfresh_redirect order by vec_l2_distance(vec, '[0,0,0]') limit 1")
	require.Equal(t, int32(1), frontHits.Load())
	require.Equal(t, int32(1), workerHits.Load())

	// Second query should go directly to the worker due to redirect cache.
	_ = tk.QueryToErr("select /*+ USE_INDEX(t_spfresh_redirect, idx_vec) */ id from t_spfresh_redirect order by vec_l2_distance(vec, '[0,0,0]') limit 1")
	require.Equal(t, int32(1), frontHits.Load())
	require.Equal(t, int32(2), workerHits.Load())
}

func TestSPFreshVectorSearchExec_HTTPErrorMapping(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	cases := []struct {
		name       string
		statusCode int
		wantSubstr string
	}{
		{"bad_request", http.StatusBadRequest, "bad request"},
		{"not_found", http.StatusNotFound, "not found"},
		{"conflict", http.StatusConflict, "not ready"},
		{"unavailable", http.StatusServiceUnavailable, "unavailable"},
		{"internal", http.StatusInternalServerError, "failed"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor.ResetSPFreshVectorSearchForTest()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte("mock error"))
			}))
			t.Cleanup(srv.Close)

			restore := config.RestoreFunc()
			t.Cleanup(restore)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.TiKVAPIServiceAddr = srv.URL
			})

			store, _ := testkit.CreateMockStoreAndDomain(t)
			t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t_spfresh_err")
			tk.MustExec("create table t_spfresh_err(id int primary key, vec vector(3))")
			tk.MustExec("create vector index idx_vec on t_spfresh_err ((vec_l2_distance(vec))) using spfresh")
			tk.MustExec("insert into t_spfresh_err values (1,'[0,0,0]')")

			err := tk.ExecToErr("select /*+ USE_INDEX(t_spfresh_err, idx_vec) */ id from t_spfresh_err order by vec_l2_distance(vec, '[0,0,0]') limit 1")
			require.Error(t, err)
			require.Contains(t, err.Error(), "spfresh /vector_search")
			require.Contains(t, err.Error(), tc.wantSubstr)
		})
	}
}

func TestSPFreshVectorSearchExec_VirtualGeneratedColumnFill(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[spfreshpb.VectorSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/vector_search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &spfreshpb.VectorSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))
		gotReq.Store(reqPB)

		// Always return the only row.
		var colVals [][]byte
		for _, col := range reqPB.RequiredColumns {
			switch col.Tp {
			case int32(mysql.TypeTiDBVectorFloat32):
				vec, err := types.ParseVectorFloat32(`[1,2,3]`)
				require.NoError(t, err)
				vecBytes, err := codec.EncodeValue(nil, nil, types.NewVectorFloat32Datum(vec))
				require.NoError(t, err)
				colVals = append(colVals, vecBytes)
			default:
				t.Fatalf("unexpected required column tp=%v", col.Tp)
			}
		}
		respPB := &spfreshpb.VectorSearchResponse{
			Rows: []*spfreshpb.VectorSearchRow{
				{
					HandleBytes:  kv.IntHandle(1).Encoded(),
					Distance:     0,
					ColumnValues: colVals,
				},
			},
		}
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = srv.URL
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_vgen")
	tk.MustExec("create table t_spfresh_vgen(id int primary key, vec vector(3), g int generated always as (id+1) virtual)")
	tk.MustExec("create vector index idx_vec on t_spfresh_vgen ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_vgen values (1,'[1,2,3]', default)")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_vgen, idx_vec) */ id, g from t_spfresh_vgen order by vec_l2_distance(vec, '[1,2,3]') limit 1").
		Check(testkit.Rows("1 2"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
}

func TestSPFreshVectorSearchExec_CommonHandlePrefixColumn(t *testing.T) {
	executor.ResetSPFreshVectorSearchForTest()

	var gotReq atomic.Pointer[spfreshpb.VectorSearchRequest]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/vector_search" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())

		reqPB := &spfreshpb.VectorSearchRequest{}
		require.NoError(t, proto.Unmarshal(body, reqPB))
		gotReq.Store(reqPB)

		// Handle bytes only contains PK prefix for `a(2)` and full `b`.
		handle := testutil.MustNewCommonHandle(t, "ab", 1)

		var colVals [][]byte
		for _, col := range reqPB.RequiredColumns {
			switch col.Tp {
			case int32(mysql.TypeVarchar):
				valBytes, err := codec.EncodeValue(nil, nil, types.NewStringDatum("abcdef"))
				require.NoError(t, err)
				colVals = append(colVals, valBytes)
			default:
				t.Fatalf("unexpected required column tp=%v", col.Tp)
			}
		}

		respPB := &spfreshpb.VectorSearchResponse{
			Rows: []*spfreshpb.VectorSearchRow{
				{
					HandleBytes:  handle.Encoded(),
					Distance:     0,
					ColumnValues: colVals,
				},
			},
		}
		respBytes, err := proto.Marshal(respPB)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	t.Cleanup(srv.Close)

	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddr = srv.URL
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	t.Cleanup(ingesttestutil.InjectMockBackendMgr(t, store))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_ch")
	tk.MustExec("create table t_spfresh_ch(a varchar(10), b int, vec vector(3), primary key (a(2), b))")
	tk.MustExec("create vector index idx_vec on t_spfresh_ch ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_ch values ('abcdef', 1, '[1,2,3]')")

	tk.MustQuery("select /*+ USE_INDEX(t_spfresh_ch, idx_vec) */ a, b from t_spfresh_ch order by vec_l2_distance(vec, '[1,2,3]') limit 1").
		Check(testkit.Rows("abcdef 1"))

	reqPB := gotReq.Load()
	require.NotNil(t, reqPB)
	require.Len(t, reqPB.RequiredColumns, 1)
	require.Equal(t, int32(mysql.TypeVarchar), reqPB.RequiredColumns[0].Tp)
}

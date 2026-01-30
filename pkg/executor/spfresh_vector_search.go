package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/spfresh/spfreshpb"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// SPFreshVectorSearchExec is a root executor for PhysicalSPFreshVectorScan.
type SPFreshVectorSearchExec struct {
	exec.BaseExecutor

	plan *plannercore.PhysicalSPFreshVectorScan

	tableID int64
	indexID int64

	requiredColumns []*spfreshpb.ColumnInfo
	// requiredColInfos aligns with requiredColumns.
	requiredColInfos []*model.ColumnInfo

	requiredColDefaultValids []bool
	requiredColDefaultValues []types.Datum

	colPlans []spfreshOutputColPlan

	physTblID int64

	// commonHandleBuf is reused per row to avoid per-row allocations.
	commonHandleBuf [][]byte

	// virtual generated columns
	virtualRetTypes  []*types.FieldType
	virtualColumnIdx []int

	rows   []*spfreshpb.VectorSearchRow
	cursor int
}

// Open implements the Executor Open interface.
func (e *SPFreshVectorSearchExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	if e.plan == nil || e.plan.Table == nil || e.plan.Index == nil {
		return errors.New("invalid SPFresh vector search plan")
	}
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	stmtCtx.VectorSearchIsANNQuery = true
	stmtCtx.VectorSearchTopK = e.plan.TopK
	if !config.EnableRemoteBackend() {
		return errors.New("spfresh vector search requires TiKVAPIServiceAddr (tikv-api-service-addr)")
	}
	if e.plan.Index.VectorInfo == nil || e.plan.Index.VectorInfo.Kind != model.VectorIndexKindSPFresh {
		return errors.New("spfresh vector search requires a SPFresh vector index")
	}

	_, e.physTblID = e.plan.IsPartition()
	if e.physTblID == 0 {
		// Fallback for safety; should have been set in planner.
		e.physTblID = e.plan.Table.ID
	}
	e.tableID = e.physTblID
	e.indexID = e.plan.Index.ID

	if err := e.initOutputColumnPlans(); err != nil {
		return err
	}

	clusterID, err := getSPFreshClusterID(ctx, e.Ctx())
	if err != nil {
		return err
	}
	keyspaceID := uint32(e.Ctx().GetStore().GetCodec().GetKeyspaceID())
	startTS, err := sessiontxn.GetTxnManager(e.Ctx()).GetStmtReadTS()
	if err != nil {
		return errors.Trace(err)
	}
	if startTS == 0 {
		return errors.New("failed to get statement read ts")
	}

	queryVecBytes, err := queryVectorBytes(e.plan.QueryVec)
	if err != nil {
		return err
	}

	reqPB := &spfreshpb.VectorSearchRequest{
		KeyspaceId:       keyspaceID,
		TableId:          e.tableID,
		IndexId:          e.indexID,
		StartTs:          startTS,
		TopK:             e.plan.TopK,
		QueryVectorF32Le: queryVecBytes,
		RequiredColumns:  e.requiredColumns,
		MaxBatches:       1,
		OversampleFactor: 1,
		Debug:            false,
	}

	resp, err := e.vectorSearch(ctx, clusterID, keyspaceID, reqPB)
	if err != nil {
		return err
	}

	maxRows := int(reqPB.TopK) * int(reqPB.OversampleFactor)
	if maxRows > 0 && len(resp.Rows) > maxRows {
		return errors.Errorf("spfresh /vector_search returns too many rows: %d > %d", len(resp.Rows), maxRows)
	}
	for _, row := range resp.Rows {
		if len(row.ColumnValues) != len(reqPB.RequiredColumns) {
			return errors.Errorf("spfresh /vector_search returns mismatched column values: %d != %d",
				len(row.ColumnValues), len(reqPB.RequiredColumns))
		}
	}

	e.rows = resp.Rows
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *SPFreshVectorSearchExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.rows) {
		return nil
	}

	outCols := e.Schema().Columns
	decoder := codec.NewDecoder(req, e.Ctx().GetSessionVars().Location())
	needIntHandle := needIntHandle(e.colPlans)
	needCommonHandle := len(e.commonHandleBuf) > 0

	for e.cursor < len(e.rows) && req.NumRows() < req.Capacity() {
		row := e.rows[e.cursor]
		e.cursor++

		var (
			intHandle      int64
			intHandleOK    bool
			commonHandleOK bool
		)
		if needIntHandle {
			var remain []byte
			var err error
			remain, intHandle, err = codec.DecodeInt(row.HandleBytes)
			if err != nil {
				return errors.Trace(err)
			}
			if len(remain) != 0 {
				return errors.New("invalid spfresh handle_bytes for int handle")
			}
			intHandleOK = true
		}
		if needCommonHandle {
			if err := decodeCommonHandle(e.commonHandleBuf, row.HandleBytes); err != nil {
				return err
			}
			commonHandleOK = true
		}

		for colIdx, cp := range e.colPlans {
			switch cp.kind {
			case spfreshFillFromWorker:
				val := row.ColumnValues[cp.requiredIdx]
				if len(val) == 0 {
					if err := e.appendOriginDefault(colIdx, cp.requiredIdx, req); err != nil {
						return err
					}
					continue
				}
				remain, err := decoder.DecodeOne(val, colIdx, outCols[colIdx].RetType)
				if err != nil {
					return errors.Trace(err)
				}
				if len(remain) != 0 {
					return errors.New("invalid spfresh column value: trailing bytes")
				}
			case spfreshFillVirtualGenerated:
				req.AppendNull(colIdx)
			case spfreshFillDistance:
				req.AppendFloat64(colIdx, float64(row.Distance))
			case spfreshFillPhysTblID:
				req.AppendInt64(colIdx, e.physTblID)
			case spfreshFillIntHandle:
				if !intHandleOK {
					return errors.New("spfresh: missing int handle for handle-derived column")
				}
				req.AppendInt64(colIdx, intHandle)
			case spfreshFillCommonHandle:
				if !commonHandleOK {
					return errors.New("spfresh: missing common handle for handle-derived column")
				}
				val := e.commonHandleBuf[cp.commonHandlePos]
				remain, err := decoder.DecodeOne(val, colIdx, outCols[colIdx].RetType)
				if err != nil {
					return errors.Trace(err)
				}
				if len(remain) != 0 {
					return errors.New("invalid spfresh handle column value: trailing bytes")
				}
			default:
				return errors.New("unknown spfresh output column plan")
			}
		}
	}

	return table.FillVirtualColumnValue(
		e.virtualRetTypes,
		e.virtualColumnIdx,
		outCols,
		e.plan.Columns,
		e.Ctx().GetExprCtx(),
		req,
	)
}

type spfreshFillKind uint8

const (
	spfreshFillFromWorker spfreshFillKind = iota
	spfreshFillVirtualGenerated
	spfreshFillDistance
	spfreshFillPhysTblID
	spfreshFillIntHandle
	spfreshFillCommonHandle
)

type spfreshOutputColPlan struct {
	kind spfreshFillKind

	// requiredIdx is the index in request.required_columns/response.column_values when kind==spfreshFillFromWorker.
	requiredIdx int

	// commonHandlePos is the PK position in handle_bytes when kind==spfreshFillCommonHandle.
	commonHandlePos int
}

func (e *SPFreshVectorSearchExec) initOutputColumnPlans() error {
	e.requiredColumns = nil
	e.requiredColInfos = nil
	e.requiredColDefaultValids = nil
	e.requiredColDefaultValues = nil
	e.colPlans = nil
	e.commonHandleBuf = nil
	e.virtualRetTypes = nil
	e.virtualColumnIdx = nil

	schemaCols := e.Schema().Columns
	if len(schemaCols) != len(e.plan.Columns) {
		return errors.New("spfresh plan schema and columns length mismatch")
	}

	tblInfo := e.plan.Table
	isCommonHandle := tblInfo.IsCommonHandle
	pkIsHandle := tblInfo.PKIsHandle

	var pkColIDs []int64
	var prefixColIDs []int64
	pkPosByColID := make(map[int64]int)
	if isCommonHandle {
		pkColIDs = tables.TryGetCommonPkColumnIds(tblInfo)
		prefixColIDs = tables.PrimaryPrefixColumnIDs(tblInfo)
		for i, id := range pkColIDs {
			pkPosByColID[id] = i
		}
	}

	colPlans := make([]spfreshOutputColPlan, len(schemaCols))
	requiredColumns := make([]*spfreshpb.ColumnInfo, 0, len(schemaCols))
	requiredColInfos := make([]*model.ColumnInfo, 0, len(schemaCols))

	for colIdx, colInfo := range e.plan.Columns {
		schemaCol := schemaCols[colIdx]
		switch {
		case schemaCol.VirtualExpr != nil || colInfo.IsVirtualGenerated():
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillVirtualGenerated}
			e.virtualRetTypes = append(e.virtualRetTypes, schemaCol.RetType)
			e.virtualColumnIdx = append(e.virtualColumnIdx, colIdx)
			continue
		case colInfo.ID == model.VirtualColVecSearchDistanceID:
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillDistance}
			continue
		case colInfo.ID == model.ExtraPhysTblID:
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillPhysTblID}
			continue
		case colInfo.ID == model.ExtraHandleID:
			if isCommonHandle {
				return errors.New("spfresh does not support ExtraHandleID on common-handle tables")
			}
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillIntHandle}
			continue
		case pkIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()):
			colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillIntHandle}
			continue
		case isCommonHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()) && !types.NeedRestoredData(schemaCol.RetType):
			pkPos, ok := pkPosByColID[colInfo.ID]
			if ok && notPKPrefixCol(colInfo.ID, prefixColIDs) {
				colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillCommonHandle, commonHandlePos: pkPos}
				continue
			}
		}

		// Otherwise, fetch from worker.
		pbCol, err := columnInfoToSPFreshPB(colInfo)
		if err != nil {
			return err
		}
		requiredIdx := len(requiredColumns)
		requiredColumns = append(requiredColumns, pbCol)
		requiredColInfos = append(requiredColInfos, colInfo)
		colPlans[colIdx] = spfreshOutputColPlan{kind: spfreshFillFromWorker, requiredIdx: requiredIdx}
	}

	e.requiredColumns = requiredColumns
	e.requiredColInfos = requiredColInfos
	e.requiredColDefaultValids = make([]bool, len(requiredColumns))
	e.requiredColDefaultValues = make([]types.Datum, len(requiredColumns))
	e.colPlans = colPlans

	// Allocate a reusable buffer for common handle decoding if needed.
	for _, cp := range e.colPlans {
		if cp.kind == spfreshFillCommonHandle {
			e.commonHandleBuf = make([][]byte, len(pkColIDs))
			break
		}
	}
	return nil
}

func (e *SPFreshVectorSearchExec) appendOriginDefault(colIdx int, requiredIdx int, chk *chunk.Chunk) error {
	if requiredIdx < 0 || requiredIdx >= len(e.requiredColInfos) {
		return errors.New("spfresh: invalid required column index")
	}
	if !e.requiredColDefaultValids[requiredIdx] {
		d, err := table.GetColOriginDefaultValue(e.Ctx().GetExprCtx(), e.requiredColInfos[requiredIdx])
		if err != nil {
			return err
		}
		e.requiredColDefaultValues[requiredIdx] = d
		e.requiredColDefaultValids[requiredIdx] = true
	}
	chk.AppendDatum(colIdx, &e.requiredColDefaultValues[requiredIdx])
	return nil
}

func needIntHandle(colPlans []spfreshOutputColPlan) bool {
	for _, cp := range colPlans {
		if cp.kind == spfreshFillIntHandle {
			return true
		}
	}
	return false
}

func decodeCommonHandle(dst [][]byte, handleBytes []byte) error {
	remain := handleBytes
	for i := range dst {
		if len(remain) == 0 {
			return errors.New("invalid spfresh handle_bytes for common handle: insufficient bytes")
		}
		if remain[0] == 0 {
			return errors.New("invalid spfresh handle_bytes for common handle: padded data before finishing pk")
		}
		var err error
		dst[i], remain, err = codec.CutOne(remain)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func columnInfoToSPFreshPB(c *model.ColumnInfo) (*spfreshpb.ColumnInfo, error) {
	if c == nil {
		return nil, errors.New("nil column info")
	}
	pc := util.ColumnToProto(c, false, false)
	return &spfreshpb.ColumnInfo{
		ColumnId:   pc.ColumnId,
		Tp:         pc.Tp,
		Collation:  pc.Collation,
		ColumnLen:  pc.ColumnLen,
		Decimal:    pc.Decimal,
		Flag:       pc.Flag,
		Elems:      pc.Elems,
		DefaultVal: pc.DefaultVal,
		PkHandle:   pc.PkHandle,
		Array:      pc.Array,
	}, nil
}

func queryVectorBytes(v types.VectorFloat32) ([]byte, error) {
	serialized := v.ZeroCopySerialize()
	if len(serialized) < 4 {
		return nil, errors.New("invalid query vector bytes")
	}
	return serialized[4:], nil
}

const (
	spfreshVectorSearchPath       = "/vector_search"
	maxVectorSearchResponseBytes  = 64 << 20 // 64MiB
	maxVectorSearchErrorBodyBytes = 4 << 10  // 4KiB
)

var (
	spfreshClusterIDCache       atomic.Uint64
	spfreshWorkerAddrByKeyspace sync.Map // map[uint32]string
)

func getSPFreshClusterID(ctx context.Context, sctx sessionctx.Context) (uint64, error) {
	if id := spfreshClusterIDCache.Load(); id != 0 {
		return id, nil
	}
	dom := domain.GetDomain(sctx)
	if dom == nil || dom.GetPDClient() == nil {
		return 0, errors.New("failed to get PD client for cluster_id")
	}
	id := dom.GetPDClient().GetClusterID(ctx)
	// Cluster ID should be stable; cache a non-zero value.
	if id != 0 {
		spfreshClusterIDCache.CompareAndSwap(0, id)
	}
	return id, nil
}

func (e *SPFreshVectorSearchExec) vectorSearch(ctx context.Context, clusterID uint64, keyspaceID uint32, reqPB *spfreshpb.VectorSearchRequest) (*spfreshpb.VectorSearchResponse, error) {
	body, err := proto.Marshal(reqPB)
	if err != nil {
		return nil, errors.Trace(err)
	}

	addr := config.GetGlobalConfig().TiKVAPIServiceAddr
	if cached, ok := spfreshWorkerAddrByKeyspace.Load(keyspaceID); ok {
		if s, ok := cached.(string); ok && s != "" {
			addr = s
		}
	}

	client := *util.InternalHTTPClient()
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	for {
		url := fmt.Sprintf("%s%s?cluster_id=%d", strings.TrimSuffix(addr, "/"), spfreshVectorSearchPath, clusterID)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, errors.Trace(err)
		}
		httpReq.Header.Set("Content-Type", "application/x-protobuf")

		resp, err := client.Do(httpReq)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if resp.StatusCode == http.StatusFound {
			location := resp.Header.Get("Location")
			resp.Body.Close()
			if location == "" {
				return nil, errors.New("spfresh /vector_search redirect without Location header")
			}
			newAddr := strings.TrimSuffix(location, spfreshVectorSearchPath)
			if newAddr == location {
				return nil, errors.Errorf("spfresh /vector_search redirect Location does not end with %q: %q", spfreshVectorSearchPath, location)
			}
			spfreshWorkerAddrByKeyspace.Store(keyspaceID, newAddr)
			addr = newAddr
			continue
		}

		if resp.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxVectorSearchErrorBodyBytes))
			resp.Body.Close()
			return nil, spfreshVectorSearchHTTPError(resp.StatusCode, msg)
		}

		respBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxVectorSearchResponseBytes))
		resp.Body.Close()
		if err != nil {
			return nil, errors.Trace(err)
		}
		respPB := &spfreshpb.VectorSearchResponse{}
		if err := proto.Unmarshal(respBytes, respPB); err != nil {
			return nil, errors.Trace(err)
		}
		return respPB, nil
	}
}

func spfreshVectorSearchHTTPError(statusCode int, msg []byte) error {
	msgStr := strings.TrimSpace(string(msg))
	switch statusCode {
	case http.StatusBadRequest:
		return errors.Errorf("spfresh /vector_search bad request: %s", msgStr)
	case http.StatusNotFound:
		return errors.Errorf("spfresh /vector_search index not found: %s", msgStr)
	case http.StatusConflict:
		return errors.Errorf("spfresh /vector_search index not ready or mismatch: %s", msgStr)
	case http.StatusServiceUnavailable:
		return errors.Errorf("spfresh /vector_search unavailable: %s", msgStr)
	default:
		return errors.Errorf("spfresh /vector_search failed: http %d: %s", statusCode, msgStr)
	}
}

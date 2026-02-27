package executor

import (
	"bytes"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/util/execdetails"
)

var zeroVectorSearchRuntimeStats = vectorSearchRuntimeStats{}

// vectorSearchRuntimeStats is runtime stats returned by the remote vector search engine.
//
// NOTE: This is used for EXPLAIN ANALYZE, and is independent from the rows returned.
type vectorSearchRuntimeStats struct {
	PartitionsScanned   uint64
	VectorsScanned      uint64
	TableLookupKeys     uint64
	TableLookupBytes    uint64
	PermitMicros        uint64
	ConfigMicros        uint64
	IndexOpenMicros     uint64
	SearchMicros        uint64
	TableLookupMicros   uint64
	TiKVClientRPCCount  uint64
	TiKVClientRPCMicros uint64
}

// Tp implements the execdetails.RuntimeStats interface.
func (*vectorSearchRuntimeStats) Tp() int {
	return execdetails.TpVectorSearchRuntimeStats
}

// Clone implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) Clone() execdetails.RuntimeStats {
	if e == nil {
		return &vectorSearchRuntimeStats{}
	}
	clone := *e
	return &clone
}

// Merge implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	other, ok := rs.(*vectorSearchRuntimeStats)
	if !ok || other == nil || e == nil {
		return
	}
	e.PartitionsScanned += other.PartitionsScanned
	e.VectorsScanned += other.VectorsScanned
	e.TableLookupKeys += other.TableLookupKeys
	e.TableLookupBytes += other.TableLookupBytes
	e.PermitMicros += other.PermitMicros
	e.ConfigMicros += other.ConfigMicros
	e.IndexOpenMicros += other.IndexOpenMicros
	e.SearchMicros += other.SearchMicros
	e.TableLookupMicros += other.TableLookupMicros
	e.TiKVClientRPCCount += other.TiKVClientRPCCount
	e.TiKVClientRPCMicros += other.TiKVClientRPCMicros
}

// String implements the execdetails.RuntimeStats interface.
func (e *vectorSearchRuntimeStats) String() string {
	if e == nil || *e == zeroVectorSearchRuntimeStats {
		return ""
	}

	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("vector_search: {")
	if e.PartitionsScanned > 0 {
		buf.WriteString("partitions_scanned: ")
		buf.WriteString(strconv.FormatUint(e.PartitionsScanned, 10))
		buf.WriteString(", ")
	}
	if e.VectorsScanned > 0 {
		buf.WriteString("vectors_scanned: ")
		buf.WriteString(strconv.FormatUint(e.VectorsScanned, 10))
		buf.WriteString(", ")
	}
	if e.TableLookupKeys > 0 {
		buf.WriteString("table_lookup_keys: ")
		buf.WriteString(strconv.FormatUint(e.TableLookupKeys, 10))
		buf.WriteString(", ")
	}
	if e.TableLookupBytes > 0 {
		buf.WriteString("table_lookup_bytes: ")
		buf.WriteString(strconv.FormatUint(e.TableLookupBytes, 10))
		buf.WriteString(", ")
	}
	if e.PermitMicros > 0 {
		buf.WriteString("permit: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.PermitMicros))
		buf.WriteString(", ")
	}
	if e.ConfigMicros > 0 {
		buf.WriteString("config: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.ConfigMicros))
		buf.WriteString(", ")
	}
	if e.IndexOpenMicros > 0 {
		buf.WriteString("index_open: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.IndexOpenMicros))
		buf.WriteString(", ")
	}
	if e.SearchMicros > 0 {
		buf.WriteString("search: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.SearchMicros))
		buf.WriteString(", ")
	}
	if e.TableLookupMicros > 0 {
		buf.WriteString("table_lookup: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.TableLookupMicros))
		buf.WriteString(", ")
	}
	if e.TiKVClientRPCCount > 0 {
		buf.WriteString("tikv_client_rpc_count: ")
		buf.WriteString(strconv.FormatUint(e.TiKVClientRPCCount, 10))
		buf.WriteString(", ")
	}
	if e.TiKVClientRPCMicros > 0 {
		buf.WriteString("tikv_client_rpc: ")
		buf.WriteString(formatVectorSearchDurationFromMicros(e.TiKVClientRPCMicros))
		buf.WriteString(", ")
	}
	if buf.Bytes()[buf.Len()-2] == ',' {
		buf.Truncate(buf.Len() - 2)
	}
	buf.WriteString("}")
	return buf.String()
}

func formatVectorSearchDurationFromMicros(micros uint64) string {
	maxMicros := uint64(math.MaxInt64 / int64(time.Microsecond))
	if micros > maxMicros {
		micros = maxMicros
	}
	return execdetails.FormatDuration(time.Duration(micros) * time.Microsecond)
}

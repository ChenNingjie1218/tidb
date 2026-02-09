package executor

import (
	"bytes"
	"strconv"

	"github.com/pingcap/tidb/pkg/util/execdetails"
)

var zeroVectorSearchRuntimeStats = vectorSearchRuntimeStats{}

// vectorSearchRuntimeStats is runtime stats returned by the remote vector search engine.
//
// NOTE: This is used for EXPLAIN ANALYZE, and is independent from the rows returned.
type vectorSearchRuntimeStats struct {
	PartitionsScanned uint64
	VectorsScanned    uint64
	TableLookupKeys   uint64
	TableLookupBytes  uint64
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
	if buf.Bytes()[buf.Len()-2] == ',' {
		buf.Truncate(buf.Len() - 2)
	}
	buf.WriteString("}")
	return buf.String()
}

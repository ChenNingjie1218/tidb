package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

type recordingIngestWriter struct {
	writeCount int
	lastKey    []byte
	lastVal    []byte
	lastHandle kv.Handle
}

var _ ingest.Writer = (*recordingIngestWriter)(nil)

func (w *recordingIngestWriter) WriteRow(_ context.Context, idxKey, idxVal []byte, handle kv.Handle) error {
	w.writeCount++
	w.lastKey = append(w.lastKey[:0], idxKey...)
	w.lastVal = append(w.lastVal[:0], idxVal...)
	w.lastHandle = handle
	return nil
}

func (*recordingIngestWriter) LockForWrite() (unlock func()) {
	return func() {}
}

func TestWriteVectorIndexRecord(t *testing.T) {
	t.Run("encode-handle-and-vector", func(t *testing.T) {
		vec, err := types.CreateVectorFloat32([]float32{1, 2, 3})
		require.NoError(t, err)
		serialized := vec.ZeroCopySerialize()
		require.Len(t, serialized, 4+3*4)

		var d types.Datum
		d.SetVectorFloat32(vec)

		w := &recordingIngestWriter{}
		vecInfo := &model.VectorIndexInfo{Dimension: 3}
		h := kv.IntHandle(42)

		err = writeVectorIndexRecord(context.Background(), w, vecInfo, []types.Datum{d}, h)
		require.NoError(t, err)
		require.Equal(t, 1, w.writeCount)
		require.Equal(t, h.Encoded(), w.lastKey)
		require.Equal(t, serialized[4:], w.lastVal)
		require.Nil(t, w.lastHandle)
	})

	t.Run("null-vector-skipped", func(t *testing.T) {
		var d types.Datum
		d.SetNull()

		w := &recordingIngestWriter{}
		vecInfo := &model.VectorIndexInfo{Dimension: 3}
		h := kv.IntHandle(42)

		err := writeVectorIndexRecord(context.Background(), w, vecInfo, []types.Datum{d}, h)
		require.NoError(t, err)
		require.Equal(t, 0, w.writeCount)
	})

	t.Run("dimension-mismatch", func(t *testing.T) {
		vec, err := types.CreateVectorFloat32([]float32{1, 2, 3})
		require.NoError(t, err)

		var d types.Datum
		d.SetVectorFloat32(vec)

		w := &recordingIngestWriter{}
		vecInfo := &model.VectorIndexInfo{Dimension: 2}
		h := kv.IntHandle(42)

		err = writeVectorIndexRecord(context.Background(), w, vecInfo, []types.Datum{d}, h)
		require.ErrorContains(t, err, "vector dimension mismatch")
		require.Equal(t, 0, w.writeCount)
	})

	t.Run("invalid-index-column-count", func(t *testing.T) {
		vec, err := types.CreateVectorFloat32([]float32{1, 2, 3})
		require.NoError(t, err)
		var d types.Datum
		d.SetVectorFloat32(vec)

		w := &recordingIngestWriter{}
		vecInfo := &model.VectorIndexInfo{Dimension: 3}
		h := kv.IntHandle(42)

		err = writeVectorIndexRecord(context.Background(), w, vecInfo, []types.Datum{d, d}, h)
		require.ErrorContains(t, err, "invalid vector index columns length")
		require.Equal(t, 0, w.writeCount)
	})
}

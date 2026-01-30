package property

import (
	"bytes"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPhysicalPropertyHashCodeVectorProp(t *testing.T) {
	t.Parallel()

	col := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vs := &expression.VectorHelper{
		FnPbCode: tipb.ScalarFuncSig(123),
		Column:   col,
	}

	prop1 := &PhysicalProperty{
		TaskTp:      RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}
	prop1.VectorProp.VectorHelper = vs
	prop1.VectorProp.TopK = 10

	prop2 := &PhysicalProperty{
		TaskTp:      RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}
	prop2.VectorProp.VectorHelper = vs
	prop2.VectorProp.TopK = 11

	require.False(t, bytes.Equal(prop1.HashCode(), prop2.HashCode()), "hash should include vector TopK")
}

func TestPhysicalPropertyCloneEssentialFieldsVectorProp(t *testing.T) {
	t.Parallel()

	col := &expression.Column{
		UniqueID: 1,
		ID:       1,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	vs := &expression.VectorHelper{
		FnPbCode: tipb.ScalarFuncSig(123),
		Column:   col,
	}

	prop := &PhysicalProperty{
		TaskTp:      RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}
	prop.VectorProp.VectorHelper = vs
	prop.VectorProp.TopK = 10

	cloned := prop.CloneEssentialFields()
	require.NotNil(t, cloned.VectorProp.VectorHelper)
	require.Equal(t, uint32(10), cloned.VectorProp.TopK)
	require.Equal(t, int64(1), cloned.VectorProp.Column.ID)
	require.Equal(t, tipb.ScalarFuncSig(123), cloned.VectorProp.FnPbCode)
}

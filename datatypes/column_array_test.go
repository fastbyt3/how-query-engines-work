package datatypes_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/stretchr/testify/require"
)

func TestIntArray(t *testing.T) {
	int8Builder := array.NewInt8Builder(memory.NewGoAllocator())
	defer int8Builder.Release()

	int8Builder.AppendValues(
		[]int8{1, 2, 3},
		[]bool{true, true, false},
	)

	arr := int8Builder.NewInt8Array()
	require.Equal(t, []int8{1, 2, 3}, arr.Int8Values())
	require.True(t, arr.IsValid(0))
	require.True(t, arr.IsNull(2))
	require.True(t, arr.IsNull(3))
}

func TestArrowArrayBuilder(t *testing.T) {
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.Int8Type)
	for i := range 10 {
		builder.Append(int8(i))
	}

	colArr := builder.Build()
	for i := range 10 {
		require.Equal(t, int8(i), colArr.GetValue(i))
	}
}

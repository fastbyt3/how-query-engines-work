package datatypes

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	BooleanType = &arrow.BooleanType{}
	Int8Type    = &arrow.Int8Type{}
	Int16Type   = &arrow.Int16Type{}
	Int32Type   = &arrow.Int32Type{}
	Int64Type   = &arrow.Int64Type{}
	UInt8Type   = &arrow.Uint8Type{}
	UInt16Type  = &arrow.Uint16Type{}
	UInt32Type  = &arrow.Uint32Type{}
	UInt64Type  = &arrow.Uint64Type{}
	FloatType   = &arrow.Float32Type{}
	DoubleType  = &arrow.Float64Type{}
	StringType  = &arrow.StringType{}
)

type ArrowArrayBuilder struct {
	builder array.Builder
}

func NewArrowArrayBuilder(mem memory.Allocator, dtype arrow.DataType) ArrowArrayBuilder {
	return ArrowArrayBuilder{
		builder: array.NewBuilder(mem, dtype),
	}
}

func (a *ArrowArrayBuilder) Append(val any) {
	if val == nil {
		a.builder.AppendNull()
		return
	}

	switch b := a.builder.(type) {
	case *array.BooleanBuilder:
		b.Append(val.(bool))
	case *array.Int8Builder:
		b.Append(val.(int8))
	case *array.Int16Builder:
		b.Append(val.(int16))
	case *array.Int32Builder:
		b.Append(val.(int32))
	case *array.Int64Builder:
		b.Append(val.(int64))
	case *array.Uint8Builder:
		b.Append(val.(uint8))
	case *array.Uint16Builder:
		b.Append(val.(uint16))
	case *array.Uint32Builder:
		b.Append(val.(uint32))
	case *array.Uint64Builder:
		b.Append(val.(uint64))
	case *array.Float32Builder:
		b.Append(val.(float32))
	case *array.Float64Builder:
		b.Append(val.(float64))
	case *array.StringBuilder:
		b.Append((val.(string)))
	default:
		panic(fmt.Errorf("arrow/array: unsupported builder for %T", b))
	}
}

func (a *ArrowArrayBuilder) AppendValues(values ...any) {
	for _, v := range values {
		a.Append(v)
	}
}

func (a *ArrowArrayBuilder) Build() ColumnArray {
	return &ArrowFieldArray{
		fieldArray: a.builder.NewArray(),
	}
}

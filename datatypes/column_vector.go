package datatypes

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type ColumnArray interface {
	GetType() arrow.DataType
	GetValue(i int) any
	Size() int
}

// LiteralValueArray represents a column where each entry is the same literal value.
type LiteralValueArray struct {
	arrowType arrow.DataType
	value     any
	arraySize int
}

func (l LiteralValueArray) GetType() arrow.DataType {
	return l.arrowType
}

func (l LiteralValueArray) GetValue(i int) any {
	if i < 0 || i >= l.arraySize {
		panic("index out of bounds")
	}
	return l.value
}

func (l LiteralValueArray) Size() int {
	return l.arraySize
}

func NewLiteralValueArray(arrowType arrow.DataType, value any, arraySize int) LiteralValueArray {
	return LiteralValueArray{arrowType, value, arraySize}
}

var _ ColumnArray = (*LiteralValueArray)(nil)

// ArrowFieldArray wraps an Arrow array.Array to implement the ColumnArray interface.
type ArrowFieldArray struct {
	fieldArray arrow.Array
}

func NewArrowFieldArray(fieldArray arrow.Array) *ArrowFieldArray {
	return &ArrowFieldArray{fieldArray}
}

func (a *ArrowFieldArray) GetType() arrow.DataType {
	return a.fieldArray.DataType()
}

func (a *ArrowFieldArray) GetValue(i int) any {
	if a.fieldArray.IsNull(i) {
		return nil
	}
	switch v := a.fieldArray.(type) {
	case *array.Boolean:
		return v.Value(i)
	case *array.Int8:
		return v.Value(i)
	case *array.Int16:
		return v.Value(i)
	case *array.Int32:
		return v.Value(i)
	case *array.Int64:
		return v.Value(i)
	case *array.Uint8:
		return v.Value(i)
	case *array.Uint16:
		return v.Value(i)
	case *array.Uint32:
		return v.Value(i)
	case *array.Uint64:
		return v.Value(i)
	case *array.Float32:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.String:
		return v.Value(i)
	default:
		panic("unsupported fieldArray type")
	}
}

func (a *ArrowFieldArray) Size() int {
	return a.fieldArray.Len()
}

var _ ColumnArray = (*ArrowFieldArray)(nil)

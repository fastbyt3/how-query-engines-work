package datatypes

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// Instead of working with `FieldVector` directly we use `ColumnVector`
type ColumnVector interface {
	GetData() arrow.DataType
	GetValue(i int) interface{}
	Size() int
}

type LiteralValueVector struct {
	arrowType arrow.DataType
	value     interface{}
	size      int
}

func (v *LiteralValueVector) GetData() (_ arrow.DataType) {
	return v.arrowType
}
func (v *LiteralValueVector) GetValue(i int) (_ interface{}) {
	if i < 0 || i >= v.size {
		panic("index out of bounds")
	}
	return v.value
}
func (v *LiteralValueVector) Size() (_ int) {
	return v.size
}

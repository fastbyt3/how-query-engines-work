package exprs

import (
	"fmt"

	"github.com/fastbyt3/query-engine/datatypes"
)

type ColumnExpr struct {
	index int
}

func (e ColumnExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return input.Field(e.index)
}

func (e ColumnExpr) String() string {
	return fmt.Sprintf("#%d", e.index)
}

func NewColumnIndexExpr(index int) ColumnExpr {
	return ColumnExpr{index}
}

package exprs

import (
	"fmt"

	"github.com/fastbyt3/query-engine/datatypes"
)

type LiteralLongExpr struct {
	val int64
}

func (e LiteralLongExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(
		datatypes.Int64Type,
		e.val,
		input.RowCount(),
	)
}

func (e LiteralLongExpr) String() string {
	return fmt.Sprint(e.val)
}

func NewLiteralLongExpr(val int64) LiteralLongExpr {
	return LiteralLongExpr{val}
}

type LiteralDoubleExpr struct {
	val float64
}

func (e LiteralDoubleExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(
		datatypes.FloatType,
		e.val,
		input.RowCount(),
	)
}

func (e LiteralDoubleExpr) String() string {
	return fmt.Sprint(e.val)
}

func NewLiteralDoubleExpr(v float64) LiteralDoubleExpr {
	return LiteralDoubleExpr{v}
}

type LiteralStringExpr struct {
	val string
}

func (e LiteralStringExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	return datatypes.NewLiteralValueArray(
		datatypes.StringType,
		e.val,
		input.RowCount(),
	)
}

func (e LiteralStringExpr) String() string {
	return fmt.Sprint(e.val)
}

func NewLiteralStringExpr(v string) LiteralStringExpr {
	return LiteralStringExpr{v}
}

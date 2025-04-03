package exprs

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fastbyt3/query-engine/physicalplan"
)

type BinaryExprEvalFunc = func(l, r any, arrowType arrow.DataType) any

type BinaryExpr struct {
	name     string
	l, r     physicalplan.PhysicalExpression
	op       string
	evalFunc BinaryExprEvalFunc
}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.l, b.op, b.r)
}

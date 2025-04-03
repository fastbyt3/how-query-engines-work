package exprs

import (
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
)

type MathExpr struct {
	BinaryExpr
}

func (m MathExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	ll := m.l.Evaluate(input)
	rr := m.r.Evaluate(input)
	if ll.Size() != rr.Size() || ll.GetType() != rr.GetType() {
		slog.Error(
			"LHS and RHS aren't compatible",
			slog.Int("ll size", ll.Size()),
			slog.Int("rr size", rr.Size()),
			slog.String("ll datatype", ll.GetType().Name()),
			slog.String("rr datatype", rr.GetType().Name()),
		)
		panic("LHS and RHS aren't compatible")
	}

	return m.binaryEvaluate(ll, rr)
}

func (m MathExpr) binaryEvaluate(l, r datatypes.ColumnArray) datatypes.ColumnArray {
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), l.GetType())
	for i := range l.Size() {
		builder.Append(m.evalFunc(l.GetValue(i), r.GetValue(i), l.GetType()))
	}
	return builder.Build()
}

var AddEvalFunc = func(lData, rData any, arrowType arrow.DataType) any {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) + rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) + rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) + rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) + rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) + rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) + rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) + rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) + rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) + rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) + rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewAddExpr(l, r physicalplan.PhysicalExpression) MathExpr {
	return MathExpr{BinaryExpr{"add", l, r, "+", AddEvalFunc}}
}

var SubtractEvalFunc = func(lData, rData any, arrowType arrow.DataType) any {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) - rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) - rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) - rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) - rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) - rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) - rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) - rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) - rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) - rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) - rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewSubtractExpr(l, r physicalplan.PhysicalExpression) MathExpr {
	return MathExpr{BinaryExpr{"subtract", l, r, "-", SubtractEvalFunc}}
}

var MultiplyEvalFunc = func(lData, rData any, arrowType arrow.DataType) any {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) * rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) * rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) * rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) * rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) * rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) * rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) * rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) * rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) * rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) * rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewMultiplyExpr(l, r physicalplan.PhysicalExpression) MathExpr {
	return MathExpr{BinaryExpr{"multiply", l, r, "*", MultiplyEvalFunc}}
}

var DivideEvalFunc = func(lData, rData any, arrowType arrow.DataType) any {
	switch arrowType {
	case datatypes.Int8Type:
		return lData.(int8) / rData.(int8)
	case datatypes.Int16Type:
		return lData.(int16) / rData.(int16)
	case datatypes.Int32Type:
		return lData.(int32) / rData.(int32)
	case datatypes.Int64Type:
		return lData.(int64) / rData.(int64)
	case datatypes.UInt8Type:
		return lData.(uint8) / rData.(uint8)
	case datatypes.UInt16Type:
		return lData.(uint16) / rData.(uint16)
	case datatypes.UInt32Type:
		return lData.(uint32) / rData.(uint32)
	case datatypes.UInt64Type:
		return lData.(uint64) / rData.(uint64)
	case datatypes.FloatType:
		return lData.(float32) / rData.(float32)
	case datatypes.DoubleType:
		return lData.(float64) / rData.(float64)
	default:
		panic(fmt.Sprintf("Unsupported data type in math expression: %s", arrowType))
	}
}

func NewDivideExpr(l, r physicalplan.PhysicalExpression) MathExpr {
	return MathExpr{BinaryExpr{"divide", l, r, "/", DivideEvalFunc}}
}

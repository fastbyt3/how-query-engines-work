package exprs

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
)

type BooleanExpr struct {
	BinaryExpr
}

func (b BooleanExpr) Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray {
	ll := b.l.Evaluate(input)
	rr := b.r.Evaluate(input)
	if ll.Size() != rr.Size() || ll.GetType() != rr.GetType() {
		panic("LHS and RHS of boolean expr aren;t compatible")
	}
	return b.binaryEvaluate(ll, rr)
}

func (b BooleanExpr) binaryEvaluate(l, r datatypes.ColumnArray) datatypes.ColumnArray {
	builder := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), datatypes.BooleanType)
	for i := range l.Size() {
		builder.Append(b.evalFunc(l.GetValue(i), r.GetValue(i), l.GetType()))
	}

	return builder.Build()
}

func toBool(data any, dt arrow.DataType) bool {
	switch dt {
	case datatypes.BooleanType:
		return data.(bool)
	case datatypes.Int16Type, datatypes.Int8Type, datatypes.Int32Type, datatypes.Int64Type,
		datatypes.UInt16Type, datatypes.UInt8Type, datatypes.UInt32Type, datatypes.UInt64Type:
		return data == 1
	default:
		panic(fmt.Sprintf("Unexpected data: %v datatype: %v", data, dt))
	}
}

var AndEvalFunc = func(l, r any, dt arrow.DataType) any {
	return toBool(l, dt) && toBool(r, dt)
}

func NewAndExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"and", l, r, "AND", AndEvalFunc}}
}

var OrEvalFunc = func(l, r any, dt arrow.DataType) any {
	return toBool(l, dt) && toBool(r, dt)
}

func NewOrExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"or", l, r, "OR", AndEvalFunc}}
}

var EqEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.BooleanType:
		return l.(bool) == r.(bool)
	case datatypes.Int8Type:
		return l.(int8) == r.(int8)
	case datatypes.Int16Type:
		return l.(int16) == r.(int16)
	case datatypes.Int32Type:
		return l.(int32) == r.(int32)
	case datatypes.Int64Type:
		return l.(int64) == r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) == r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) == r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) == r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) == r.(uint64)
	case datatypes.FloatType:
		return l.(float32) == r.(float32)
	case datatypes.DoubleType:
		return l.(float64) == r.(float64)
	case datatypes.StringType:
		return l.(string) == r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewEqExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"eq", l, r, "==", EqEvalFunc}}
}

var NeqEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.BooleanType:
		return l.(bool) != r.(bool)
	case datatypes.Int8Type:
		return l.(int8) != r.(int8)
	case datatypes.Int16Type:
		return l.(int16) != r.(int16)
	case datatypes.Int32Type:
		return l.(int32) != r.(int32)
	case datatypes.Int64Type:
		return l.(int64) != r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) != r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) != r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) != r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) != r.(uint64)
	case datatypes.FloatType:
		return l.(float32) != r.(float32)
	case datatypes.DoubleType:
		return l.(float64) != r.(float64)
	case datatypes.StringType:
		return l.(string) != r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewNeqExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"neq", l, r, "!=", NeqEvalFunc}}
}

var LtEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.Int8Type:
		return l.(int8) < r.(int8)
	case datatypes.Int16Type:
		return l.(int16) < r.(int16)
	case datatypes.Int32Type:
		return l.(int32) < r.(int32)
	case datatypes.Int64Type:
		return l.(int64) < r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) < r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) < r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) < r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) < r.(uint64)
	case datatypes.FloatType:
		return l.(float32) < r.(float32)
	case datatypes.DoubleType:
		return l.(float64) < r.(float64)
	case datatypes.StringType:
		return l.(string) < r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewLtExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"lt", l, r, "<", LtEvalFunc}}
}

var LtEqEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.Int8Type:
		return l.(int8) <= r.(int8)
	case datatypes.Int16Type:
		return l.(int16) <= r.(int16)
	case datatypes.Int32Type:
		return l.(int32) <= r.(int32)
	case datatypes.Int64Type:
		return l.(int64) <= r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) <= r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) <= r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) <= r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) <= r.(uint64)
	case datatypes.FloatType:
		return l.(float32) <= r.(float32)
	case datatypes.DoubleType:
		return l.(float64) <= r.(float64)
	case datatypes.StringType:
		return l.(string) <= r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewLtEqExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"lteq", l, r, "<=", LtEqEvalFunc}}
}

var GtEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.Int8Type:
		return l.(int8) > r.(int8)
	case datatypes.Int16Type:
		return l.(int16) > r.(int16)
	case datatypes.Int32Type:
		return l.(int32) > r.(int32)
	case datatypes.Int64Type:
		return l.(int64) > r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) > r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) > r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) > r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) > r.(uint64)
	case datatypes.FloatType:
		return l.(float32) > r.(float32)
	case datatypes.DoubleType:
		return l.(float64) > r.(float64)
	case datatypes.StringType:
		return l.(string) > r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewGtExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"gt", l, r, ">", GtEvalFunc}}
}

var GtEqEvalFunc = func(l, r any, dt arrow.DataType) any {
	switch dt {
	case datatypes.Int8Type:
		return l.(int8) >= r.(int8)
	case datatypes.Int16Type:
		return l.(int16) >= r.(int16)
	case datatypes.Int32Type:
		return l.(int32) >= r.(int32)
	case datatypes.Int64Type:
		return l.(int64) >= r.(int64)
	case datatypes.UInt8Type:
		return l.(uint8) >= r.(uint8)
	case datatypes.UInt16Type:
		return l.(uint16) >= r.(uint16)
	case datatypes.UInt32Type:
		return l.(uint32) >= r.(uint32)
	case datatypes.UInt64Type:
		return l.(uint64) >= r.(uint64)
	case datatypes.FloatType:
		return l.(float32) >= r.(float32)
	case datatypes.DoubleType:
		return l.(float64) >= r.(float64)
	case datatypes.StringType:
		return l.(string) >= r.(string)
	default:
		panic(fmt.Sprintf("Unexpected arrow datatype: %v; l = %v; r = %v", dt, l, r))
	}
}

func NewGtEqExpr(l, r physicalplan.PhysicalExpression) BooleanExpr {
	return BooleanExpr{BinaryExpr{"gteq", l, r, ">=", GtEqEvalFunc}}
}

package logicalplans

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fastbyt3/query-engine/datatypes"
)

type LogicalExpr interface {
	ToField(input LogicalPlan) arrow.Field
	String() string
}

// Column -> reference to a named column
//
// Column refers to column produced by input logical plan and could represent a col in datasource
// or result of an expr being evaluated against other inputs
type Column struct {
	Name string
}

func (c *Column) ToField(input LogicalPlan) arrow.Field {
	schema := input.Schema()
	for _, f := range schema.Fields() {
		if f.Name == c.Name {
			return f
		}
	}

	panic(fmt.Sprintf("failed to find a column: %s in logicPlan", c.Name))
}

func (c *Column) String() string {
	return c.Name
}

// Literal Expressions

// LiteralString represents a string val
type LiteralString struct {
	Str string
}

func NewLiteralString(s string) LiteralString {
	return LiteralString{Str: s}
}

func (e *LiteralString) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.Str,
		Type: datatypes.StringType,
	}
}

func (e *LiteralString) String() string {
	return fmt.Sprintf("'%s'", e.Str)
}

type LiteralLong struct {
	N int64
}

func NewLiteralLong(n int64) LiteralLong {
	return LiteralLong{n}
}

func (e *LiteralLong) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: fmt.Sprintf("%d", e.N),
		Type: datatypes.Int64Type,
	}
}

func (e *LiteralLong) String() string {
	return fmt.Sprintf("%d", e.N)
}

type LiteralFloat struct {
	N float32
}

func NewLiteralFloat(n float32) LiteralFloat {
	return LiteralFloat{n}
}

func (e *LiteralFloat) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: fmt.Sprintf("%g", e.N),
		Type: datatypes.FloatType,
	}
}

func (e *LiteralFloat) String() string {
	return fmt.Sprintf("%g", e.N)
}

// Binary Expressions

type BinaryExpr struct {
	name string
	op   string
	l    LogicalExpr
	r    LogicalExpr
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.l, e.op, e.r)
}

type BooleanBinaryExpr struct {
	BinaryExpr
}

var _ LogicalExpr = (*BooleanBinaryExpr)(nil)

func (e *BooleanBinaryExpr) ToField(lp LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: e.name,
		Type: datatypes.BooleanType,
	}
}

func NewEqExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"eq", "=", l, r}}
}

func NewNegExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"neg", "!=", l, r}}
}

func NewGtExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"gt", ">", l, r}}
}

func NewLtExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"lt", "<", l, r}}
}

func NewGtEqExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"gt", ">=", l, r}}
}

func NewLtEqExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"lteq", "<=", l, r}}
}

func NewAndExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"and", "&", l, r}}
}

func NewOrExpr(l, r LogicalExpr) BooleanBinaryExpr {
	return BooleanBinaryExpr{BinaryExpr{"or", "OR", l, r}}
}

// Math expressions

type MathExpr struct {
	BinaryExpr
}

// ToField implements LogicalExpr.
func (m *MathExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: m.name,
		Type: m.l.ToField(input).Type,
	}
}

var _ LogicalExpr = (*MathExpr)(nil)

func NewAdd(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{
		name: "add",
		op:   "+",
		l:    l,
		r:    r,
	}}
}

func NewSub(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"sub", "-", l, r}}
}

func NewMult(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"mult", "*", l, r}}
}

func NewDiv(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"div", "/", l, r}}
}

func NewMod(l, r LogicalExpr) MathExpr {
	return MathExpr{BinaryExpr{"mod", "%", l, r}}
}

// Aggregate expressions

type AggregateExpr struct {
	name string
	expr LogicalExpr
}

func (a *AggregateExpr) ToField(input LogicalPlan) arrow.Field {
	return arrow.Field{
		Name: a.name,
		Type: a.expr.ToField(input).Type,
	}
}

func (a *AggregateExpr) String() string {
	return fmt.Sprintf("%s(%s)", a.name, a.expr)
}

var _ LogicalExpr = (*AggregateExpr)(nil)

func NewSumExpr(input LogicalExpr) AggregateExpr {
	return AggregateExpr{name: "SUM", expr: input}
}

func NewMinExpr(input LogicalExpr) AggregateExpr {
	return AggregateExpr{name: "MIN", expr: input}
}

func NewMaxExpr(input LogicalExpr) AggregateExpr {
	return AggregateExpr{name: "MAX", expr: input}
}

func NewAvgExpr(input LogicalExpr) AggregateExpr {
	return AggregateExpr{name: "AVG", expr: input}
}

type AggregateCountExpr struct {
	AggregateExpr
}

func (a *AggregateCountExpr) ToField(_ LogicalPlan) arrow.Field {
	return arrow.Field{Name: "COUNT", Type: datatypes.Int64Type}
}

func (a *AggregateCountExpr) String() string {
	return fmt.Sprintf("COUNT(%s)", a.expr)
}

var _ LogicalExpr = (*AggregateCountExpr)(nil)

func NewAggregateCountExpr(input LogicalExpr) AggregateExpr {
	return AggregateExpr{"COUNT", input}
}

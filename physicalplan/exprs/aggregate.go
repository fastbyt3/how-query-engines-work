package exprs

import (
	"fmt"

	"github.com/fastbyt3/query-engine/physicalplan"
)

type Accumulator interface {
	Accumulate(value any)
	FinalValue() any
}

// Aggregate expressions perform the specific aggregation operation across multiple batches of data
// to produce a single value, this requires Accumulators specific to the aggregation being performed
type AggregateExpression interface {
	InputExpression() physicalplan.PhysicalExpression
	CreateAccumulator() Accumulator
}

// ---- Aggregate operation: MAX
type MaxExpr struct {
	expr physicalplan.PhysicalExpression
}

func (e *MaxExpr) InputExpression() physicalplan.PhysicalExpression {
	return e.expr
}
func (e *MaxExpr) CreateAccumulator() Accumulator {
	return &MaxAccumulator{}
}

func (e MaxExpr) String() string {
	return fmt.Sprintf("MAX(%s)", e.expr)
}

type MaxAccumulator struct {
	value any
}

func (m *MaxAccumulator) Accumulate(value any) {
	if value == nil {
		return
	}

	if m.value == nil {
		m.value = value
		return
	}

	isMax := false
	switch v := m.value.(type) {
	case int8:
		isMax = value.(int8) > v
	case int16:
		isMax = value.(int16) > v
	case int32:
		isMax = value.(int32) > v
	case int64:
		isMax = value.(int64) > v
	case uint8:
		isMax = value.(uint8) > v
	case uint16:
		isMax = value.(uint16) > v
	case uint32:
		isMax = value.(uint32) > v
	case uint64:
		isMax = value.(uint64) > v
	case float32:
		isMax = value.(float32) > v
	case float64:
		isMax = value.(float64) > v
	case string:
		isMax = value.(string) > v
	default:
		panic(fmt.Sprintf("Value passed to accumulator: %v - does not have MAX operation", value))
	}

	if isMax {
		m.value = value
	}
}

func (m *MaxAccumulator) FinalValue() any {
	return m.value
}

// ---- Aggregate operation: MIN
type MinExpr struct {
	expr physicalplan.PhysicalExpression
}

func (e *MinExpr) InputExpression() physicalplan.PhysicalExpression {
	return e.expr
}
func (e *MinExpr) CreateAccumulator() Accumulator {
	return &MinAccumulator{}
}

func (e MinExpr) String() string {
	return fmt.Sprintf("MIN(%s)", e.expr)
}

type MinAccumulator struct {
	value any
}

func (m *MinAccumulator) Accumulate(value any) {
	if value == nil {
		return
	}

	if m.value == nil {
		m.value = value
		return
	}

	isMin := false
	switch v := m.value.(type) {
	case int8:
		isMin = value.(int8) < v
	case int16:
		isMin = value.(int16) < v
	case int32:
		isMin = value.(int32) < v
	case int64:
		isMin = value.(int64) < v
	case uint8:
		isMin = value.(uint8) < v
	case uint16:
		isMin = value.(uint16) < v
	case uint32:
		isMin = value.(uint32) < v
	case uint64:
		isMin = value.(uint64) < v
	case float32:
		isMin = value.(float32) < v
	case float64:
		isMin = value.(float64) < v
	case string:
		isMin = value.(string) < v
	default:
		panic(fmt.Sprintf("Value passed to accumulator: %v - does not have MIN operation", value))
	}

	if isMin {
		m.value = value
	}
}

func (m *MinAccumulator) FinalValue() any {
	return m.value
}

// ---- Aggregate operation: SUM
type SumExpr struct {
	expr physicalplan.PhysicalExpression
}

func (e *SumExpr) InputExpression() physicalplan.PhysicalExpression {
	return e.expr
}

func (e *SumExpr) CreateAccumulator() Accumulator {
	return &SumAccumulator{}
}

func (e *SumExpr) String() string {
	return fmt.Sprintf("SUM(%s)", e.expr)
}

type SumAccumulator struct {
	value any
}

func (a *SumAccumulator) Accumulate(value any) {
	if value == nil {
		return
	}

	if a.value == nil {
		a.value = value
		return
	}

	switch v := a.value.(type) {
	case int8:
		a.value = value.(int8) + v
	case int16:
		a.value = value.(int16) + v
	case int32:
		a.value = value.(int32) + v
	case int64:
		a.value = value.(int64) + v
	case uint8:
		a.value = value.(uint8) + v
	case uint16:
		a.value = value.(uint16) + v
	case uint32:
		a.value = value.(uint32) + v
	case uint64:
		a.value = value.(uint64) + v
	case float32:
		a.value = value.(float32) + v
	case float64:
		a.value = value.(float64) + v
	case string:
		a.value = value.(string) + v
	default:
		panic(fmt.Sprintf("Value passed to accumulator: %v - does not have SUM operation", value))
	}
}

func (a *SumAccumulator) FinalValue() any {
	return a.value
}

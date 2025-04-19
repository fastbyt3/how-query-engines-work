package plans

import (
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
	"github.com/fastbyt3/query-engine/physicalplan/exprs"
)

// HashAggregate plan must process all incoming batches and maintain a hash map of accumulators
// and update the acc for each row being processed. Finally the results of the accumulators are
// used to create one record batch at end containing results of aggregate query
type HashAggregateExec struct {
	input physicalplan.PhysicalPlan

	// evaluate record batch and produce grouping keys
	groupExprs []physicalplan.PhysicalExpression

	// eval record batch and produce aggregate keys, and accumulators for every aggregateExpr
	aggregateExprs []exprs.AggregateExpression

	// schema represents group and aggregate expressions
	schema datatypes.Schema
}

func (h HashAggregateExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{h.input}
}

func (h HashAggregateExec) Execute() iter.Seq[datatypes.RecordBatch] {
	// hashmap := make(map[[]any][]Accumulator) // this can't be created in go since lists aren't valid keys for a map
	rowToAccMap := make(map[string][]exprs.Accumulator)
	rowHashValMap := make(map[string][]any)

	for rb := range h.input.Execute() {
		groupKeys := make([]datatypes.ColumnArray, len(h.groupExprs))
		for i, groupExpr := range h.groupExprs {
			groupKeys[i] = groupExpr.Evaluate(rb)
		}

		aggrInputValues := make([]datatypes.ColumnArray, len(h.aggregateExprs))
		for i, aggrExpr := range h.aggregateExprs {
			aggrInputValues[i] = aggrExpr.InputExpression().Evaluate(rb)
		}

		for rowIdx := range rb.RowCount() {
			// create hash key for each row
			rowKey := make([]any, len(groupKeys))
			for j, groupKey := range groupKeys {
				rowKey[j] = groupKey.GetValue(rowIdx)
			}

			// since we can't directly have []any as a key in map, we encode it to string of values in slice
			rowKeyEnc := h.encodeCols(rowKey)

			rowAccumulators, exists := rowToAccMap[rowKeyEnc]
			if !exists {
				rowAccumulators = make([]exprs.Accumulator, len(h.aggregateExprs))
				for i, aggrExpr := range h.aggregateExprs {
					rowAccumulators[i] = aggrExpr.CreateAccumulator()
				}
				rowToAccMap[rowKeyEnc] = rowAccumulators
				rowHashValMap[rowKeyEnc] = rowKey
			}

			for i, acc := range rowAccumulators {
				val := aggrInputValues[i].GetValue(rowIdx)
				acc.Accumulate(val)
			}
		}
	}

	// create record batch with final aggregate values
	builders := make([]datatypes.ArrowArrayBuilder, h.schema.NumFields())
	for i, field := range h.schema.Fields() {
		builders[i] = datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.Type)
	}

	for rowKeyEnc, rowKey := range rowHashValMap {
		for i, col := range rowKey {
			builders[i].Append(col)
		}

		rowAccumulators, _ := rowToAccMap[rowKeyEnc]
		for i, acc := range rowAccumulators {
			builders[len(h.groupExprs)+i].Append(acc.FinalValue())
		}
	}

	panic("unimpl")
}

func (h HashAggregateExec) Schema() datatypes.Schema {
	return h.schema
}

func (h HashAggregateExec) String() string {
	return fmt.Sprintf("HashAggregateExec: groupExpr=%v, aggrExpr=%v", h.groupExprs, h.aggregateExprs)
}

func (h *HashAggregateExec) encodeCols(cols []any) string {
	s := ""
	for _, c := range cols {
		s += fmt.Sprint(c)
	}
	return s
}

package plans

import (
	"iter"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
)

type SelectionExec struct {
	input physicalplan.PhysicalPlan
	expr  physicalplan.PhysicalExpression
}

func (s *SelectionExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{s.input}
}

func (s *SelectionExec) Execute() iter.Seq[datatypes.RecordBatch] {
	rbIter := s.input.Execute()
	return func(yield func(datatypes.RecordBatch) bool) {
		for rb := range rbIter {
			evalRes := s.expr.Evaluate(rb)
			if evalRes.GetType() != datatypes.BooleanType {
				slog.Error(
					"SelectionExec expression result not boolean",
					slog.String("Type", evalRes.GetType().String()),
				)
				panic("Selection Exec expression not boolean")
			}

			filteredColArr := s.filter(rb, evalRes)
			if !yield(*datatypes.NewRecordBatch(rb.Schema, filteredColArr)) {
				return
			}
		}
	}
}

func (s *SelectionExec) Schema() datatypes.Schema {
	return s.Schema()
}

func (s *SelectionExec) filter(rb datatypes.RecordBatch, selectExprRes datatypes.ColumnArray) []datatypes.ColumnArray {
	fields := make([]datatypes.ColumnArray, rb.ColumnCount())
	for i := range rb.ColumnCount() {
		colArr := rb.Field(i)
		newColArr := datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), colArr.GetType())
		for j := range colArr.Size() {
			if selectExprRes.GetValue(j).(bool) {
				newColArr.Append(colArr.GetValue(j))
			}
		}
		fields[i] = newColArr.Build()
	}

	return fields
}

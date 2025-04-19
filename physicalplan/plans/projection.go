package plans

import (
	"fmt"
	"iter"

	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
)

// Projection execution plan evaluates the projection expressions against the input columns
// and then produces a Record Batch containing the derived columns
type ProjectionExec struct {
	input  physicalplan.PhysicalPlan
	schema datatypes.Schema
	exprs  []physicalplan.PhysicalExpression
}

func (p ProjectionExec) Children() []physicalplan.PhysicalPlan {
	return []physicalplan.PhysicalPlan{p.input}
}

// Projection execute needs to apply list of expressions
func (p ProjectionExec) Execute() iter.Seq[datatypes.RecordBatch] {
	recordBatchIter := p.input.Execute()
	return func(yield func(datatypes.RecordBatch) bool) {
		for rb := range recordBatchIter {
			fields := make([]datatypes.ColumnArray, len(p.exprs))
			for i := range len(p.exprs) {
				fields[i] = p.exprs[i].Evaluate(rb)
			}

			if !yield(*datatypes.NewRecordBatch(p.schema, fields)) {
				return
			}
		}
	}
}

func (p ProjectionExec) Schema() datatypes.Schema {
	return p.schema
}

func (p ProjectionExec) String() string {
	return fmt.Sprintf("ProjectionExec: %v", p.exprs)
}

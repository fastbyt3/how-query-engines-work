package physicalplan

import "github.com/fastbyt3/query-engine/datatypes"

// Physical Expressions contains code to evaluate the (logical) expressions at runtime.
// There can be one to many relationship between logical and physical plans
//
// Evaluated against record batches and results in columns
type PhysicalExpression interface {
	Evaluate(input datatypes.RecordBatch) datatypes.ColumnArray
}

package physicalplan

import (
	"iter"

	"github.com/fastbyt3/query-engine/datatypes"
)

// Physical plans go into "how" to do the underlying logical plan
//
// Logical plans and physical plans have an one to many relationship. For eg. there could be multiple
// physical plans (single / distributed execution or CPU / GPU execution) for a single logical plan
//
// Different algoorithms can also be accomodated for a logical plan. Example:
// Logical plan -> Aggregate; Physical plan(s): Group aggregate, Hash aggregate, etc.
type PhysicalPlan interface {
	Schema() datatypes.Schema
	Execute() iter.Seq[datatypes.RecordBatch]
	Children() []PhysicalPlan
}

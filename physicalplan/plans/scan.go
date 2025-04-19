package plans

import (
	"fmt"
	"iter"

	"github.com/fastbyt3/query-engine/datasources"
	"github.com/fastbyt3/query-engine/datatypes"
	"github.com/fastbyt3/query-engine/physicalplan"
)

type ScanExec struct {
	ds         datasources.DataSource
	projection []string
}

func (s ScanExec) Children() []physicalplan.PhysicalPlan {
	// Since scan is a leaf node it has no children
	return []physicalplan.PhysicalPlan{}
}

func (s ScanExec) Execute() iter.Seq[datatypes.RecordBatch] {
	return s.ds.Scan(s.projection)
}

func (s ScanExec) Schema() datatypes.Schema {
	initialSchema := s.ds.Schema()
	schema, _ := initialSchema.Select(s.projection)
	return schema
}

func (s ScanExec) String() string {
	return fmt.Sprintf("ScanExec: schema=%v, projection=%v", s.Schema(), s.projection)
}

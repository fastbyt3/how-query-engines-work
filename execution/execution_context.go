package execution

import (
	"github.com/fastbyt3/query-engine/datasources"
	"github.com/fastbyt3/query-engine/logicalplans"
)

type ExecutionContext struct {
	BatchSize int
}

func (e *ExecutionContext) CSV(filename string) logicalplans.Dataframe {
	return logicalplans.NewDefaultDataframe(logicalplans.NewScan(
		filename,
		datasources.NewCSVDatasource(filename, 1024),
		[]string{},
	))
}

func NewExecutionContext(batchSize int) *ExecutionContext {
	return &ExecutionContext{
		BatchSize: batchSize,
	}
}

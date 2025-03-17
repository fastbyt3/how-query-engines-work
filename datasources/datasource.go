package datasources

import (
	"iter"

	"github.com/fastbyt3/query-engine/datatypes"
)

type DataSource interface {
	Schema() datatypes.Schema
	Scan(projection []string) iter.Seq[datatypes.RecordBatch]
}

/*

Data source examples:

1. CSV
2. JSON
3. Parquet -> compressed efficient columnar data representation
4. ORC -> Optimized Row Column

*/

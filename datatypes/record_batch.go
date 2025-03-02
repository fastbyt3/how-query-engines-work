package datatypes

import "github.com/apache/arrow-go/v18/arrow"

// Record Batch represents a bunch of columnar data
type RecordBatch struct {
	schema arrow.Schema
	fields []ColumnVector
}

func (rb *RecordBatch) RowCount() int {
	return rb.fields[0].Size()
}

func (rb *RecordBatch) ColumnCount() int {
	return len(rb.fields)
}

func (rb *RecordBatch) Field(i int) ColumnVector {
	return rb.fields[i]
}

package datatypes

// Record Batch represents a bunch of columnar data
type RecordBatch struct {
	Schema Schema
	Fields []ColumnArray
}

func NewRecordBatch(schema Schema, fields []ColumnArray) *RecordBatch {
	return &RecordBatch{
		Schema: schema,
		Fields: fields,
	}
}

func (rb *RecordBatch) RowCount() int {
	return rb.Fields[0].Size()
}

func (rb *RecordBatch) ColumnCount() int {
	return len(rb.Fields)
}

func (rb *RecordBatch) Field(i int) ColumnArray {
	return rb.Fields[i]
}

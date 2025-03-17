package datatypes

// Record Batch represents a bunch of columnar data
type RecordBatch struct {
	schema Schema
	fields []ColumnArray
}

func NewRecordBatch(schema Schema, fields []ColumnArray) *RecordBatch {
	return &RecordBatch{
		schema: schema,
		fields: fields,
	}
}

func (rb *RecordBatch) RowCount() int {
	return rb.fields[0].Size()
}

func (rb *RecordBatch) ColumnCount() int {
	return len(rb.fields)
}

func (rb *RecordBatch) Field(i int) ColumnArray {
	return rb.fields[i]
}

package datasources

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fastbyt3/query-engine/datatypes"
)

type CSVDatasource struct {
	Filename  string
	schema    datatypes.Schema
	batchSize int

	reader *csv.Reader

	pjSchema  datatypes.Schema
	pjIndices []int
	builders  []datatypes.ArrowArrayBuilder
}

func NewCSVDatasource(filename string, batchSize int) *CSVDatasource {
	ds := CSVDatasource{
		Filename: filename,
	}

	if batchSize == 0 {
		ds.batchSize = 1024
	} else {
		ds.batchSize = batchSize
	}

	ds.inferSchema()

	return &ds
}

func (c *CSVDatasource) Scan(projection []string) iter.Seq[datatypes.RecordBatch] {
	slog.Info(fmt.Sprintf("scan() projection=%v", projection))
	c.inferProjection(projection)
	return c.createBatch(c.pjSchema, c.pjIndices)
}

func (c *CSVDatasource) createBatch(readSchema datatypes.Schema, readIndices []int) iter.Seq[datatypes.RecordBatch] {
	return func(yield func(datatypes.RecordBatch) bool) {
		rowsParsed := 0

		for {
			row, err := c.reader.Read()
			if err != nil && !errors.Is(err, io.EOF) {
				panic(fmt.Sprintf("unexpected error parsing csv, error = %v", err))
			}

			// build only for the projected schema
			for j := range readIndices {
				c.builders[j].Append(row[readIndices[j]])
			}

			fields := make([]datatypes.ColumnArray, len(readSchema.Fields()))
			for _, i := range readIndices {
				fields[i] = c.builders[i].Build()
			}
			rb := datatypes.NewRecordBatch(readSchema, fields)

			rowsParsed += 1
			if (err == io.EOF || rowsParsed == c.batchSize) && !yield(*rb) {
				return
			}
		}
	}
}

func (c *CSVDatasource) inferProjection(projection []string) {
	c.pjSchema, c.pjIndices = c.schema.Select(projection)
}

func (c *CSVDatasource) Schema() datatypes.Schema {
	return c.schema
}

func (c *CSVDatasource) inferSchema() {
	f, err := os.ReadFile(c.Filename)
	if err != nil {
		panic(fmt.Sprintf("failed to read file: %s. Error = %s", c.Filename, err.Error()))
	}

	reader := csv.NewReader(bytes.NewReader(f))
	headerRow, err := reader.Read()
	if err != nil {
		panic(fmt.Sprintf("faild to read header row. Error = %s", err.Error()))
	}

	var headers []arrow.Field
	for _, cell := range headerRow {
		headers = append(headers, arrow.Field{Name: cell, Type: datatypes.StringType})
	}

	c.reader = reader
	c.schema = *datatypes.NewSchema(headers)

	c.builders = make([]datatypes.ArrowArrayBuilder, len(c.schema.Fields()))
	for i, field := range c.schema.Fields() {
		c.builders[i] = datatypes.NewArrowArrayBuilder(memory.NewGoAllocator(), field.Type)
	}
}

var _ DataSource = (*CSVDatasource)(nil)

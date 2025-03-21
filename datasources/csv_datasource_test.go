package datasources_test

import (
	"fmt"
	"iter"
	"testing"

	"github.com/fastbyt3/query-engine/datasources"
	"github.com/stretchr/testify/require"
)

const SAMPLE_CSV_FILE = "../test-data/sample.csv"

func TestCSVDatasourceSchema(t *testing.T) {
	ds := datasources.NewCSVDatasource(SAMPLE_CSV_FILE, 10)
	dsIterator := ds.Scan([]string{})

	next, stop := iter.Pull(dsIterator)
	defer stop()

	recordBatch, valid := next()
	require.True(t, valid)
	require.Len(t, recordBatch.Fields, 13)

	actualHeaders := []string{"id", "model", "mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb"}
	for i, v := range actualHeaders {
		require.Equal(t, v, recordBatch.Schema.Field(i).Name)
	}
}

func TestCSVDatasourceScan(t *testing.T) {
	firstRowData := []string{"1", "Mazda RX4", "21", "6", "160", "110", "3.9", "2.62", "16.46", "0", "1", "4", "4"}

	// Read with batch size of 1024 => read all rows
	ds := datasources.NewCSVDatasource(SAMPLE_CSV_FILE, 1024)
	dsIterator := ds.Scan([]string{})
	next, stop := iter.Pull(dsIterator)
	defer stop()
	recordBatch, valid := next()
	require.True(t, valid)
	firstCol := recordBatch.Field(0)
	require.Equal(t, 32, firstCol.Size())
	fmt.Println("firstCol :: ", firstCol)

	for i := range len(firstRowData) {
		col := recordBatch.Field(i)
		// col.GetValue needs 0 since we are checking for first row only
		require.Equal(t, firstRowData[i], col.GetValue(0))
	}

	// Read with batch size of 10 => read only 10 rows
	ds = datasources.NewCSVDatasource(SAMPLE_CSV_FILE, 10)
	dsIterator = ds.Scan([]string{})
	next, stop = iter.Pull(dsIterator)
	defer stop()
	recordBatch, valid = next()
	require.True(t, valid)
	firstCol = recordBatch.Field(0)
	require.Equal(t, 10, firstCol.Size())
	// check if last row's id matches
	require.Equal(t, "10", firstCol.GetValue(firstCol.Size()-1))
}

func TestCSVDatasourceProjectionScan(t *testing.T) {
	ds := datasources.NewCSVDatasource(SAMPLE_CSV_FILE, 10)
	dsIterator := ds.Scan([]string{"id", "model"})
	next, stop := iter.Pull(dsIterator)
	defer stop()
	rb, valid := next()
	require.True(t, valid)
	require.Len(t, rb.Fields, 2)

	firstRowData := []string{"1", "Mazda RX4"}
	for i := range len(firstRowData) {
		require.Equal(t, firstRowData[i], rb.Field(i).GetValue(0))
	}
}

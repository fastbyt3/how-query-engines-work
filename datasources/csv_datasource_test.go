package datasources_test

import (
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
	require.Len(t, recordBatch.Fields, 12)

	actualHeaders := []string{"model", "mpg", "cyl", "disp", "hp", "drat", "wt", "qsec", "vs", "am", "gear", "carb"}
	for i, v := range actualHeaders {
		require.Equal(t, v, recordBatch.Schema.Field(i).Name)
	}
}

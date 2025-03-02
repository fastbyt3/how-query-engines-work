package datatypes

import (
	"github.com/apache/arrow-go/v18/arrow"
)

var (
	BooleanType = &arrow.BooleanType{}
	Int8Type    = &arrow.Int8Type{}
	Int16Type   = &arrow.Int16Type{}
	Int32Type   = &arrow.Int32Type{}
	Int64Type   = &arrow.Int64Type{}
	UInt8Type   = &arrow.Uint8Type{}
	UInt16Type  = &arrow.Uint16Type{}
	UInt32Type  = &arrow.Uint32Type{}
	UInt64Type  = &arrow.Uint64Type{}
	FloatType   = &arrow.Float32Type{}
	DoubleType  = &arrow.Float64Type{}
	StringType  = &arrow.StringType{}
)

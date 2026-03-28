package arrowtest

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Int32Batch builds a single-column RecordBatch (as arrow.Record).
// In Arrow Go v18, RecordBatch is represented via arrow.Record.
func Int32Batch(values ...int32) arrow.RecordBatch {
	mem := memory.NewGoAllocator()

	b := array.NewInt32Builder(mem)
	defer b.Release()

	for _, v := range values {
		b.Append(v)
	}

	arr := b.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(len(values)))

	return rec
}

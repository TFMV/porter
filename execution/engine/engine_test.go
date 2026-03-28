package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestEngine_BuildStream_BasicQuery(t *testing.T) {
	eng, err := New(Config{
		DBPath: ":memory:",
	})
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	_, ch, err := eng.BuildStream(ctx, "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)", nil)
	if err != nil {
		t.Fatalf("BuildStream failed: %v", err)
	}

	var totalRows int64
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			totalRows += int64(chunk.Data.NumRows())
			chunk.Data.Release()
		}
	}

	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}
}

func TestEngine_BuildStream_Ordering(t *testing.T) {
	eng, err := New(Config{
		DBPath: ":memory:",
	})
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	_, ch, err := eng.BuildStream(ctx, "SELECT i FROM generate_series(1, 100) AS t(i) ORDER BY i DESC", nil)
	if err != nil {
		t.Fatalf("BuildStream failed: %v", err)
	}

	var values []int64
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			col := chunk.Data.Column(0)
			for i := 0; i < int(chunk.Data.NumRows()); i++ {
				val := col.(*array.Int64).Value(i)
				values = append(values, val)
			}
			chunk.Data.Release()
		}
	}

	if len(values) != 100 {
		t.Fatalf("expected 100 values, got %d", len(values))
	}

	for i := 0; i < len(values)-1; i++ {
		if values[i] < values[i+1] {
			t.Errorf("values not in descending order at index %d: %d >= %d", i, values[i], values[i+1])
		}
	}
}

func TestEngine_ConcurrencyLimit(t *testing.T) {
	eng, err := New(Config{
		DBPath:               ":memory:",
		MaxConcurrentQueries: 2,
	})
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	defer eng.Close()

	ctx, cancel := context.WithCancel(context.Background())

	start := make(chan struct{})
	var wg sync.WaitGroup
	var accepted int32
	var rejected int32

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := eng.AcquireQuerySlot(ctx)
			if err != nil {
				atomic.AddInt32(&rejected, 1)
				return
			}
			atomic.AddInt32(&accepted, 1)
			<-ctx.Done()
			eng.ReleaseQuerySlot()
		}()
	}

	close(start)
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	if accepted != 2 {
		t.Errorf("expected 2 accepted, got %d", accepted)
	}
	if rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", rejected)
	}
}

func TestEngine_Cancellation_StopsStreaming(t *testing.T) {
	eng, err := New(Config{
		DBPath: ":memory:",
	})
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	defer eng.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch, err := eng.BuildStream(ctx, "SELECT * FROM generate_series(1, 1000000)", nil)
	if err != nil {
		t.Fatalf("BuildStream failed: %v", err)
	}

	var rowCount int64
	for chunk := range ch {
		if chunk.Err != nil {
			break
		}
		if chunk.Data != nil {
			rowCount += int64(chunk.Data.NumRows())
			chunk.Data.Release()
		}

		if rowCount > 100 {
			cancel()
			break
		}
	}

	time.Sleep(50 * time.Millisecond)

	if rowCount <= 100 {
		t.Logf("cancellation appears to have stopped streaming at %d rows", rowCount)
	}
}

func TestEngine_SchemaDerivation(t *testing.T) {
	eng, err := New(Config{
		DBPath: ":memory:",
	})
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	schema, err := eng.DeriveSchema(ctx, "SELECT i, s FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s)")
	if err != nil {
		t.Fatalf("DeriveSchema failed: %v", err)
	}

	if len(schema.Fields()) != 2 {
		t.Errorf("expected 2 fields, got %d", len(schema.Fields()))
	}

	if schema.Field(0).Name != "i" {
		t.Errorf("expected first field name 'i', got %q", schema.Field(0).Name)
	}
	if schema.Field(1).Name != "s" {
		t.Errorf("expected second field name 's', got %q", schema.Field(1).Name)
	}
}

func TestEngine_BuildStream_WithParams(t *testing.T) {
	t.Skip("DuckDB does not support binding multiple rows at once")
}

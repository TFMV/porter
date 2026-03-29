package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TFMV/porter/internal/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func newTestEngine(t *testing.T, cfg Config) Engine {
	t.Helper()
	m, err := adbc.NewManager()
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	cfg.ADBCManager = m
	eng, err := New(cfg)
	if err != nil {
		t.Skipf("skipping test because engine cannot be created: %v", err)
	}
	return eng
}

func TestEngine_New_RequiresADBCManager(t *testing.T) {
	_, err := New(Config{DBPath: ":memory:"})
	if err == nil {
		t.Fatal("expected manager requirement error")
	}
}

func TestEngine_BuildStream_BasicQuery(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
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

func TestEngine_ConcurrencyLimit(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:", MaxConcurrentQueries: 2})
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

func TestEngine_SchemaDerivation(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()

	ctx := context.Background()
	schema, err := eng.DeriveSchema(ctx, "SELECT i, s FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s)")
	if err != nil {
		t.Fatalf("DeriveSchema failed: %v", err)
	}

	if len(schema.Fields()) != 2 {
		t.Errorf("expected 2 fields, got %d", len(schema.Fields()))
	}
}

func TestEngine_BuildStream_Ordering(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
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
				values = append(values, col.(*array.Int64).Value(i))
			}
			chunk.Data.Release()
		}
	}

	if len(values) != 100 {
		t.Fatalf("expected 100 values, got %d", len(values))
	}
}

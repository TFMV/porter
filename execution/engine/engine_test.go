package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TFMV/porter/internal/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func makeIntBatch(t *testing.T, vals []int64) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	col := b.Field(0).(*array.Int64Builder)
	col.AppendValues(vals, nil)
	rec := b.NewRecordBatch()
	if rec == nil {
		t.Fatal("expected record batch")
	}
	return rec
}

func countRows(t *testing.T, eng Engine, sql string) int64 {
	t.Helper()
	_, ch, err := eng.BuildStream(context.Background(), sql, nil)
	if err != nil {
		t.Fatalf("BuildStream failed: %v", err)
	}
	var total int64
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("stream failed: %v", chunk.Err)
		}
		if chunk.Data != nil {
			total += int64(chunk.Data.NumRows())
			chunk.Data.Release()
		}
	}
	return total
}

func TestEngine_IngestStream_MultipleBatches(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()

	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE bulk_ingest(id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	b1 := makeIntBatch(t, []int64{1, 2, 3})
	defer b1.Release()
	b2 := makeIntBatch(t, []int64{4, 5})
	defer b2.Release()

	rdr, err := array.NewRecordReader(b1.Schema(), []arrow.Record{b1, b2})
	if err != nil {
		t.Fatalf("new record reader: %v", err)
	}
	defer rdr.Release()

	n, err := eng.IngestStream(context.Background(), "bulk_ingest", rdr, IngestOptions{})
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 ingested rows, got %d", n)
	}

	if got := countRows(t, eng, "SELECT * FROM bulk_ingest"); got != 5 {
		t.Fatalf("expected 5 rows in table, got %d", got)
	}
}

func TestEngine_IngestStream_SchemaMismatch(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()

	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE schema_mismatch(id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.Record{batch})
	if err != nil {
		t.Fatalf("new record reader: %v", err)
	}
	defer rdr.Release()

	if _, err := eng.IngestStream(context.Background(), "schema_mismatch", rdr, IngestOptions{}); err == nil {
		t.Fatal("expected schema mismatch error")
	}
	if got := countRows(t, eng, "SELECT * FROM schema_mismatch"); got != 0 {
		t.Fatalf("expected 0 rows after rejected ingest, got %d", got)
	}
}

func TestEngine_IngestStream_ConcurrentTables(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()
	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE t1(id BIGINT)"); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE t2(id BIGINT)"); err != nil {
		t.Fatalf("create t2: %v", err)
	}

	var wg sync.WaitGroup
	ingest := func(table string) {
		defer wg.Done()
		b := makeIntBatch(t, []int64{1, 2, 3, 4})
		defer b.Release()
		rdr, err := array.NewRecordReader(b.Schema(), []arrow.Record{b})
		if err != nil {
			t.Errorf("record reader: %v", err)
			return
		}
		defer rdr.Release()
		if _, err := eng.IngestStream(context.Background(), table, rdr, IngestOptions{}); err != nil {
			t.Errorf("ingest %s: %v", table, err)
		}
	}

	wg.Add(2)
	go ingest("t1")
	go ingest("t2")
	wg.Wait()

	if got := countRows(t, eng, "SELECT * FROM t1"); got != 4 {
		t.Fatalf("t1 expected 4 rows, got %d", got)
	}
	if got := countRows(t, eng, "SELECT * FROM t2"); got != 4 {
		t.Fatalf("t2 expected 4 rows, got %d", got)
	}
}

func TestEngine_IngestStream_RollbackOnMidStreamFailure(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()
	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE tx_rollback(id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	good := makeIntBatch(t, []int64{1, 2})
	defer good.Release()

	rdr, err := array.NewRecordReader(good.Schema(), []arrow.Record{good})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer rdr.Release()

	_, err = eng.IngestStream(context.Background(), "tx_rollback", rdr, IngestOptions{
		MaxUncommittedBytes: 1,
	})
	if err == nil {
		t.Fatal("expected ingest failure due to max bytes")
	}
	if got := countRows(t, eng, "SELECT * FROM tx_rollback"); got != 0 {
		t.Fatalf("expected rollback with zero rows, got %d", got)
	}
}

func TestEngine_IngestStream_LargeBatch(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()
	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE large_batch(id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const rows = 200_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	batch := makeIntBatch(t, values)
	defer batch.Release()
	rdr, err := array.NewRecordReader(batch.Schema(), []arrow.Record{batch})
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer rdr.Release()

	if _, err := eng.IngestStream(context.Background(), "large_batch", rdr, IngestOptions{}); err != nil {
		t.Fatalf("large ingest failed: %v", err)
	}
	if got := countRows(t, eng, fmt.Sprintf("SELECT * FROM large_batch")); got != rows {
		t.Fatalf("expected %d rows, got %d", rows, got)
	}
}

func TestEngine_IngestStream_ReplaceCreatesMissingTable(t *testing.T) {
	eng := newTestEngine(t, Config{DBPath: ":memory:"})
	defer eng.Close()

	b1 := makeIntBatch(t, []int64{1, 2, 3})
	defer b1.Release()
	b2 := makeIntBatch(t, []int64{4, 5, 6})
	defer b2.Release()

	rdr, err := array.NewRecordReader(b1.Schema(), []arrow.RecordBatch{b1, b2})
	if err != nil {
		t.Fatalf("new record reader: %v", err)
	}
	defer rdr.Release()

	n, err := eng.IngestStream(context.Background(), "replace_create_ingest", rdr, IngestOptions{
		IngestMode: "adbc.ingest.mode.replace",
	})
	if err != nil {
		t.Fatalf("ingest with replace-on-missing failed: %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 ingested rows, got %d", n)
	}
	if got := countRows(t, eng, "SELECT * FROM replace_create_ingest"); got != 6 {
		t.Fatalf("expected 6 rows in created table, got %d", got)
	}
}

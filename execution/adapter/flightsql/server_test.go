package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TFMV/porter/execution/engine"
	"github.com/TFMV/porter/internal/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeMessageReader struct {
	batches []arrow.RecordBatch
	index   int
	err     error
}

func (r *fakeMessageReader) Retain()  {}
func (r *fakeMessageReader) Release() {}
func (r *fakeMessageReader) Schema() *arrow.Schema {
	if len(r.batches) == 0 {
		return nil
	}
	return r.batches[0].Schema()
}
func (r *fakeMessageReader) Next() bool {
	if r.index >= len(r.batches) {
		return false
	}
	r.index++
	return true
}
func (r *fakeMessageReader) RecordBatch() arrow.RecordBatch {
	if r.index == 0 || r.index > len(r.batches) {
		return nil
	}
	return r.batches[r.index-1]
}
func (r *fakeMessageReader) Record() arrow.RecordBatch { return r.RecordBatch() }
func (r *fakeMessageReader) Err() error                { return r.err }
func (r *fakeMessageReader) Read() (arrow.RecordBatch, error) {
	if !r.Next() {
		return nil, io.EOF
	}
	return r.RecordBatch(), nil
}
func (r *fakeMessageReader) Chunk() flight.StreamChunk                        { return flight.StreamChunk{} }
func (r *fakeMessageReader) LatestFlightDescriptor() *flight.FlightDescriptor { return nil }
func (r *fakeMessageReader) LatestAppMetadata() []byte                        { return nil }

type ingestCaptureEngine struct {
	table       string
	calls       int32
	updateCalls int32
	seenRows    int64
	seenBatches int
	lastOpts    engine.IngestOptions
	lastSchema  *arrow.Schema
}

func (e *ingestCaptureEngine) BuildStream(context.Context, string, arrow.RecordBatch) (*arrow.Schema, <-chan engine.StreamChunk, error) {
	return nil, nil, nil
}
func (e *ingestCaptureEngine) IngestStream(_ context.Context, table string, reader array.RecordReader, opts engine.IngestOptions) (int64, error) {
	atomic.AddInt32(&e.calls, 1)
	e.table = table
	e.lastOpts = opts
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec == nil {
			continue
		}
		e.seenBatches++
		e.seenRows += rec.NumRows()
		if e.lastSchema == nil {
			e.lastSchema = rec.Schema()
		}
	}
	if err := reader.Err(); err != nil {
		return 0, err
	}
	return e.seenRows, nil
}
func (e *ingestCaptureEngine) AcquireQuerySlot(context.Context) error { return nil }
func (e *ingestCaptureEngine) ReleaseQuerySlot()                      {}
func (e *ingestCaptureEngine) DeriveSchema(context.Context, string) (*arrow.Schema, error) {
	return nil, nil
}
func (e *ingestCaptureEngine) ExecuteUpdate(context.Context, string) (int64, error) {
	atomic.AddInt32(&e.updateCalls, 1)
	return 0, nil
}
func (e *ingestCaptureEngine) Allocator() memory.Allocator { return memory.NewGoAllocator() }
func (e *ingestCaptureEngine) Close() error                { return nil }

type testPreparedStatementQuery struct {
	handle []byte
}

func (q testPreparedStatementQuery) GetPreparedStatementHandle() []byte {
	return q.handle
}

type testStatementIngest struct {
	table string
}

func (t testStatementIngest) GetTableDefinitionOptions() *fsql.TableDefinitionOptions { return nil }
func (t testStatementIngest) GetTable() string                                        { return t.table }
func (t testStatementIngest) GetSchema() string                                       { return "" }
func (t testStatementIngest) GetCatalog() string                                      { return "" }
func (t testStatementIngest) GetTemporary() bool                                      { return false }
func (t testStatementIngest) GetTransactionId() []byte                                { return nil }
func (t testStatementIngest) GetOptions() map[string]string                           { return nil }

func makeInt32Batch(t *testing.T, values []int32) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	intBuilder := builder.Field(0).(*array.Int32Builder)
	for _, v := range values {
		intBuilder.Append(v)
	}
	rec := builder.NewRecordBatch()
	if rec == nil {
		t.Fatal("failed to build test record batch")
	}
	return rec
}

func TestBatchGuardIdempotentRelease(t *testing.T) {
	batch := makeInt32Batch(t, []int32{1})
	guard := NewBatchGuard(batch)
	retained := guard.Retain()
	if retained == nil {
		t.Fatal("expected retained batch")
	}
	retained.Release()
	guard.Release()
	guard.Release()
}

func TestDoPutPreparedStatementQueryOnlyRetainsWhenHandleExists(t *testing.T) {
	s := &Server{preparedStmts: make(map[string]preparedEntry)}
	handle := "test-handle"
	s.preparedStmts[handle] = preparedEntry{sql: "SELECT 1", expires: time.Now().Add(time.Minute)}

	batch := makeInt32Batch(t, []int32{1})
	reader := &fakeMessageReader{batches: []arrow.RecordBatch{batch}}
	req := testPreparedStatementQuery{handle: []byte(handle)}

	got, err := s.DoPutPreparedStatementQuery(context.Background(), req, reader, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != handle {
		t.Fatalf("unexpected handle: got %q, want %q", got, handle)
	}

	e, ok := s.preparedStmts[handle]
	if !ok {
		t.Fatal("prepared statement missing after DoPutPreparedStatementQuery")
	}
	if e.paramGuard == nil {
		t.Fatal("expected paramGuard to be set")
	}
	batch.Release()
	e.paramGuard.Release()
}

func TestDoPutPreparedStatementQueryReturnsNotFoundOnMissingHandle(t *testing.T) {
	s := &Server{preparedStmts: make(map[string]preparedEntry)}
	batch := makeInt32Batch(t, []int32{1})
	reader := &fakeMessageReader{batches: []arrow.RecordBatch{batch}}
	req := testPreparedStatementQuery{handle: []byte("missing")}

	_, err := s.DoPutPreparedStatementQuery(context.Background(), req, reader, nil)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	batch.Release()
}

func TestAcquireQuerySlotLimitsConcurrency(t *testing.T) {
	s, err := NewServer(Config{MaxConcurrentQueries: 1})
	if err != nil {
		t.Skipf("skipping test: %v", err)
	}
	defer s.Close()

	if err := s.acquireQuerySlot(context.Background()); err != nil {
		t.Fatalf("failed to acquire first slot: %v", err)
	}
	defer s.releaseQuerySlot()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.acquireQuerySlot(ctx); err == nil {
		t.Fatal("expected second slot acquisition to fail")
	}
}

func TestDoGetPreparedStatementWithNilParams(t *testing.T) {
	s, err := NewServer(Config{DevMode: true})
	if err != nil {
		t.Skipf("skipping integration test because server cannot be created: %v", err)
	}
	defer s.Close()

	handle, err := newHandle()
	if err != nil {
		t.Fatal(err)
	}
	s.preparedStmts[handle] = preparedEntry{sql: "SELECT 1", expires: time.Now().Add(time.Minute)}

	schema, ch, err := s.DoGetPreparedStatement(context.Background(), testPreparedStatementQuery{handle: []byte(handle)})
	if err != nil {
		t.Fatalf("DoGetPreparedStatement failed: %v", err)
	}
	if schema == nil {
		t.Fatal("expected non-nil schema")
	}

	count := 0
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		if chunk.Data != nil {
			chunk.Data.Release()
			count++
		}
	}
	if count == 0 {
		t.Fatal("expected at least one result chunk")
	}
}

func TestBatchGuardConcurrentAccess(t *testing.T) {
	t.Parallel()

	batch := makeInt32Batch(t, []int32{1, 2, 3})
	guard := NewBatchGuard(batch)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := guard.Retain()
			if rec != nil {
				time.Sleep(time.Millisecond)
				rec.Release()
			}
		}()
	}

	wg.Wait()
	guard.Release()
	guard.Release()
}

func TestDoPutPreparedStatementQueryConcurrent(t *testing.T) {
	t.Parallel()

	s := &Server{preparedStmts: make(map[string]preparedEntry)}
	handle := "test"
	s.preparedStmts[handle] = preparedEntry{
		sql:     "SELECT 1",
		expires: time.Now().Add(time.Minute),
	}

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			batch := makeInt32Batch(t, []int32{int32(i)})
			reader := &fakeMessageReader{batches: []arrow.RecordBatch{batch}}

			_, err := s.DoPutPreparedStatementQuery(context.Background(),
				testPreparedStatementQuery{handle: []byte(handle)},
				reader,
				nil,
			)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			batch.Release()
		}(i)
	}

	wg.Wait()

	e := s.preparedStmts[handle]
	if e.paramGuard == nil {
		t.Fatal("expected paramGuard after concurrent writes")
	}

	e.paramGuard.Release()
}

func TestDoPutCommandStatementIngest(t *testing.T) {
	eng := &ingestCaptureEngine{}
	s := &Server{Engine: eng}
	batch1 := makeInt32Batch(t, []int32{1, 2})
	defer batch1.Release()
	batch2 := makeInt32Batch(t, []int32{3, 4, 5})
	defer batch2.Release()
	reader := &fakeMessageReader{batches: []arrow.RecordBatch{batch1, batch2}}

	count, err := s.DoPutCommandStatementIngest(context.Background(), testStatementIngest{table: "ingest_target"}, reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected row count 5, got %d", count)
	}
	if atomic.LoadInt32(&eng.calls) != 1 {
		t.Fatalf("expected ingest call count 1, got %d", eng.calls)
	}
	if atomic.LoadInt32(&eng.updateCalls) != 0 {
		t.Fatalf("expected SQL fallback path to remain unused, got %d ExecuteUpdate calls", eng.updateCalls)
	}
	if eng.seenBatches != 2 {
		t.Fatalf("expected 2 streamed batches, got %d", eng.seenBatches)
	}
	if eng.lastSchema == nil {
		t.Fatal("expected streaming reader to preserve the first batch schema")
	}
}

type gatedEngine struct {
	engine.Engine
	blockBefore int
	blocked     chan struct{}
	release     chan struct{}
}

func (g *gatedEngine) IngestStream(ctx context.Context, table string, reader array.RecordReader, opts engine.IngestOptions) (int64, error) {
	return g.Engine.IngestStream(ctx, table, &gatedRecordReader{
		src:         reader,
		blockBefore: g.blockBefore,
		blocked:     g.blocked,
		release:     g.release,
	}, opts)
}

type gatedRecordReader struct {
	src         array.RecordReader
	index       int
	blockBefore int
	blocked     chan struct{}
	release     chan struct{}
	blockOnce   sync.Once
}

func (r *gatedRecordReader) Retain()  {}
func (r *gatedRecordReader) Release() {}
func (r *gatedRecordReader) Schema() *arrow.Schema {
	return r.src.Schema()
}
func (r *gatedRecordReader) Next() bool {
	if r.index == r.blockBefore {
		r.blockOnce.Do(func() { close(r.blocked) })
		<-r.release
	}
	if !r.src.Next() {
		return false
	}
	r.index++
	return true
}
func (r *gatedRecordReader) RecordBatch() arrow.RecordBatch { return r.src.RecordBatch() }
func (r *gatedRecordReader) Record() arrow.RecordBatch      { return r.RecordBatch() }
func (r *gatedRecordReader) Err() error                     { return r.src.Err() }

func makeInt64Batch(t *testing.T, alloc memory.Allocator, values []int64) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues(values, nil)
	rec := builder.NewRecordBatch()
	if rec == nil {
		t.Fatal("failed to build int64 record batch")
	}
	return rec
}

func newIntegrationEngine(t *testing.T) engine.Engine {
	t.Helper()
	manager, err := adbc.NewManager()
	if err != nil {
		t.Fatalf("create adbc manager: %v", err)
	}
	eng, err := engine.New(engine.Config{
		DBPath:      ":memory:",
		ADBCManager: manager,
		DevMode:     true,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Skipf("skipping integration test because engine cannot be created: %v", err)
	}
	return eng
}

func countRowsFromEngine(t *testing.T, eng engine.Engine, sql string) int64 {
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
			total += chunk.Data.NumRows()
			chunk.Data.Release()
		}
	}
	return total
}

func TestFlightSQLDoPutStatementIngestStreamingCommitAndRollback(t *testing.T) {
	baseEngine := newIntegrationEngine(t)
	eng := &gatedEngine{
		Engine:      baseEngine,
		blockBefore: 2,
		blocked:     make(chan struct{}),
		release:     make(chan struct{}),
	}

	srv, err := NewServer(Config{
		Engine: eng,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, srv.AsFlightServer())
	defer grpcServer.Stop()
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	client, err := fsql.NewClient(
		"passthrough:///bufnet",
		nil,
		nil,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("new flightsql client: %v", err)
	}
	defer client.Close()

	if _, err := eng.ExecuteUpdate(context.Background(), "CREATE TABLE ingest_target(id BIGINT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	checked, ok := baseEngine.Allocator().(*memory.CheckedAllocator)
	if !ok {
		t.Fatal("expected checked allocator in dev mode")
	}
	baseline := checked.CurrentAlloc()

	clientAlloc := memory.NewGoAllocator()
	batch1 := makeInt64Batch(t, clientAlloc, []int64{1, 2})
	defer batch1.Release()
	batch2 := makeInt64Batch(t, clientAlloc, []int64{3, 4})
	defer batch2.Release()
	batch3 := makeInt64Batch(t, clientAlloc, []int64{5, 6})
	defer batch3.Release()

	successReader, err := array.NewRecordReader(batch1.Schema(), []arrow.RecordBatch{batch1, batch2, batch3})
	if err != nil {
		t.Fatalf("new success record reader: %v", err)
	}
	defer successReader.Release()

	type ingestResult struct {
		rows int64
		err  error
	}
	successDone := make(chan ingestResult, 1)
	go func() {
		rows, err := client.ExecuteIngest(context.Background(), successReader, &fsql.ExecuteIngestOpts{
			TableDefinitionOptions: &fsql.TableDefinitionOptions{
				IfNotExist: fsql.TableDefinitionOptionsTableNotExistOptionFail,
				IfExists:   fsql.TableDefinitionOptionsTableExistsOptionAppend,
			},
			Table: "ingest_target",
		})
		successDone <- ingestResult{rows: rows, err: err}
	}()

	select {
	case <-eng.blocked:
	case result := <-successDone:
		t.Fatalf("ingest returned before streaming checkpoint: rows=%d err=%v", result.rows, result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for engine ingest stream to block")
	}

	if got := countRowsFromEngine(t, eng, "SELECT * FROM ingest_target"); got != 0 {
		t.Fatalf("expected no partial visibility before commit, got %d rows", got)
	}

	close(eng.release)

	success := <-successDone
	if success.err != nil {
		t.Fatalf("successful ingest failed: %v", success.err)
	}
	if success.rows != 6 {
		t.Fatalf("expected 6 ingested rows, got %d", success.rows)
	}
	if got := countRowsFromEngine(t, eng, "SELECT * FROM ingest_target"); got != 6 {
		t.Fatalf("expected 6 rows after commit, got %d", got)
	}

	failBatch1 := makeInt64Batch(t, clientAlloc, []int64{7, 8})
	defer failBatch1.Release()
	failBatch2 := makeInt64Batch(t, clientAlloc, []int64{9, 10})
	defer failBatch2.Release()
	failBatch3 := makeInt64Batch(t, clientAlloc, []int64{11, 12})
	defer failBatch3.Release()

	failReader, err := array.NewRecordReader(failBatch1.Schema(), []arrow.RecordBatch{failBatch1, failBatch2, failBatch3})
	if err != nil {
		t.Fatalf("new failing record reader: %v", err)
	}
	defer failReader.Release()

	failedRows, err := client.ExecuteIngest(context.Background(), failReader, &fsql.ExecuteIngestOpts{
		TableDefinitionOptions: &fsql.TableDefinitionOptions{
			IfNotExist: fsql.TableDefinitionOptionsTableNotExistOptionFail,
			IfExists:   fsql.TableDefinitionOptionsTableExistsOptionAppend,
		},
		Table: "ingest_target",
		Options: map[string]string{
			maxUncommittedBytesOption: "1",
		},
	})
	if err == nil {
		t.Fatal("expected bounded-memory ingest failure")
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	if failedRows != 0 {
		t.Fatalf("expected failed ingest to report 0 committed rows, got %d", failedRows)
	}
	if got := countRowsFromEngine(t, eng, "SELECT * FROM ingest_target"); got != 6 {
		t.Fatalf("expected rollback to preserve committed rows only, got %d", got)
	}

	checked.AssertSize(t, baseline)
}

func TestDoGetPreparedStatementConcurrentReaders(t *testing.T) {
	t.Parallel()

	s, err := NewServer(Config{DevMode: true, MaxConcurrentQueries: 4})
	if err != nil {
		t.Skip(err)
	}
	defer s.Close()

	handle, _ := newHandle()
	s.preparedStmts[handle] = preparedEntry{
		sql:     "SELECT 1",
		expires: time.Now().Add(time.Minute),
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, ch, err := s.DoGetPreparedStatement(context.Background(),
				testPreparedStatementQuery{handle: []byte(handle)},
			)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			for chunk := range ch {
				if chunk.Err != nil {
					t.Errorf("stream error: %v", chunk.Err)
				}
				if chunk.Data != nil {
					chunk.Data.Release()
				}
			}
		}()
	}

	wg.Wait()
}

/*
	func TestShutdownCancelsActiveQuery(t *testing.T) {
		t.Parallel()

		s, err := NewServer(Config{DevMode: true})
		if err != nil {
			t.Skip(err)
		}

		handle, _ := newHandle()
		s.preparedStmts[handle] = preparedEntry{
			sql:     "SELECT * FROM range(100000000)", // long-running
			expires: time.Now().Add(time.Minute),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)

		go func() {
			_, ch, err := s.DoGetPreparedStatement(ctx,
				testPreparedStatementQuery{handle: []byte(handle)},
			)
			if err != nil {
				done <- err
				return
			}

			for chunk := range ch {
				if chunk.Err != nil {
					done <- chunk.Err
					return
				}
				if chunk.Data != nil {
					chunk.Data.Release()
				}
			}

			done <- nil
		}()

		time.Sleep(50 * time.Millisecond)
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}

		select {
		case err := <-done:
			if err == nil {
				t.Fatal("expected cancellation error")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown did not cancel query in time")
		}
	}
*/
func TestAcquireQuerySlotParallel(t *testing.T) {
	t.Parallel()

	s, err := NewServer(Config{MaxConcurrentQueries: 2})
	if err != nil {
		t.Skipf("skipping test: %v", err)
	}
	defer s.Close()

	var wg sync.WaitGroup
	success := int32(0)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			if err := s.acquireQuerySlot(ctx); err == nil {
				atomic.AddInt32(&success, 1)
				time.Sleep(10 * time.Millisecond)
				s.releaseQuerySlot()
			}
		}()
	}

	wg.Wait()

	if success == 0 {
		t.Fatal("expected some successful slot acquisitions")
	}
}

func TestEvictionReleasesGuards(t *testing.T) {
	t.Parallel()

	s := &Server{
		preparedStmts: make(map[string]preparedEntry),
	}

	batch := makeInt32Batch(t, []int32{1})
	guard := NewBatchGuard(batch)

	s.preparedStmts["x"] = preparedEntry{
		sql:        "SELECT 1",
		paramGuard: guard,
		expires:    time.Now().Add(-time.Minute),
	}

	s.evict()

	if len(s.preparedStmts) != 0 {
		t.Fatal("expected eviction to remove expired statement")
	}

	// Should be safe to call again (idempotency)
	guard.Release()
}

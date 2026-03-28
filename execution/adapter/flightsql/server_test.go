package server

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
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

type testPreparedStatementQuery struct {
	handle []byte
}

func (q testPreparedStatementQuery) GetPreparedStatementHandle() []byte {
	return q.handle
}

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
	rec := builder.NewRecord()
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
	s := &Server{querySemaphore: make(chan struct{}, 1)}
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
	s.zapLogger = zap.NewNop()

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

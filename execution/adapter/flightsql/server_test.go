package server

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

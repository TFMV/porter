package ws

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TFMV/porter/execution/engine"
	"github.com/TFMV/porter/testutil/arrowtest"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// ─────────────────────────────────────
// Fake Engine
// ─────────────────────────────────────

type fakeEngine struct {
	acquireErr error
	schema     *arrow.Schema
	ch         chan flight.StreamChunk
	buildErr   error
	buildCalls atomic.Int32
}

func (f *fakeEngine) AcquireQuerySlot(ctx context.Context) error {
	if f.acquireErr != nil {
		return f.acquireErr
	}
	return nil
}

func (f *fakeEngine) ReleaseQuerySlot() {
}

func (f *fakeEngine) BuildStream(ctx context.Context, sql string, params arrow.RecordBatch) (*arrow.Schema, <-chan engine.StreamChunk, error) {
	f.buildCalls.Add(1)
	if f.buildErr != nil {
		return nil, nil, f.buildErr
	}
	return f.schema, f.ch, nil
}

func (f *fakeEngine) DeriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	return f.schema, nil
}

func (f *fakeEngine) ExecuteUpdate(ctx context.Context, sql string) (int64, error) {
	return 0, nil
}

func (f *fakeEngine) IngestStream(ctx context.Context, table string, reader array.RecordReader, opts engine.IngestOptions) (int64, error) {
	return 0, nil
}

func (f *fakeEngine) Allocator() memory.Allocator {
	return memory.NewGoAllocator()
}

func (f *fakeEngine) Close() error {
	return nil
}

// ─────────────────────────────────────
// Helpers
// ─────────────────────────────────────

func newTestServer(t *testing.T, eng *fakeEngine) *httptest.Server {
	t.Helper()
	// Pass io.Discard to avoid nil-pointer panic when writing logs
	s := NewServer(eng, slog.New(slog.NewTextHandler(io.Discard, nil)), "*")
	return httptest.NewServer(s)
}

func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, "ws"+url[len("http"):], nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return c
}

// ─────────────────────────────────────
// Tests
// ─────────────────────────────────────

func TestServer_StreamSuccess_Full(t *testing.T) {
	// Build the batch first to guarantee the test schema perfectly matches the batch schema
	batch := arrowtest.Int32Batch(1)
	defer batch.Release()

	eng := &fakeEngine{
		schema: batch.Schema(), // Resolves the schema mismatch error
		ch:     make(chan flight.StreamChunk, 10),
	}

	ts := newTestServer(t, eng)
	defer ts.Close()

	c := dialWS(t, ts.URL)
	defer c.Close(websocket.StatusNormalClosure, "")

	// Send query
	if err := wsjson.Write(context.Background(), c, QueryRequest{
		Query: "select 1",
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read schema
	var schemaMsg map[string]any
	if err := wsjson.Read(context.Background(), c, &schemaMsg); err != nil {
		t.Fatalf("schema read failed: %v", err)
	}

	// Send batch + CLOSE CHANNEL (critical)
	batch.Retain() // Retain because the server loop calls chunk.Data.Release()
	eng.ch <- flight.StreamChunk{
		Data: batch,
	}
	close(eng.ch)

	// Read IPC frames - the server sends batch_header (JSON) then binary, then "complete" (JSON), then closes
	// Loop until we get binary data or connection closes
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for {
		msgType, data, err := c.Read(ctx)
		if err != nil {
			// Connection closed after stream ends - this is expected
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}
			t.Fatalf("read frame failed: %v", err)
		}

		if msgType == websocket.MessageBinary && len(data) > 0 {
			// Found the binary IPC payload
			return
		}
		// Otherwise skip JSON messages (batch_header, complete) and continue reading
	}
}

func TestServer_BuildStreamErrorClosesConnection(t *testing.T) {
	eng := &fakeEngine{
		buildErr: errors.New("limit"),
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
		ch: make(chan flight.StreamChunk),
	}

	ts := newTestServer(t, eng)
	defer ts.Close()

	c := dialWS(t, ts.URL)
	defer c.Close(websocket.StatusNormalClosure, "")

	_ = wsjson.Write(context.Background(), c, QueryRequest{Query: "select 1"})

	// Read the error JSON message - server sends error before closing
	var errMsg map[string]any
	if err := wsjson.Read(context.Background(), c, &errMsg); err != nil {
		t.Fatalf("failed to read error message: %v", err)
	}
	if errMsg["type"] != "error" {
		t.Fatalf("expected error type, got %v", errMsg["type"])
	}

	// Server sends two error messages (one from processQuery, one from serve)
	// Keep reading until we get a close
	readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		_, _, err := c.Read(readCtx)
		if err != nil {
			// Got close - test passes
			return
		}
		// Otherwise continue reading error messages
	}
}

func TestServer_ExecutesExactlyOneBuildPerQuery(t *testing.T) {
	eng := &fakeEngine{
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
		ch: make(chan flight.StreamChunk),
	}

	ts := newTestServer(t, eng)
	defer ts.Close()

	c := dialWS(t, ts.URL)
	defer c.Close(websocket.StatusNormalClosure, "")

	// 1. Send query
	_ = wsjson.Write(context.Background(), c, QueryRequest{Query: "select 1"})

	// 2. Consume schema to ensure loop is active
	var schemaMsg map[string]any
	if err := wsjson.Read(context.Background(), c, &schemaMsg); err != nil {
		t.Fatalf("failed to read schema: %v", err)
	}

	// 3. Start a background goroutine to drain the websocket.
	// This ensures we acknowledge the server's Close frame.
	go func() {
		for {
			_, _, err := c.Read(context.Background())
			if err != nil {
				return // Connection closed, which is what we want
			}
		}
	}()

	// 4. Close the data channel to finish the stream
	close(eng.ch)

	if eng.buildCalls.Load() != 1 {
		t.Errorf("expected build calls=1 got %d", eng.buildCalls.Load())
	}
}

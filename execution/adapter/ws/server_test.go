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
	acquired   atomic.Int32
	released   atomic.Int32
	// Added: A signal channel to notify the test when release is DONE
	releaseNotify chan struct{}
}

func (f *fakeEngine) AcquireQuerySlot(ctx context.Context) error {
	if f.acquireErr != nil {
		return f.acquireErr
	}
	f.acquired.Add(1)
	return nil
}

func (f *fakeEngine) ReleaseQuerySlot() {
	f.released.Add(1)
	if f.releaseNotify != nil {
		select {
		case f.releaseNotify <- struct{}{}:
		default:
		}
	}
}

func (f *fakeEngine) BuildStream(ctx context.Context, sql string, params arrow.RecordBatch) (*arrow.Schema, <-chan engine.StreamChunk, error) {
	return f.schema, f.ch, nil
}

func (f *fakeEngine) DeriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	return f.schema, nil
}

func (f *fakeEngine) ExecuteUpdate(ctx context.Context, sql string) (int64, error) {
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

	// Read IPC frame with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msgType, data, err := c.Read(ctx)
	if err != nil {
		// With continuous streaming, server closes connection after stream ends
		// If we get a close frame after schema, that's expected
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		t.Fatalf("read frame failed: %v", err)
	}

	if msgType != websocket.MessageBinary {
		t.Fatalf("expected binary message, got %v", msgType)
	}

	if len(data) == 0 {
		t.Fatal("expected non-empty IPC payload")
	}
}

func TestServer_ConcurrentSlotRejection(t *testing.T) {
	eng := &fakeEngine{
		acquireErr: errors.New("limit"),
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

	_, _, err := c.Read(context.Background())
	if err == nil {
		t.Fatal("expected rejection close")
	}
}

func TestServer_EngineSlotLifecycle(t *testing.T) {
	notify := make(chan struct{}, 1)
	eng := &fakeEngine{
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
		ch:            make(chan flight.StreamChunk),
		releaseNotify: notify,
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

	// 5. Wait for the server to actually call ReleaseQuerySlot
	select {
	case <-notify:
		// Success: The defer has executed
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ReleaseQuerySlot to be called")
	}

	// 6. Final assertions
	if eng.acquired.Load() != 1 {
		t.Errorf("expected acquire=1 got %d", eng.acquired.Load())
	}
	if eng.released.Load() != 1 {
		t.Errorf("expected release=1 got %d", eng.released.Load())
	}
}

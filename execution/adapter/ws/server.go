package ws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/TFMV/porter/execution/engine"
	"github.com/TFMV/porter/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Engine = engine.Engine

// ── Wire protocol ─────────────────────────────────────────────────────────────

type QueryRequest struct {
	Query string `json:"query"`
}

type ControlMessage struct {
	Type       string   `json:"type"`
	StreamID   string   `json:"stream_id"`
	Fields     []string `json:"fields,omitempty"`
	BatchIndex int      `json:"batch_index,omitempty"`
	NumRows    int      `json:"num_rows,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// ── Tuning constants ──────────────────────────────────────────────────────────

const (
	// writeTimeout caps every individual WebSocket write so a stalled or dead
	// client cannot hold a goroutine forever.
	writeTimeout = 30 * time.Second

	// queryReadTimeout is how long the server waits for the client to send its
	// initial query before giving up.
	queryReadTimeout = 60 * time.Second

	// sendQueueDepth is the number of fully-serialised IPC batches that may be
	// buffered between the serialisation goroutine and the sender goroutine.
	// This provides bounded backpressure: when the network is slower than the
	// engine the serialiser stalls here rather than accumulating unbounded RAM.
	sendQueueDepth = 8

	// initialBufCap is the starting capacity of each recycled serialisation
	// buffer.  Buffers grow on demand and are returned to the pool after use.
	initialBufCap = 512 * 1024 // 512 KB
)

// ── Buffer pool ───────────────────────────────────────────────────────────────

// bufPool recycles serialisation buffers across batches to eliminate per-batch
// heap allocations under high row counts.
var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, initialBufCap))
	},
}

// ── Internal pipeline type ────────────────────────────────────────────────────

// sendItem carries one fully-serialised, self-contained Arrow IPC stream
// (schema message + one record-batch message + EOS marker) together with its
// control-plane metadata.
//
// Ownership: the serialisation loop allocates buf from bufPool and transfers
// ownership to the sender goroutine, which returns buf to bufPool after the
// WebSocket write completes (or is skipped due to a prior error).
type sendItem struct {
	streamID   string
	batchIndex int
	numRows    int
	numBytes   int
	buf        *bytes.Buffer
}

// ── Server ────────────────────────────────────────────────────────────────────

type Server struct {
	Engine         Engine
	Logger         *slog.Logger
	Telemetry      telemetry.Publisher
	OriginPatterns []string
}

func NewServer(engine Engine, logger *slog.Logger, publisher telemetry.Publisher, origins ...string) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	return &Server{
		Engine:         engine,
		Logger:         logger,
		Telemetry:      publisher,
		OriginPatterns: origins,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: s.OriginPatterns,
	})
	if err != nil {
		s.Logger.Error("websocket accept failed", slog.Any("err", err))
		return
	}
	h := &streamHandler{conn: conn, engine: s.Engine, logger: s.Logger, telemetry: s.Telemetry}
	h.serve(r.Context())
}

// ── streamHandler ─────────────────────────────────────────────────────────────

type streamHandler struct {
	conn      *websocket.Conn
	engine    Engine
	logger    *slog.Logger
	telemetry telemetry.Publisher
	closeOnce sync.Once
}

// serve handles exactly one query per WebSocket connection.
//
// Protocol (one connection = one query):
//
//	client → server   JSON  QueryRequest
//	server → client   JSON  ControlMessage{type:"schema"}
//	server → client   JSON  ControlMessage{type:"batch_header"} \
//	server → client   BIN   Arrow IPC stream (self-contained)   /  × N batches
//	server → client   JSON  ControlMessage{type:"complete"}
//	server → client   WS close frame (StatusNormalClosure)
//
// The client's read loop in the benchmark exits only when conn.Read returns
// an error whose CloseStatus == StatusNormalClosure (or context.Canceled).
// It never inspects the "complete" JSON message to decide it is done — it
// keeps reading until the server closes the connection.
//
// The previous implementation sent "complete" and then looped back into a
// 60-second read-wait.  The benchmark read "complete" as just another text
// frame and continued blocking on conn.Read.  The server was waiting for
// a new query.  Neither side would move.  Permanent hang.
//
// Fix: close with StatusNormalClosure immediately after "complete".
func (h *streamHandler) serve(ctx context.Context) {
	requestStarted := time.Now()
	var req QueryRequest
	readCtx, cancel := context.WithTimeout(ctx, queryReadTimeout)
	err := wsjson.Read(readCtx, h.conn, &req)
	cancel()

	if err != nil {
		h.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(requestStarted),
			Metadata: map[string]any{
				"path":          "websocket",
				"operation":     "query",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		if isDisconnect(err) {
			h.close(websocket.StatusNormalClosure, "")
		} else {
			h.close(websocket.StatusUnsupportedData, "bad request")
		}
		return
	}

	if req.Query == "" {
		h.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(requestStarted),
			Metadata: map[string]any{
				"path":          "websocket",
				"operation":     "query",
				"reason":        "empty query",
				"path_terminal": true,
			},
		})
		h.close(websocket.StatusUnsupportedData, "empty query")
		return
	}

	h.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "websocket",
			"operation": "query",
		},
	})

	if err := h.processQuery(ctx, req, requestStarted); err != nil {
		if isDisconnect(err) {
			h.close(websocket.StatusNormalClosure, "")
			return
		}
		h.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentEgress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(requestStarted),
			Metadata: map[string]any{
				"path":          "websocket",
				"operation":     "query",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		h.logger.Error("query failed", slog.Any("err", err))
		_ = h.writeControlWithTimeout(ctx, ControlMessage{
			Type:  "error",
			Error: "query processing failed",
		})
		h.close(websocket.StatusInternalError, "query failed")
		return
	}

	// Signal clean end-of-stream.  The client's conn.Read will return a
	// *websocket.CloseError with CloseStatus == StatusNormalClosure, which
	// breaks its read loop and records the operation as a success.
	h.close(websocket.StatusNormalClosure, "")
}

func (h *streamHandler) processQuery(ctx context.Context, req QueryRequest, requestStarted time.Time) error {
	streamID := fmt.Sprintf("req-%d", time.Now().UnixNano())

	execCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "websocket",
		"operation": "query",
	})
	execCtx, cancel := context.WithCancel(execCtx)
	defer cancel()

	schema, streamCh, err := h.engine.BuildStream(execCtx, req.Query, nil)
	if err != nil {
		_ = h.writeControlWithTimeout(ctx, ControlMessage{
			Type:     "error",
			StreamID: streamID,
			Error:    "execution failed",
		})
		return err
	}

	h.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Latency:   time.Since(requestStarted),
		Metadata: map[string]any{
			"path":      "websocket",
			"operation": "query",
		},
	})

	// Drain any unconsumed chunks so the engine can free its resources even
	// when we return early due to a send error.
	defer func() {
		for chunk := range streamCh {
			if chunk.Data != nil {
				chunk.Data.Release()
			}
		}
	}()

	if err := h.sendSchema(ctx, streamID, schema); err != nil {
		return err
	}

	return h.stream(ctx, streamID, schema, streamCh, requestStarted)
}

// stream is the core data pipeline.
//
// Architecture
// ────────────
//
// The original design routed Arrow IPC bytes through an io.Pipe into a
// 64 KB chunked WebSocket sender.  At 20 MM rows that caused three failures:
//
//  1. Deadlock.  conn.Write blocked (slow network) → pipe filled (no depth
//     limit) → IPC writer blocked → errCh full (cap 1) → nothing unblocked.
//
//  2. Broken IPC framing.  Arbitrary 64 KB pipe reads produced WebSocket
//     frames that sliced Arrow IPC messages at random byte offsets.  The
//     client's ipc.NewReader received unparseable fragments.
//
//  3. No write deadline.  A dead client held a goroutine indefinitely.
//
// Replacement
// ───────────
//
//	Serialisation loop (calling goroutine)
//	  • Pulls Arrow records from streamCh.
//	  • Encodes each as a *self-contained* Arrow IPC stream
//	    (schema + one record batch + EOS) into a pooled buffer.
//	  • Releases the Arrow record immediately after encoding (minimises RSS).
//	  • Pushes the buffer onto sendQ (bounded channel = backpressure).
//
//	Sender goroutine
//	  • Drains sendQ.
//	  • For each item: (a) emits a JSON batch_header, (b) sends the IPC
//	    bytes as a single binary WebSocket message.
//	  • One batch == one WebSocket message; message boundaries are preserved.
//	  • Every write is bounded by writeTimeout.
//	  • On error continues draining sendQ (returning bufs) so the serialiser
//	    never stalls on a full channel waiting for space that will not come.
//
//	Shutdown
//	  • Serialisation loop closes sendQ when it exits (normal or error).
//	  • Sender's range loop exits and reports its first error via senderDone.
//	  • No goroutines are leaked.
func (h *streamHandler) stream(
	ctx context.Context,
	streamID string,
	schema *arrow.Schema,
	streamCh <-chan engine.StreamChunk,
	requestStarted time.Time,
) error {
	sendQ := make(chan sendItem, sendQueueDepth)
	senderDone := make(chan error, 1)

	// ── Sender goroutine ──────────────────────────────────────────────────────
	go func() {
		var firstErr error
		for item := range sendQ {
			if firstErr == nil {
				firstErr = h.sendBatch(ctx, item)
			}
			// Always return the buffer even on error so the serialisation loop
			// can push new items and is never left blocking on a full sendQ.
			bufPool.Put(item.buf)
		}
		senderDone <- firstErr
	}()

	// ── Serialisation loop ────────────────────────────────────────────────────
	rows, bytes, serErr := h.serializeAndQueue(ctx, streamID, schema, streamCh, sendQ)

	// Closing sendQ signals the sender's range loop to exit.
	close(sendQ)

	// Wait for the sender to finish draining before we return and allow
	// serve() to close the connection while the sender is still writing.
	senderErr := <-senderDone

	if serErr != nil {
		return serErr
	}
	if senderErr != nil {
		return senderErr
	}

	h.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentTransport,
		Type:      telemetry.TypeQueueDepth,
		Metadata: map[string]any{
			"path":           "websocket",
			"operation":      "query",
			"queue_depth":    0,
			"queue_capacity": cap(sendQ),
			"queue_unit":     "batches",
		},
	})

	h.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentEgress,
		Type:      telemetry.TypeRequestEnd,
		Rows:      rows,
		Bytes:     bytes,
		Latency:   time.Since(requestStarted),
		Metadata: map[string]any{
			"path":          "websocket",
			"operation":     "query",
			"path_terminal": true,
		},
	})

	// All batches delivered; notify client before the connection is closed.
	return h.writeControlWithTimeout(ctx, ControlMessage{
		Type:     "complete",
		StreamID: streamID,
	})
}

// serializeAndQueue encodes each Arrow record batch as a self-contained IPC
// stream and pushes it onto sendQ for the sender goroutine.
func (h *streamHandler) serializeAndQueue(
	ctx context.Context,
	streamID string,
	schema *arrow.Schema,
	streamCh <-chan engine.StreamChunk,
	sendQ chan<- sendItem,
) (int64, int64, error) {
	batchIdx := 0
	var totalRows int64
	var totalBytes int64

	for chunk := range streamCh {
		if chunk.Err != nil {
			if chunk.Data != nil {
				chunk.Data.Release()
			}
			return totalRows, totalBytes, chunk.Err
		}
		if chunk.Data == nil {
			continue
		}

		rows := int(chunk.Data.NumRows())
		if rows == 0 {
			chunk.Data.Release()
			continue
		}

		// Encode into a pooled buffer.
		// Each buffer is a *complete* Arrow IPC stream so the client's
		// ipc.NewReader can decode each binary WebSocket message independently.
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		w := ipc.NewWriter(buf, ipc.WithSchema(schema))
		encErr := w.Write(chunk.Data)
		chunk.Data.Release() // free Arrow memory as soon as it is encoded
		closeErr := w.Close()

		if encErr != nil || closeErr != nil {
			bufPool.Put(buf)
			if encErr != nil {
				return totalRows, totalBytes, encErr
			}
			return totalRows, totalBytes, closeErr
		}

		byteCount := buf.Len()
		item := sendItem{
			streamID:   streamID,
			batchIndex: batchIdx,
			numRows:    rows,
			numBytes:   byteCount,
			buf:        buf,
		}

		// Bounded push with cancellation: if ctx is cancelled we drop the
		// buffer and return immediately rather than leaking it.
		select {
		case sendQ <- item:
			totalRows += int64(rows)
			totalBytes += int64(byteCount)
			h.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentTransport,
				Type:      telemetry.TypeBatch,
				Rows:      int64(rows),
				Bytes:     int64(byteCount),
				Metadata: map[string]any{
					"path":      "websocket",
					"operation": "query",
				},
			})
			h.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentTransport,
				Type:      telemetry.TypeQueueDepth,
				Rows:      int64(rows),
				Bytes:     int64(byteCount),
				Metadata: map[string]any{
					"path":           "websocket",
					"operation":      "query",
					"queue_depth":    len(sendQ),
					"queue_capacity": cap(sendQ),
					"queue_unit":     "batches",
				},
			})
			batchIdx++
		case <-ctx.Done():
			bufPool.Put(buf)
			h.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentTransport,
				Type:      telemetry.TypeStall,
				Rows:      totalRows,
				Bytes:     totalBytes,
				Metadata: map[string]any{
					"path":      "websocket",
					"operation": "query",
					"reason":    ctx.Err().Error(),
				},
			})
			return totalRows, totalBytes, ctx.Err()
		}
	}

	return totalRows, totalBytes, nil
}

// sendBatch sends one record batch over the wire:
//  1. A JSON batch_header control message.
//  2. The IPC payload as a single binary WebSocket message.
//
// One batch == one WebSocket message, so ipc.NewReader on the client side
// receives a complete, independently-decodable stream per message.
//
// buf ownership: the caller (sender goroutine) returns buf to bufPool after
// sendBatch returns; sendBatch must not touch buf after it returns.
func (h *streamHandler) sendBatch(ctx context.Context, item sendItem) error {
	if err := h.writeControlWithTimeout(ctx, ControlMessage{
		Type:       "batch_header",
		StreamID:   item.streamID,
		BatchIndex: item.batchIndex,
		NumRows:    item.numRows,
	}); err != nil {
		return err
	}

	wCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	started := time.Now()
	if err := h.conn.Write(wCtx, websocket.MessageBinary, item.buf.Bytes()); err != nil {
		return err
	}
	h.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentEgress,
		Type:      telemetry.TypeBatch,
		Rows:      int64(item.numRows),
		Bytes:     int64(item.numBytes),
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":          "websocket",
			"operation":     "query",
			"path_terminal": true,
		},
	})
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (h *streamHandler) sendSchema(ctx context.Context, streamID string, schema *arrow.Schema) error {
	fields := make([]string, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields[i] = f.Name
	}
	return h.writeControlWithTimeout(ctx, ControlMessage{
		Type:     "schema",
		StreamID: streamID,
		Fields:   fields,
	})
}

// writeControlWithTimeout sends a JSON control message with an explicit write
// deadline so a slow or dead client cannot block control-plane writes.
func (h *streamHandler) writeControlWithTimeout(ctx context.Context, msg ControlMessage) error {
	wCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return wsjson.Write(wCtx, h.conn, msg)
}

func (h *streamHandler) publish(ctx context.Context, evt telemetry.Event) {
	if h.telemetry == nil {
		return
	}
	h.telemetry.Publish(telemetry.ScopedEvent(ctx, evt))
}

func (h *streamHandler) close(code websocket.StatusCode, reason string) {
	h.closeOnce.Do(func() {
		if len(reason) > 125 {
			reason = reason[:122] + "..."
		}
		_ = h.conn.Close(code, reason)
	})
}

func isDisconnect(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, context.Canceled) ||
		websocket.CloseStatus(err) >= 1000
}

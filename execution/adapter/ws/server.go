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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Engine = engine.Engine

type QueryRequest struct {
	Query string `json:"query"`
}

type Server struct {
	Engine         Engine
	Logger         *slog.Logger
	OriginPatterns []string
}

func NewServer(engine Engine, logger *slog.Logger, origins ...string) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	return &Server{
		Engine:         engine,
		Logger:         logger,
		OriginPatterns: origins,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  s.OriginPatterns,
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		s.Logger.Error("websocket upgrade failed", slog.Any("err", err))
		return
	}

	handler := &streamHandler{
		conn:   conn,
		engine: s.Engine,
		logger: s.Logger,
	}
	handler.serve(r.Context())
}

type streamHandler struct {
	conn      *websocket.Conn
	engine    Engine
	logger    *slog.Logger
	closeOnce sync.Once
}

func (h *streamHandler) serve(ctx context.Context) {
	// 1. Read JSON request
	var req QueryRequest
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := wsjson.Read(readCtx, h.conn, &req)
	cancel()
	if err != nil {
		h.close(websocket.StatusUnsupportedData, "invalid request")
		return
	}
	if req.Query == "" {
		h.close(websocket.StatusUnsupportedData, "empty query")
		return
	}

	// 2. Execution context + concurrency slot
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	if err := h.engine.AcquireQuerySlot(execCtx); err != nil {
		h.close(websocket.StatusPolicyViolation, "too many concurrent queries")
		return
	}
	defer h.engine.ReleaseQuerySlot()

	// 3. Build Arrow stream
	schema, streamCh, err := h.engine.BuildStream(execCtx, req.Query, nil)
	if err != nil {
		h.logger.Error("build stream failed", slog.Any("err", err))
		h.close(websocket.StatusInternalError, "execution failed")
		return
	}

	// 4. Send schema once
	if err := h.sendSchema(execCtx, schema); err != nil {
		return
	}

	// 5. Stream batches
	for {
		select {
		case <-execCtx.Done():
			h.close(websocket.StatusNormalClosure, "cancelled")
			return

		case chunk, ok := <-streamCh:
			if !ok {
				h.close(websocket.StatusNormalClosure, "done")
				return
			}
			if chunk.Err != nil {
				h.logger.Error("stream error", slog.Any("err", chunk.Err))
				h.close(websocket.StatusInternalError, "stream error")
				return
			}
			if chunk.Data == nil {
				continue
			}

			frame, err := encodeArrowBatch(schema, chunk.Data)
			chunk.Data.Release()

			if err != nil {
				h.logger.Error("ipc encode failed", slog.Any("err", err))
				h.close(websocket.StatusInternalError, "encoding error")
				return
			}

			writeCtx, cancelWrite := context.WithTimeout(execCtx, 5*time.Second)
			err = h.conn.Write(writeCtx, websocket.MessageBinary, frame)
			cancelWrite()

			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					h.close(websocket.StatusNormalClosure, "client disconnected")
				} else {
					h.logger.Error("write failed", slog.Any("err", err))
					h.close(websocket.StatusAbnormalClosure, "write failure")
				}
				return
			}
		}
	}
}

func (h *streamHandler) sendSchema(ctx context.Context, schema *arrow.Schema) error {
	meta := map[string]any{
		"type": "schema",
		"fields": func() []string {
			out := make([]string, len(schema.Fields()))
			for i, f := range schema.Fields() {
				out[i] = f.Name
			}
			return out
		}(),
	}
	if err := wsjson.Write(ctx, h.conn, meta); err != nil {
		h.logger.Error("schema send failed", slog.Any("err", err))
		h.close(websocket.StatusInternalError, "schema send failed")
		return err
	}
	return nil
}

func encodeArrowBatch(schema *arrow.Schema, batch arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))

	if err := w.Write(batch); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("ipc write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("ipc close: %w", err)
	}
	return buf.Bytes(), nil
}

func (h *streamHandler) close(code websocket.StatusCode, reason string) {
	h.safeClose(code, reason)
}

func (h *streamHandler) safeClose(code websocket.StatusCode, reason string) {
	h.closeOnce.Do(func() {
		// We truncate the reason to 125 bytes as per RFC 6455
		if len(reason) > 125 {
			reason = reason[:122] + "..."
		}

		// Close the connection. Note: coder/websocket's Close
		// is generally safe, but we log errors for visibility.
		err := h.conn.Close(code, reason)
		if err != nil && websocket.CloseStatus(err) != code {
			// Only log if it's not a normal/expected closure error
			h.logger.Debug("websocket close info", slog.Any("err", err))
		}
	})
}

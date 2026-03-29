package ws

import (
	"bytes"
	"context"
	"errors"
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
	BatchSize      int
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
		BatchSize:      100,
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
		conn:      conn,
		engine:    s.Engine,
		logger:    s.Logger,
		batchSize: s.BatchSize,
	}
	handler.serve(r.Context())
}

type streamHandler struct {
	conn      *websocket.Conn
	engine    Engine
	logger    *slog.Logger
	closeOnce sync.Once
	batchSize int
}

func (h *streamHandler) serve(ctx context.Context) {
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

	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	if err := h.engine.AcquireQuerySlot(execCtx); err != nil {
		h.close(websocket.StatusPolicyViolation, "too many concurrent queries")
		return
	}
	defer h.engine.ReleaseQuerySlot()

	schema, streamCh, err := h.engine.BuildStream(execCtx, req.Query, nil)
	if err != nil {
		h.logger.Error("build stream failed", slog.Any("err", err))
		h.close(websocket.StatusInternalError, "execution failed")
		return
	}

	if err := h.sendSchema(execCtx, schema); err != nil {
		return
	}

	h.streamBatches(execCtx, schema, streamCh)
}

func (h *streamHandler) streamBatches(ctx context.Context, schema *arrow.Schema, streamCh <-chan engine.StreamChunk) {
	var pending []arrow.RecordBatch

	sendBatch := func(batches []arrow.RecordBatch) error {
		if len(batches) == 0 {
			return nil
		}

		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(schema))

		for _, batch := range batches {
			if err := w.Write(batch); err != nil {
				w.Close()
				return err
			}
			batch.Release()
		}

		if err := w.Close(); err != nil {
			return err
		}

		return h.conn.Write(ctx, websocket.MessageBinary, buf.Bytes())
	}

	for {
		select {
		case <-ctx.Done():
			for _, b := range pending {
				b.Release()
			}
			h.close(websocket.StatusNormalClosure, "cancelled")
			return

		case chunk, ok := <-streamCh:
			if !ok {
				if err := sendBatch(pending); err != nil {
					h.handleWriteError(err)
				}
				h.close(websocket.StatusNormalClosure, "done")
				return
			}
			if chunk.Err != nil {
				for _, b := range pending {
					b.Release()
				}
				h.logger.Error("stream error", slog.Any("err", chunk.Err))
				h.close(websocket.StatusInternalError, "stream error")
				return
			}
			if chunk.Data == nil {
				continue
			}

			pending = append(pending, chunk.Data)

			if len(pending) >= h.batchSize {
				if err := sendBatch(pending); err != nil {
					h.handleWriteError(err)
					return
				}
				pending = nil
			}
		}
	}
}

func (h *streamHandler) handleWriteError(err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		h.close(websocket.StatusNormalClosure, "client disconnected")
	} else {
		h.logger.Error("write failed", slog.Any("err", err))
		h.close(websocket.StatusAbnormalClosure, "write failure")
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

func (h *streamHandler) close(code websocket.StatusCode, reason string) {
	h.safeClose(code, reason)
}

func (h *streamHandler) safeClose(code websocket.StatusCode, reason string) {
	h.closeOnce.Do(func() {
		if len(reason) > 125 {
			reason = reason[:122] + "..."
		}

		err := h.conn.Close(code, reason)
		if err != nil && websocket.CloseStatus(err) != code {
			h.logger.Debug("websocket close info", slog.Any("err", err))
		}
	})
}

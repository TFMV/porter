package ws

import (
	"bufio"
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
		OriginPatterns: s.OriginPatterns,
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
	for {
		var req QueryRequest
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := wsjson.Read(readCtx, h.conn, &req)
		cancel()
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				h.close(websocket.StatusNormalClosure, "client disconnected")
			} else {
				h.close(websocket.StatusUnsupportedData, "invalid request")
			}
			return
		}
		if req.Query == "" {
			h.close(websocket.StatusUnsupportedData, "empty query")
			return
		}

		execCtx, cancelExec := context.WithCancel(ctx)

		schema, streamCh, err := h.engine.BuildStream(execCtx, req.Query, nil)
		if err != nil {
			cancelExec()
			h.logger.Error("build stream failed", slog.Any("err", err))
			h.close(websocket.StatusInternalError, "execution failed")
			return
		}

		if err := h.sendSchema(execCtx, schema); err != nil {
			cancelExec()
			return
		}

		if err := h.streamBatches(execCtx, schema, streamCh); err != nil {
			cancelExec()
			return
		}

		cancelExec()
		h.close(websocket.StatusNormalClosure, "query complete")
	}
}

func (h *streamHandler) streamBatches(ctx context.Context, schema *arrow.Schema, streamCh <-chan engine.StreamChunk) error {
	wsWriter, err := h.conn.Writer(ctx, websocket.MessageBinary)
	if err != nil {
		h.handleWriteError(err)
		return err
	}
	buffered := bufio.NewWriterSize(wsWriter, 1<<20)

	ipcWriter := ipc.NewWriter(buffered, ipc.WithSchema(schema))
	pendingBatches := 0

	closeWriters := func() error {
		if err := ipcWriter.Close(); err != nil {
			return err
		}
		if err := buffered.Flush(); err != nil {
			return err
		}
		return wsWriter.Close()
	}

	for {
		select {
		case <-ctx.Done():
			_ = closeWriters()
			h.close(websocket.StatusNormalClosure, "cancelled")
			return ctx.Err()

		case chunk, ok := <-streamCh:
			if !ok {
				if err := closeWriters(); err != nil {
					h.handleWriteError(err)
					return err
				}
				return nil
			}
			if chunk.Err != nil {
				_ = closeWriters()
				h.logger.Error("stream error", slog.Any("err", chunk.Err))
				h.close(websocket.StatusInternalError, "stream error")
				return chunk.Err
			}
			if chunk.Data == nil {
				continue
			}

			if err := ipcWriter.Write(chunk.Data); err != nil {
				chunk.Data.Release()
				_ = closeWriters()
				h.close(websocket.StatusInternalError, "write error")
				return err
			}
			pendingBatches++
			if pendingBatches >= 8 {
				if err := buffered.Flush(); err != nil {
					_ = closeWriters()
					h.handleWriteError(err)
					return err
				}
				pendingBatches = 0
			}
			chunk.Data.Release()
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

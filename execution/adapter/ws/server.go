// Package ws provides a native WebSocket transport for Arrow Flight SQL streams.
package ws

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Engine defines the contract with the core database execution layer.
type Engine interface {
	BuildStream(ctx context.Context, sql string, params arrow.RecordBatch) (*arrow.Schema, <-chan flight.StreamChunk, error)
}

// QueryRequest defines the initial JSON payload sent by the client upon connecting.
type QueryRequest struct {
	Query string `json:"query"`
	// Cursor string `json:"cursor,omitempty"` // For reconnect/resume support in the future
}

// Server acts as the WebSocket transport adapter.
type Server struct {
	Engine Engine
	Logger *slog.Logger
}

func NewServer(engine Engine, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		Engine: engine,
		Logger: logger,
	}
}

// ServeHTTP upgrades the connection to a WebSocket, reads the execution request,
// and streams the Arrow IPC format directly over the socket.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Upgrade the HTTP connection to WebSocket.
	// CompressionContextTakeover provides massive network savings for Arrow data
	// if native IPC compression (zstd/lz4) is not enabled.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"}, // Adjust for production CORS
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		s.Logger.Error("websocket upgrade failed", slog.Any("err", err))
		return
	}
	// Ensure the connection is always closed if we panic or exit early.
	defer c.Close(websocket.StatusInternalError, "server closed unexpectedly")

	// 2. Read the configuration/query payload (JSON-first framing)
	var req QueryRequest
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	err = wsjson.Read(ctx, c, &req)
	cancel()
	if err != nil {
		s.Logger.Warn("failed to read query request", slog.Any("err", err))
		c.Close(websocket.StatusUnsupportedData, "invalid or missing query payload")
		return
	}

	if req.Query == "" {
		c.Close(websocket.StatusUnsupportedData, "empty query")
		return
	}

	s.Logger.Debug("received websocket query", slog.String("query", req.Query))

	// 3. Bind execution to the WebSocket lifecycle (Cancellation Propagation)
	// r.Context() is tied to the underlying TCP connection. If the client drops,
	// this context cancels, which propagates down to BuildStream -> pumpRecordsSafely.
	execCtx, execCancel := context.WithCancel(r.Context())
	defer execCancel()

	schema, streamCh, err := s.Engine.BuildStream(execCtx, req.Query, nil)
	if err != nil {
		s.Logger.Error("query execution failed", slog.Any("err", err), slog.String("query", req.Query))
		c.Close(websocket.StatusInternalError, truncateReason("execution failed: "+err.Error()))
		return
	}

	// 4. Transform the message-based WebSocket into a continuous byte stream.
	// This writes binary WS frames, perfectly matching the Arrow IPC stream spec.
	netConn := websocket.NetConn(execCtx, c, websocket.MessageBinary)
	defer netConn.Close()

	// 5. Schema-First Streaming
	// ipc.NewWriter immediately writes the Arrow Schema payload to the netConn.
	ipcWriter := ipc.NewWriter(netConn, ipc.WithSchema(schema))
	defer ipcWriter.Close()

	// 6. Backpressure-Aware Pumping
	for chunk := range streamCh {
		if chunk.Err != nil {
			s.Logger.Error("stream encountered error", slog.Any("err", chunk.Err))
			c.Close(websocket.StatusInternalError, truncateReason("stream error: "+chunk.Err.Error()))
			return
		}

		if chunk.Data == nil {
			continue // defensive check
		}

		// Write pushes bytes to the socket. If the client reads slowly, the TCP
		// buffer fills -> Write blocks -> this loop blocks -> streamCh backs up ->
		// your pumpRecordsSafely blocks -> DuckDB stops scanning memory.
		err := ipcWriter.Write(chunk.Data)
		chunk.Data.Release() // Always release the batch after writing

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				s.Logger.Debug("client disconnected during stream")
				c.Close(websocket.StatusNormalClosure, "client disconnected")
			} else {
				s.Logger.Error("failed to write IPC batch", slog.Any("err", err))
				c.Close(websocket.StatusAbnormalClosure, "transport write error")
			}
			return
		}
	}

	// Clean shutdown: flush IPC writer, close stream, and close WS normally.
	if err := ipcWriter.Close(); err != nil {
		s.Logger.Warn("failed to close ipc writer", slog.Any("err", err))
	}
	if err := netConn.Close(); err != nil {
		s.Logger.Warn("failed to close net conn", slog.Any("err", err))
	}

	c.Close(websocket.StatusNormalClosure, "EOF")
}

// truncateReason ensures the websocket close reason respects the 125-byte limit.
func truncateReason(reason string) string {
	if len(reason) > 125 {
		return reason[:122] + "..."
	}
	return reason
}

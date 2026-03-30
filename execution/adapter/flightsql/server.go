// Package server provides a production-grade DuckDB-backed Arrow Flight SQL server.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/TFMV/porter/execution/engine"
	"github.com/TFMV/porter/telemetry"
	adbc_driver "github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	handleSize                = 16
	defaultHandleTTL          = 30 * time.Minute
	defaultGCInterval         = 5 * time.Minute
	defaultSchemaProbeTimeout = 5 * time.Second
	maxUncommittedBytesOption = "porter.ingest.max_uncommitted_bytes"
)

// ─── ERROR TAXONOMY ───────────────────────────────────────────────────────────

var (
	ErrEmptyQuery = status.Error(codes.InvalidArgument, "empty query provided")
	ErrNotFound   = status.Error(codes.NotFound, "statement handle not found or expired")
	ErrInternal   = status.Error(codes.Internal, "internal server error")
	ErrCancelled  = status.Error(codes.Canceled, "execution cancelled by client")
	ErrInvalidSQL = status.Error(codes.InvalidArgument, "invalid SQL statement")
)

type Engine = engine.Engine

func wrapInternal(err error, msg string) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

// ─── LIFECYCLE & OWNERSHIP ────────────────────────────────────────────────────

// BatchGuard is an alias for the engine's BatchGuard for backward compatibility.
type BatchGuard = engine.BatchGuard

// NewBatchGuard is an alias for the engine's NewBatchGuard for backward compatibility.
var NewBatchGuard = engine.NewBatchGuard

// ─── HANDLE TYPES ─────────────────────────────────────────────────────────────

type queryHandle struct {
	sql     string
	expires time.Time
}

func (h queryHandle) isExpired() bool { return time.Now().After(h.expires) }

type preparedEntry struct {
	sql        string
	paramGuard *BatchGuard // Deterministic ownership of bound parameters
	expires    time.Time
}

func (e preparedEntry) isExpired() bool { return time.Now().After(e.expires) }

// ─── SERVER DEFINITION ────────────────────────────────────────────────────────

type Server struct {
	fsql.BaseServer

	Engine        Engine
	handleTTL     time.Duration
	Alloc         memory.Allocator
	Logger        *slog.Logger
	gcInterval    time.Duration
	queryHandles  map[string]queryHandle
	preparedStmts map[string]preparedEntry
	handlesMu     sync.RWMutex

	stopGC     chan struct{}
	stopGCOnce sync.Once
	Telemetry  telemetry.Publisher
}

type Config struct {
	DBPath               string
	HandleTTL            time.Duration
	GCInterval           time.Duration
	SchemaProbeTimeout   time.Duration
	MaxConcurrentQueries int
	ReadOnly             bool
	Logger               *slog.Logger
	DevMode              bool
	Engine               Engine
	Telemetry            telemetry.Publisher
}

func NewServer(cfg Config) (*Server, error) {
	var eng Engine
	if cfg.Engine != nil {
		eng = cfg.Engine
	} else {
		var err error
		eng, err = engine.New(engine.Config{
			DBPath:               cfg.DBPath,
			MaxConcurrentQueries: cfg.MaxConcurrentQueries,
			SchemaProbeTimeout:   cfg.SchemaProbeTimeout,
			ReadOnly:             cfg.ReadOnly,
			Logger:               cfg.Logger,
			DevMode:              cfg.DevMode,
		})
		if err != nil {
			return nil, err
		}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	handleTTL := cfg.HandleTTL
	if handleTTL == 0 {
		handleTTL = defaultHandleTTL
	}

	gcInterval := cfg.GCInterval
	if gcInterval == 0 {
		gcInterval = defaultGCInterval
	}

	srv := &Server{
		Engine:        eng,
		handleTTL:     handleTTL,
		gcInterval:    gcInterval,
		Alloc:         eng.Allocator(),
		Logger:        logger,
		queryHandles:  make(map[string]queryHandle),
		preparedStmts: make(map[string]preparedEntry),
		stopGC:        make(chan struct{}),
		Telemetry:     cfg.Telemetry,
	}

	go srv.runGC()
	return srv, nil
}

// ─── LIFECYCLE MANAGEMENT ─────────────────────────────────────────────────────

func (s *Server) Close() error {
	return s.Shutdown(context.Background())
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stopGCOnce.Do(func() {
		close(s.stopGC)
	})

	s.handlesMu.Lock()
	for _, e := range s.preparedStmts {
		if e.paramGuard != nil {
			e.paramGuard.Release()
		}
	}
	s.preparedStmts = nil
	s.handlesMu.Unlock()

	if s.Engine != nil {
		return s.Engine.Close()
	}
	return nil
}

// ─── GC & EVICTION ────────────────────────────────────────────────────────────

func (s *Server) runGC() {
	t := time.NewTicker(s.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.evict()
		case <-s.stopGC:
			return
		}
	}
}

func (s *Server) evict() {
	s.handlesMu.Lock()
	for k, h := range s.queryHandles {
		if h.isExpired() {
			delete(s.queryHandles, k)
		}
	}

	var guardsToRelease []*BatchGuard
	for k, e := range s.preparedStmts {
		if e.isExpired() {
			if e.paramGuard != nil {
				guardsToRelease = append(guardsToRelease, e.paramGuard)
			}
			delete(s.preparedStmts, k)
		}
	}

	s.handlesMu.Unlock()

	// Release outside the lock to prevent blocking active queries
	for _, g := range guardsToRelease {
		g.Release()
	}
}

func (s *Server) acquireQuerySlot(ctx context.Context) error {
	return s.Engine.AcquireQuerySlot(ctx)
}

func (s *Server) releaseQuerySlot() {
	s.Engine.ReleaseQuerySlot()
}

// ─── UTILS ────────────────────────────────────────────────────────────────────

func newHandle() (string, error) {
	b := make([]byte, handleSize)
	if _, err := rand.Read(b); err != nil {
		return "", wrapInternal(err, "failed to generate handle")
	}
	return hex.EncodeToString(b), nil
}

func preparedStatementTicket(handle []byte) ([]byte, error) {
	cmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: handle}
	a, err := anypb.New(cmd)
	if err != nil {
		return nil, wrapInternal(err, "failed to marshal prepared statement ticket")
	}
	return proto.Marshal(a)
}

func (s *Server) deriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	return s.Engine.DeriveSchema(ctx, sql)
}

func (s *Server) publish(ctx context.Context, evt telemetry.Event) {
	if s.Telemetry == nil {
		return
	}
	s.Telemetry.Publish(telemetry.ScopedEvent(ctx, evt))
}

// ─── BACKPRESSURE-AWARE STREAMING ─────────────────────────────────────────────

func (s *Server) buildStream(
	ctx context.Context,
	sql string,
	params arrow.RecordBatch,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.Engine.BuildStream(ctx, sql, params)
}

func (s *Server) instrumentStream(
	ctx context.Context,
	streamCh <-chan flight.StreamChunk,
	path string,
	operation string,
	started time.Time,
) <-chan flight.StreamChunk {
	out := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(out)
		var totalRows int64
		var totalBytes int64

		for chunk := range streamCh {
			if chunk.Err != nil {
				s.publish(ctx, telemetry.Event{
					Component: telemetry.ComponentEgress,
					Type:      telemetry.TypeError,
					Rows:      totalRows,
					Bytes:     totalBytes,
					Latency:   time.Since(started),
					Metadata: map[string]any{
						"path":          path,
						"operation":     operation,
						"reason":        chunk.Err.Error(),
						"path_terminal": true,
					},
				})
				out <- chunk
				return
			}

			if chunk.Data != nil {
				rows := chunk.Data.NumRows()
				bytes := estimateRecordBatchBytes(chunk.Data)
				totalRows += rows
				totalBytes += bytes
				s.publish(ctx, telemetry.Event{
					Component: telemetry.ComponentEgress,
					Type:      telemetry.TypeBatch,
					Rows:      rows,
					Bytes:     bytes,
					Metadata: map[string]any{
						"path":          path,
						"operation":     operation,
						"path_terminal": true,
					},
				})
			}

			select {
			case out <- chunk:
			case <-ctx.Done():
				if chunk.Data != nil {
					chunk.Data.Release()
				}
				s.publish(ctx, telemetry.Event{
					Component: telemetry.ComponentEgress,
					Type:      telemetry.TypeStall,
					Rows:      totalRows,
					Bytes:     totalBytes,
					Latency:   time.Since(started),
					Metadata: map[string]any{
						"path":          path,
						"operation":     operation,
						"reason":        ctx.Err().Error(),
						"path_terminal": true,
					},
				})
				return
			}
		}

		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentEgress,
			Type:      telemetry.TypeRequestEnd,
			Rows:      totalRows,
			Bytes:     totalBytes,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          path,
				"operation":     operation,
				"path_terminal": true,
			},
		})
	}()
	return out
}

// ingestStreamReader adapts the Flight DoPut stream to the engine's Arrow
// RecordReader contract without taking ownership of the underlying stream.
//
// Ownership rules:
//   - The caller owns the incoming Flight MessageReader and is responsible for
//     releasing it.
//   - The adapter never buffers the full dataset; it forwards RecordBatches as
//     the engine pulls them.
//   - The engine may retain individual batches during ingestion, but the
//     adapter itself does not retain or release them.
type ingestStreamReader struct {
	src         flight.MessageReader
	firstSchema *arrow.Schema
	err         error
	seenRows    int64
	seenBytes   int64
}

func newIngestStreamReader(src flight.MessageReader) (*ingestStreamReader, error) {
	if src == nil {
		return nil, status.Error(codes.InvalidArgument, "ingest stream reader is required")
	}

	return &ingestStreamReader{
		src:         src,
		firstSchema: src.Schema(),
	}, nil
}

func (r *ingestStreamReader) Retain() {}

func (r *ingestStreamReader) Release() {}

func (r *ingestStreamReader) Schema() *arrow.Schema {
	if r.firstSchema != nil {
		return r.firstSchema
	}
	return r.src.Schema()
}

func (r *ingestStreamReader) Next() bool {
	if !r.src.Next() {
		return false
	}

	rec := r.src.RecordBatch()
	if rec == nil {
		return true
	}

	r.seenRows += rec.NumRows()
	r.seenBytes += estimateRecordBatchBytes(rec)

	if r.firstSchema == nil {
		r.firstSchema = rec.Schema()
		return true
	}

	if !rec.Schema().Equal(r.firstSchema) {
		r.err = status.Error(codes.InvalidArgument, "ingest stream schema changed mid-stream")
		return false
	}

	return true
}

func (r *ingestStreamReader) RecordBatch() arrow.RecordBatch {
	if r.err != nil {
		return nil
	}
	return r.src.RecordBatch()
}

func (r *ingestStreamReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *ingestStreamReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.src.Err()
}

func ingestModeForCommand(cmd fsql.StatementIngest) string {
	opts := cmd.GetTableDefinitionOptions()
	if opts == nil {
		return adbc_driver.OptionValueIngestModeAppend
	}

	switch opts.IfExists {
	case fsql.TableDefinitionOptionsTableExistsOptionReplace:
		return adbc_driver.OptionValueIngestModeReplace
	case fsql.TableDefinitionOptionsTableExistsOptionAppend:
		if opts.IfNotExist == fsql.TableDefinitionOptionsTableNotExistOptionCreate {
			return adbc_driver.OptionValueIngestModeCreateAppend
		}
		return adbc_driver.OptionValueIngestModeAppend
	case fsql.TableDefinitionOptionsTableExistsOptionFail:
		if opts.IfNotExist == fsql.TableDefinitionOptionsTableNotExistOptionCreate {
			return adbc_driver.OptionValueIngestModeCreate
		}
	}

	if opts.IfNotExist == fsql.TableDefinitionOptionsTableNotExistOptionCreate {
		return adbc_driver.OptionValueIngestModeCreateAppend
	}

	return adbc_driver.OptionValueIngestModeAppend
}

func buildIngestOptions(cmd fsql.StatementIngest) (engine.IngestOptions, error) {
	extra := make(map[string]string, len(cmd.GetOptions()))
	for key, val := range cmd.GetOptions() {
		extra[key] = val
	}

	var maxBytes int64
	if raw, ok := extra[maxUncommittedBytesOption]; ok && raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return engine.IngestOptions{}, status.Errorf(codes.InvalidArgument, "%s must be a valid int64", maxUncommittedBytesOption)
		}
		if parsed <= 0 {
			return engine.IngestOptions{}, status.Errorf(codes.InvalidArgument, "%s must be greater than zero", maxUncommittedBytesOption)
		}
		maxBytes = parsed
		delete(extra, maxUncommittedBytesOption)
	}

	return engine.IngestOptions{
		Catalog:             cmd.GetCatalog(),
		DBSchema:            cmd.GetSchema(),
		Temporary:           cmd.GetTemporary(),
		TransactionID:       append([]byte(nil), cmd.GetTransactionId()...),
		IngestMode:          ingestModeForCommand(cmd),
		ExtraOptions:        extra,
		MaxUncommittedBytes: maxBytes,
	}, nil
}

// ─── FLIGHTSQL STATEMENT ROUTING ──────────────────────────────────────────────

func (s *Server) GetFlightInfoStatement(
	ctx context.Context,
	cmd fsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return nil, ErrEmptyQuery
	}

	schema, err := s.deriveSchema(ctx, sql)
	if err != nil {
		return nil, err
	}

	handle, err := newHandle()
	if err != nil {
		return nil, err
	}

	s.handlesMu.Lock()
	s.queryHandles[handle] = queryHandle{sql: sql, expires: time.Now().Add(s.handleTTL)}
	s.handlesMu.Unlock()

	ticketBytes, err := fsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, wrapInternal(err, "create ticket")
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticketBytes}}},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *Server) GetSchemaStatement(
	ctx context.Context,
	cmd fsql.StatementQuery,
	_ *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return nil, ErrEmptyQuery
	}
	schema, err := s.deriveSchema(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.Alloc)}, nil
}

func (s *Server) DoGetStatement(
	ctx context.Context,
	ticket fsql.StatementQueryTicket,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	started := time.Now()
	handle := string(ticket.GetStatementHandle())

	s.handlesMu.Lock()
	h, ok := s.queryHandles[handle]
	if ok {
		delete(s.queryHandles, handle)
	}
	s.handlesMu.Unlock()

	if !ok || h.isExpired() {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "query",
				"reason":        "statement handle not found",
				"path_terminal": true,
			},
		})
		return nil, nil, ErrNotFound
	}

	scopedCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "flightsql",
		"operation": "query",
	})
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "query",
		},
	})
	schema, streamCh, err := s.buildStream(scopedCtx, h.sql, nil)
	if err != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "query",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return nil, nil, err
	}
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "query",
		},
	})
	return schema, s.instrumentStream(scopedCtx, streamCh, "flightsql", "query", started), nil
}

// ─── PREPARED STATEMENTS ──────────────────────────────────────────────────────

func (s *Server) CreatePreparedStatement(
	ctx context.Context,
	req fsql.ActionCreatePreparedStatementRequest,
) (fsql.ActionCreatePreparedStatementResult, error) {
	sql := req.GetQuery()
	if sql == "" {
		return fsql.ActionCreatePreparedStatementResult{}, ErrEmptyQuery
	}

	handle, err := newHandle()
	if err != nil {
		return fsql.ActionCreatePreparedStatementResult{}, err
	}

	s.handlesMu.Lock()
	s.preparedStmts[handle] = preparedEntry{
		sql:     sql,
		expires: time.Now().Add(s.handleTTL),
	}
	s.handlesMu.Unlock()

	return fsql.ActionCreatePreparedStatementResult{Handle: []byte(handle)}, nil
}

func (s *Server) ClosePreparedStatement(
	_ context.Context,
	req fsql.ActionClosePreparedStatementRequest,
) error {
	handle := string(req.GetPreparedStatementHandle())

	s.handlesMu.Lock()
	e, ok := s.preparedStmts[handle]
	if ok {
		delete(s.preparedStmts, handle)
	}
	s.handlesMu.Unlock()

	if ok && e.paramGuard != nil {
		e.paramGuard.Release()
	}
	return nil
}

func (s *Server) GetFlightInfoPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	handle := string(cmd.GetPreparedStatementHandle())

	s.handlesMu.RLock()
	e, ok := s.preparedStmts[handle]
	s.handlesMu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}

	s.handlesMu.Lock()
	e, ok = s.preparedStmts[handle]
	if ok {
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
	}
	s.handlesMu.Unlock()

	if !ok {
		return nil, ErrNotFound
	}

	ticketBytes, err := preparedStatementTicket([]byte(handle))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticketBytes}}},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *Server) GetSchemaPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
	_ *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	key := cmd.GetPreparedStatementHandle()

	s.handlesMu.RLock()
	e, ok := s.preparedStmts[string(key)]
	s.handlesMu.RUnlock()

	if !ok {
		return nil, ErrNotFound
	}

	schema, err := s.deriveSchema(ctx, e.sql)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.Alloc)}, nil
}

func (s *Server) DoPutPreparedStatementQuery(
	_ context.Context,
	cmd fsql.PreparedStatementQuery,
	reader flight.MessageReader,
	_ flight.MetadataWriter,
) ([]byte, error) {
	handleBytes := cmd.GetPreparedStatementHandle()
	handle := string(handleBytes)

	var rec arrow.RecordBatch
	if reader.Next() {
		rec = reader.RecordBatch()
		// Explicitly handle nil batches; Arrow payloads may sometimes be absent.
		if rec == nil {
			if s.Logger != nil {
				s.Logger.Debug("received nil RecordBatch in DoPutPreparedStatementQuery", slog.String("handle", handle))
			}
		}
	}

	s.handlesMu.Lock()
	e, ok := s.preparedStmts[handle]
	var oldGuard *BatchGuard
	if ok {
		oldGuard = e.paramGuard
		if rec != nil {
			// Only retain the batch when we know the prepared statement exists.
			rec.Retain()
			e.paramGuard = NewBatchGuard(rec) // Transfers ownership to the guard
		}
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
	}
	s.handlesMu.Unlock()

	if !ok {
		if rec != nil {
			rec.Release()
		}
		if s.Logger != nil {
			s.Logger.Debug("DoPutPreparedStatementQuery missing prepared statement handle", slog.String("handle", handle))
		}
		return nil, ErrNotFound
	}

	if oldGuard != nil {
		oldGuard.Release()
	}
	return cmd.GetPreparedStatementHandle(), nil
}

func (s *Server) DoGetPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	started := time.Now()
	handle := string(cmd.GetPreparedStatementHandle())

	s.handlesMu.Lock()
	e, ok := s.preparedStmts[handle]
	var boundParams arrow.RecordBatch
	if ok {
		if e.paramGuard != nil {
			boundParams = e.paramGuard.Retain() // buildStream takes ownership of this Retain
		}
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
		if s.Logger != nil {
			s.Logger.Debug("prepared statement fetched", slog.String("handle", handle), slog.Bool("hasParams", boundParams != nil))
		}
	}
	s.handlesMu.Unlock()

	if !ok {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "prepared_query",
				"reason":        "prepared statement not found",
				"path_terminal": true,
			},
		})
		return nil, nil, ErrNotFound
	}

	scopedCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "flightsql",
		"operation": "prepared_query",
	})
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "prepared_query",
		},
	})
	schema, streamCh, err := s.buildStream(scopedCtx, e.sql, boundParams)
	if err != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "prepared_query",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return nil, nil, err
	}
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "prepared_query",
		},
	})
	return schema, s.instrumentStream(scopedCtx, streamCh, "flightsql", "prepared_query", started), nil
}

func (s *Server) DoPutCommandStatementUpdate(
	ctx context.Context,
	cmd fsql.StatementUpdate,
) (int64, error) {
	started := time.Now()
	sql := cmd.GetQuery()
	if sql == "" {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "update",
				"reason":        "empty query",
				"path_terminal": true,
			},
		})
		return 0, ErrEmptyQuery
	}

	scopedCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "flightsql",
		"operation": "update",
	})
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "update",
		},
	})
	rows, err := s.Engine.ExecuteUpdate(scopedCtx, sql)
	if err != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "update",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return 0, err
	}
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Rows:      rows,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":          "flightsql",
			"operation":     "update",
			"path_terminal": true,
		},
	})
	return rows, nil
}

func (s *Server) DoPutCommandStatementIngest(
	ctx context.Context,
	cmd fsql.StatementIngest,
	reader flight.MessageReader,
) (int64, error) {
	started := time.Now()
	table := cmd.GetTable()
	if table == "" {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "ingest",
				"operation":     "ingest",
				"reason":        "ingest table is required",
				"path_terminal": true,
			},
		})
		return 0, status.Error(codes.InvalidArgument, "ingest table is required")
	}
	if reader == nil {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "ingest",
				"operation":     "ingest",
				"reason":        "ingest stream reader is required",
				"path_terminal": true,
			},
		})
		return 0, status.Error(codes.InvalidArgument, "ingest stream reader is required")
	}

	streamReader, err := newIngestStreamReader(reader)
	if err != nil {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "ingest",
				"operation":     "ingest",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return 0, err
	}

	ingestOpts, err := buildIngestOptions(cmd)
	if err != nil {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "ingest",
				"operation":     "ingest",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return 0, err
	}

	scopedCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "ingest",
		"operation": "ingest",
	})
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "ingest",
			"operation": "ingest",
			"table":     table,
		},
	})
	rows, err := s.Engine.IngestStream(scopedCtx, table, streamReader, ingestOpts)
	if err != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "ingest",
				"operation":     "ingest",
				"table":         table,
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return 0, err
	}
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Rows:      rows,
		Bytes:     streamReader.seenBytes,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":          "ingest",
			"operation":     "ingest",
			"table":         table,
			"path_summary":  true,
			"path_terminal": true,
		},
	})
	return rows, nil
}

// ─── DOEXCHANGE (ZERO-COPY DIRECT STREAMING) ──────────────────────────────────

type flightDataStreamReader struct {
	first  *flight.FlightData
	stream flight.FlightService_DoExchangeServer
}

func (r *flightDataStreamReader) Recv() (*flight.FlightData, error) {
	if r.first != nil {
		msg := r.first
		r.first = nil
		return msg, nil
	}
	return r.stream.Recv()
}

func (s *Server) readFlightDataParameterBatch(firstMsg *flight.FlightData, stream flight.FlightService_DoExchangeServer) (arrow.RecordBatch, error) {
	if firstMsg == nil || (len(firstMsg.GetDataHeader()) == 0 && len(firstMsg.GetDataBody()) == 0) {
		return nil, nil
	}

	rdr, err := flight.NewRecordReader(&flightDataStreamReader{first: firstMsg, stream: stream}, ipc.WithAllocator(s.Alloc))
	if err != nil {
		return nil, wrapInternal(err, "create record reader")
	}
	defer rdr.Release()

	var params arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec == nil {
			continue
		}
		if params == nil {
			rec.Retain()
			params = rec
		} else {
			rec.Release()
		}
	}

	if rdr.Err() != nil {
		if params != nil {
			params.Release()
		}
		return nil, wrapInternal(rdr.Err(), "read parameter batch")
	}
	return params, nil
}

func estimateRecordBatchBytes(batch arrow.RecordBatch) int64 {
	if batch == nil {
		return 0
	}
	var total int64
	for i := 0; i < int(batch.NumCols()); i++ {
		data := batch.Column(i).Data()
		if data == nil {
			continue
		}
		for _, buf := range data.Buffers() {
			if buf != nil {
				total += int64(buf.Len())
			}
		}
	}
	return total
}

// DoExchange operates outside the BaseServer routing and allows us to bypass channels entirely
// for true, zero-copy, synchronous streaming.
func (s *Server) DoExchange(
	ctx context.Context,
	stream flight.FlightService_DoExchangeServer,
) error {
	started := time.Now()
	msg, err := stream.Recv()
	if err != nil {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "exchange",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return status.Errorf(codes.InvalidArgument, "recv first message: %v", err)
	}
	if msg == nil || msg.GetFlightDescriptor() == nil {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "exchange",
				"reason":        "missing FlightDescriptor",
				"path_terminal": true,
			},
		})
		return status.Error(codes.InvalidArgument, "missing FlightDescriptor")
	}

	sql := string(msg.GetFlightDescriptor().GetCmd())
	if sql == "" {
		s.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "exchange",
				"reason":        "empty query",
				"path_terminal": true,
			},
		})
		return ErrEmptyQuery
	}

	scopedCtx := telemetry.WithScope(ctx, map[string]any{
		"path":      "flightsql",
		"operation": "exchange",
	})
	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "exchange",
		},
	})

	var params arrow.RecordBatch
	var readErr error
	if len(msg.GetDataHeader()) > 0 || len(msg.GetDataBody()) > 0 {
		params, readErr = s.readFlightDataParameterBatch(msg, stream)
	} else {
		next, recvErr := stream.Recv()
		if recvErr == nil && (len(next.GetDataHeader()) > 0 || len(next.GetDataBody()) > 0) {
			params, readErr = s.readFlightDataParameterBatch(next, stream)
		} else if recvErr != nil && recvErr != io.EOF {
			return wrapInternal(recvErr, "recv param batch")
		}
	}
	if readErr != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "exchange",
				"reason":        readErr.Error(),
				"path_terminal": true,
			},
		})
		return readErr
	}

	// buildStream will inherently track this execution through trackQuery
	schema, ch, err := s.buildStream(scopedCtx, sql, params)
	if err != nil {
		s.publish(scopedCtx, telemetry.Event{
			Component: telemetry.ComponentIngress,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"path":          "flightsql",
				"operation":     "exchange",
				"reason":        err.Error(),
				"path_terminal": true,
			},
		})
		return err
	}

	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentIngress,
		Type:      telemetry.TypeRequestEnd,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":      "flightsql",
			"operation": "exchange",
		},
	})

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(s.Alloc))
	defer writer.Close()
	var totalRows int64
	var totalBytes int64

	for chunk := range ch {
		if chunk.Err != nil {
			s.publish(scopedCtx, telemetry.Event{
				Component: telemetry.ComponentEgress,
				Type:      telemetry.TypeError,
				Rows:      totalRows,
				Bytes:     totalBytes,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"path":          "flightsql",
					"operation":     "exchange",
					"reason":        chunk.Err.Error(),
					"path_terminal": true,
				},
			})
			return chunk.Err
		}

		writeStarted := time.Now()
		if err := writer.Write(chunk.Data); err != nil {
			rows := int64(0)
			bytes := int64(0)
			if chunk.Data != nil {
				rows = chunk.Data.NumRows()
				bytes = estimateRecordBatchBytes(chunk.Data)
			}
			chunk.Data.Release()
			s.publish(scopedCtx, telemetry.Event{
				Component: telemetry.ComponentEgress,
				Type:      telemetry.TypeError,
				Rows:      totalRows + rows,
				Bytes:     totalBytes + bytes,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"path":          "flightsql",
					"operation":     "exchange",
					"reason":        err.Error(),
					"path_terminal": true,
				},
			})
			return wrapInternal(err, "write stream chunk")
		}
		if chunk.Data != nil {
			rows := chunk.Data.NumRows()
			bytes := estimateRecordBatchBytes(chunk.Data)
			totalRows += rows
			totalBytes += bytes
			s.publish(scopedCtx, telemetry.Event{
				Component: telemetry.ComponentEgress,
				Type:      telemetry.TypeBatch,
				Rows:      rows,
				Bytes:     bytes,
				Latency:   time.Since(writeStarted),
				Metadata: map[string]any{
					"path":          "flightsql",
					"operation":     "exchange",
					"path_terminal": true,
				},
			})
		}
		chunk.Data.Release()

		if ctx.Err() != nil {
			s.publish(scopedCtx, telemetry.Event{
				Component: telemetry.ComponentEgress,
				Type:      telemetry.TypeStall,
				Rows:      totalRows,
				Bytes:     totalBytes,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"path":          "flightsql",
					"operation":     "exchange",
					"reason":        ctx.Err().Error(),
					"path_terminal": true,
				},
			})
			return ErrCancelled
		}
	}

	s.publish(scopedCtx, telemetry.Event{
		Component: telemetry.ComponentEgress,
		Type:      telemetry.TypeRequestEnd,
		Rows:      totalRows,
		Bytes:     totalBytes,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"path":          "flightsql",
			"operation":     "exchange",
			"path_terminal": true,
		},
	})
	return nil
}

type flightServerWithExchange struct {
	flight.FlightServer
	srv *Server
}

func (f *flightServerWithExchange) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return f.srv.DoExchange(stream.Context(), stream)
}

func (s *Server) AsFlightServer() flight.FlightServer {
	return &flightServerWithExchange{FlightServer: fsql.NewFlightServer(s), srv: s}
}

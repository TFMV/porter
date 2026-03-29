// Package server provides a production-grade DuckDB-backed Arrow Flight SQL server.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/TFMV/porter/execution/engine"
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

// ─── BACKPRESSURE-AWARE STREAMING ─────────────────────────────────────────────

func (s *Server) buildStream(
	ctx context.Context,
	sql string,
	params arrow.RecordBatch,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.Engine.BuildStream(ctx, sql, params)
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
	handle := string(ticket.GetStatementHandle())

	s.handlesMu.Lock()
	h, ok := s.queryHandles[handle]
	if ok {
		delete(s.queryHandles, handle)
	}
	s.handlesMu.Unlock()

	if !ok || h.isExpired() {
		return nil, nil, ErrNotFound
	}

	return s.buildStream(ctx, h.sql, nil)
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
		return nil, nil, ErrNotFound
	}

	return s.buildStream(ctx, e.sql, boundParams)
}

func (s *Server) DoPutCommandStatementUpdate(
	ctx context.Context,
	cmd fsql.StatementUpdate,
) (int64, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return 0, ErrEmptyQuery
	}

	return s.Engine.ExecuteUpdate(ctx, sql)
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

// DoExchange operates outside the BaseServer routing and allows us to bypass channels entirely
// for true, zero-copy, synchronous streaming.
func (s *Server) DoExchange(
	ctx context.Context,
	stream flight.FlightService_DoExchangeServer,
) error {
	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "recv first message: %v", err)
	}
	if msg == nil || msg.GetFlightDescriptor() == nil {
		return status.Error(codes.InvalidArgument, "missing FlightDescriptor")
	}

	sql := string(msg.GetFlightDescriptor().GetCmd())
	if sql == "" {
		return ErrEmptyQuery
	}

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
		return readErr
	}

	// buildStream will inherently track this execution through trackQuery
	schema, ch, err := s.buildStream(ctx, sql, params)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(s.Alloc))
	defer writer.Close()

	for chunk := range ch {
		if chunk.Err != nil {
			return chunk.Err
		}

		if err := writer.Write(chunk.Data); err != nil {
			chunk.Data.Release()
			return wrapInternal(err, "write stream chunk")
		}
		chunk.Data.Release()

		if ctx.Err() != nil {
			return ErrCancelled
		}
	}

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

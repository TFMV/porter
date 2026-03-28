// Package server provides a production-grade DuckDB-backed Arrow Flight SQL server.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

func wrapInternal(err error, msg string) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

// ─── LIFECYCLE & OWNERSHIP ────────────────────────────────────────────────────

// BatchGuard provides deterministic ownership of an Arrow RecordBatch.
// It ensures that a batch is released exactly once, preventing double-frees
// and memory leaks on early returns, evictions, or overwrites.
type BatchGuard struct {
	mu    sync.Mutex
	batch arrow.RecordBatch
}

func NewBatchGuard(b arrow.RecordBatch) *BatchGuard {
	// Caller must transfer ownership to the guard. Do not Release the batch after
	// calling NewBatchGuard; the guard is responsible for releasing it exactly once.
	return &BatchGuard{batch: b}
}

// Release drops the reference safely. It is idempotent.
func (g *BatchGuard) Release() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.batch != nil {
		g.batch.Release()
		g.batch = nil
	}
}

// Retain returns the underlying batch and increments its reference count.
// The caller is now responsible for releasing the returned batch.
func (g *BatchGuard) Retain() arrow.RecordBatch {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.batch != nil {
		g.batch.Retain()
		return g.batch
	}
	return nil
}

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

	db                 adbc.Database
	handleTTL          time.Duration
	schemaProbeTimeout time.Duration
	Alloc              memory.Allocator
	Logger             *slog.Logger
	gcInterval         time.Duration
	queryHandles       map[string]queryHandle
	preparedStmts      map[string]preparedEntry
	handlesMu          sync.RWMutex

	activeOps      sync.WaitGroup
	querySemaphore chan struct{}

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
}

func NewServer(cfg Config) (*Server, error) {
	drv := drivermgr.Driver{}
	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = ":memory:"
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger.Debug("initializing DuckDB database", slog.String("dbPath", dbPath), slog.Bool("readOnly", cfg.ReadOnly))

	params := map[string]string{
		"driver": "duckdb",
		"path":   dbPath,
	}
	if dbPath != ":memory:" {
		if cfg.ReadOnly {
			params["access_mode"] = "read_only"
		} else {
			params["access_mode"] = "read_write"
		}
	}

	db, err := drv.NewDatabase(params)
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", dbPath, err)
	}

	var alloc memory.Allocator = memory.NewGoAllocator()
	if cfg.DevMode {
		alloc = memory.NewCheckedAllocator(memory.NewGoAllocator())
	}

	handleTTL := cfg.HandleTTL
	if handleTTL == 0 {
		handleTTL = defaultHandleTTL
	}

	gcInterval := cfg.GCInterval
	if gcInterval == 0 {
		gcInterval = defaultGCInterval
	}

	schemaProbeTimeout := cfg.SchemaProbeTimeout
	if schemaProbeTimeout == 0 {
		schemaProbeTimeout = defaultSchemaProbeTimeout
	}

	srv := &Server{
		db:                 db,
		handleTTL:          handleTTL,
		gcInterval:         gcInterval,
		schemaProbeTimeout: schemaProbeTimeout,
		Alloc:              alloc,
		Logger:             logger,
		queryHandles:       make(map[string]queryHandle),
		preparedStmts:      make(map[string]preparedEntry),
		stopGC:             make(chan struct{}),
	}

	if cfg.MaxConcurrentQueries > 0 {
		srv.querySemaphore = make(chan struct{}, cfg.MaxConcurrentQueries)
	}

	go srv.runGC()
	return srv, nil
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

func (s *Server) Close() error {
	return s.Shutdown(context.Background())
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.stopGCOnce.Do(func() {
		close(s.stopGC)
	})

	done := make(chan struct{})
	go func() {
		s.activeOps.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.handlesMu.Lock()
		for _, e := range s.preparedStmts {
			if e.paramGuard != nil {
				e.paramGuard.Release()
			}
		}
		s.preparedStmts = nil
		s.handlesMu.Unlock()
		return s.db.Close()
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if s.querySemaphore == nil {
		return nil
	}
	select {
	case s.querySemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) releaseQuerySlot() {
	if s.querySemaphore == nil {
		return
	}
	<-s.querySemaphore
}

// ─── UTILS ────────────────────────────────────────────────────────────────────

func newHandle() (string, error) {
	b := make([]byte, 16)
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
	probeCtx, cancel := context.WithTimeout(ctx, s.schemaProbeTimeout)
	defer cancel()

	conn, err := s.db.Open(probeCtx)
	if err != nil {
		return nil, wrapInternal(err, "failed to open adbc connection")
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, wrapInternal(err, "failed to create adbc statement")
	}
	defer stmt.Close()

	probe := fmt.Sprintf("SELECT * FROM (%s) AS _schema_probe LIMIT 0", sql)
	if err := stmt.SetSqlQuery(probe); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sql for schema derivation: %v", err)
	}

	reader, _, err := stmt.ExecuteQuery(probeCtx)
	if err != nil {
		return nil, wrapInternal(err, "failed to execute schema probe")
	}
	defer reader.Release()

	return reader.Schema(), nil
}

// ─── BACKPRESSURE-AWARE STREAMING ─────────────────────────────────────────────

// buildStream safely constructs an ADBC execution and wires it to a backpressure-aware
// channel for the FlightSQL framework. It guarantees that parameters are released.
func (s *Server) buildStream(
	ctx context.Context,
	sql string,
	params arrow.RecordBatch, // Caller hands ownership to this function
) (*arrow.Schema, <-chan flight.StreamChunk, error) {

	// Guarantee parameter release on all exit paths (early return or after streaming)
	cleanupParams := sync.Once{}
	releaseParams := func() {
		cleanupParams.Do(func() {
			if params != nil {
				params.Release()
			}
		})
	}

	conn, err := s.db.Open(ctx)
	if err != nil {
		releaseParams()
		return nil, nil, wrapInternal(err, "open db connection")
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		conn.Close()
		releaseParams()
		return nil, nil, wrapInternal(err, "create statement")
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		conn.Close()
		releaseParams()
		return nil, nil, ErrInvalidSQL
	}

	if params != nil {
		if err := stmt.Prepare(ctx); err != nil {
			stmt.Close()
			conn.Close()
			releaseParams()
			return nil, nil, wrapInternal(err, "prepare statement")
		}
		if err := stmt.Bind(ctx, params); err != nil {
			stmt.Close()
			conn.Close()
			releaseParams()
			return nil, nil, wrapInternal(err, "bind parameters")
		}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		conn.Close()
		releaseParams()
		return nil, nil, wrapInternal(err, "execute query")
	}

	if err := s.acquireQuerySlot(ctx); err != nil {
		reader.Release()
		stmt.Close()
		conn.Close()
		releaseParams()
		return nil, nil, wrapInternal(err, "acquire query slot")
	}

	s.activeOps.Add(1)

	schema := reader.Schema()

	// Unbuffered or minimal buffer ensures true backpressure against the Flight stream
	ch := make(chan flight.StreamChunk, 1)

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			reader.Release()
			stmt.Close()
			conn.Close()
			releaseParams()
			s.releaseQuerySlot()
			s.activeOps.Done()
		})
	}

	go pumpRecordsSafely(ctx, reader, cleanup, ch)
	return schema, ch, nil
}

// pumpRecordsSafely streams records with strict context cancellation checks.
func pumpRecordsSafely(
	ctx context.Context,
	reader array.RecordReader,
	cleanup func(),
	ch chan<- flight.StreamChunk,
) {
	defer close(ch)
	defer cleanup()

	for reader.Next() {
		// Strict cancellation check at write boundary
		if ctx.Err() != nil {
			return
		}

		rec := reader.RecordBatch()
		rec.Retain() // Retain for the channel; framework will release

		select {
		case ch <- flight.StreamChunk{Data: rec}:
		case <-ctx.Done():
			rec.Release()
			return
		}
	}

	if err := reader.Err(); err != nil {
		select {
		case ch <- flight.StreamChunk{Err: wrapInternal(err, "record reader error")}:
		case <-ctx.Done():
		}
	}
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

	s.handlesMu.Lock()
	e, ok := s.preparedStmts[handle]
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
	handle := string(cmd.GetPreparedStatementHandle())

	s.handlesMu.RLock()
	e, ok := s.preparedStmts[handle]
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
	handle := string(cmd.GetPreparedStatementHandle())

	var newBatch arrow.RecordBatch
	if reader.Next() {
		rec := reader.RecordBatch()
		if rec != nil {
			rec.Retain()
			newBatch = rec
		}
	}

	s.handlesMu.Lock()
	e, ok := s.preparedStmts[handle]
	var oldGuard *BatchGuard
	if ok {
		oldGuard = e.paramGuard
		if newBatch != nil {
			e.paramGuard = NewBatchGuard(newBatch) // Transfers ownership to the guard
			newBatch = nil
		}
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
	}
	s.handlesMu.Unlock()

	if !ok {
		if newBatch != nil {
			newBatch.Release()
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
		boundParams = e.paramGuard.Retain() // buildStream takes ownership of this Retain
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
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

	conn, err := s.db.Open(ctx)
	if err != nil {
		return 0, wrapInternal(err, "open db connection")
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, wrapInternal(err, "create statement")
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return 0, ErrInvalidSQL
	}
	return stmt.ExecuteUpdate(ctx)
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
	// FlightSQL parameter payloads are typically a single batch. Retain only the first
	// batch and release any additional batches to avoid accidental memory retention.
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
	if len(msg.GetDataHeader()) > 0 || len(msg.GetDataBody()) > 0 {
		params, err = s.readFlightDataParameterBatch(msg, stream)
	} else {
		next, err := stream.Recv()
		if err == nil && (len(next.GetDataHeader()) > 0 || len(next.GetDataBody()) > 0) {
			params, err = s.readFlightDataParameterBatch(next, stream)
		} else if err != nil && err != io.EOF {
			return wrapInternal(err, "recv param batch")
		}
	}
	if err != nil {
		return err
	}

	// We utilize buildStream to manage the ADBC lifecycle, but consume the channel immediately.
	// Because the channel is size 1, this blocks DuckDB from overproducing data if the network is slow.
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

		// Direct, backpressure-sensitive write to the gRPC stream
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

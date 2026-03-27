// Package server provides a DuckDB-backed Arrow Flight SQL server built on top
// of the upstream flightsql routing layer (fsql.NewFlightServer).
//
// Design invariants
//
//  1. The flightsql framework (fsql.NewFlightServer) owns all protocol routing.
//     This file implements only the subset of the flightsql.Server interface
//     that this server actively supports; every other method falls back to the
//     "unimplemented" stubs in fsql.BaseServer.
//
//  2. Handle tiers are flat.
//     - queryHandles    one-shot, created by GetFlightInfoStatement,
//     consumed by DoGetStatement.
//     - preparedStmts   persistent, created by CreatePreparedStatement,
//     deleted by ClosePreparedStatement.
//     GetFlightInfoPreparedStatement encodes the prepared handle directly into
//     a CommandPreparedStatementQuery ticket so the framework routes DoGet to
//     DoGetPreparedStatement — no intermediate exec handle.
//
//  3. All handle maps are TTL-bounded. A background goroutine evicts stale
//     entries to prevent unbounded growth under long-lived servers.
//
//  4. Parameter binding is real. DoPutPreparedStatementQuery stores a retained
//     RecordBatch; DoGetPreparedStatement uses stmt.Prepare + stmt.Bind before
//     executing. Reference counts are tracked explicitly.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
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
	defaultHandleTTL = 30 * time.Minute
	gcInterval       = 5 * time.Minute
)

// ─── handle types ─────────────────────────────────────────────────────────────

// queryHandle is a one-shot record. The handle is deleted from the map as soon
// as DoGetStatement claims it.
type queryHandle struct {
	sql     string
	expires time.Time
}

func (h queryHandle) isExpired() bool { return time.Now().After(h.expires) }

// preparedEntry is persistent across executions. paramBatch is nil until the
// client calls DoPutPreparedStatementQuery. We retain the batch when we store
// it and release it when the entry is deleted (ClosePreparedStatement / GC).
type preparedEntry struct {
	sql        string
	paramBatch arrow.RecordBatch // guarded by preparedStmtsMu; nil = no binding
	expires    time.Time         // reset on every successful use
}

func (e preparedEntry) isExpired() bool { return time.Now().After(e.expires) }

// ─── server ───────────────────────────────────────────────────────────────────

// Server is a DuckDB-backed FlightSQL server. It embeds fsql.BaseServer to
// satisfy the full flightsql.Server interface via "unimplemented" stubs, then
// overrides only the methods this server actually provides.
type Server struct {
	fsql.BaseServer

	db        adbc.Database
	handleTTL time.Duration

	queryHandles   map[string]queryHandle
	queryHandlesMu sync.Mutex

	// Single mutex rather than RWMutex: the write paths (Put, GC) are
	// infrequent, and avoiding the complexity of upgrading locks is worthwhile.
	preparedStmts   map[string]preparedEntry
	preparedStmtsMu sync.Mutex

	stopGC chan struct{}
}

type Config struct {
	DBPath string
	Port   int
}

// NewServer opens a DuckDB database at dbPath ("" or ":memory:" for in-memory)
// and starts a background goroutine that evicts expired handles.
func NewServer(cfg Config) (*Server, error) {
	if cfg.DBPath == "" {
		cfg.DBPath = ":memory:"
	}
	drv := drivermgr.Driver{}
	db, err := drv.NewDatabase(map[string]string{
		"driver": "duckdb",
		"path":   cfg.DBPath,
	})
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", cfg.DBPath, err)
	}

	srv := &Server{
		db:            db,
		handleTTL:     defaultHandleTTL,
		queryHandles:  make(map[string]queryHandle),
		preparedStmts: make(map[string]preparedEntry),
		stopGC:        make(chan struct{}),
	}
	srv.Alloc = memory.NewGoAllocator() // field on fsql.BaseServer

	go srv.runGC()
	return srv, nil
}

// flightServerWithExchange wraps a FlightSQL server and exposes a raw
// Flight DoExchange implementation from the underlying adapter.
type flightServerWithExchange struct {
	flight.FlightServer
	srv *Server
}

func (f *flightServerWithExchange) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return f.srv.DoExchange(stream.Context(), stream)
}

// AsFlightServer wraps this server with the fsql routing layer for gRPC
// registration:
//
//	flight.RegisterFlightServiceServer(grpcSrv, srv.AsFlightServer())
func (s *Server) AsFlightServer() flight.FlightServer {
	return &flightServerWithExchange{FlightServer: fsql.NewFlightServer(s), srv: s}
}

// Close stops the GC goroutine, releases all retained parameter batches, and
// shuts down the database connection.
func (s *Server) Close() error {
	close(s.stopGC)

	s.preparedStmtsMu.Lock()
	for _, e := range s.preparedStmts {
		if e.paramBatch != nil {
			e.paramBatch.Release()
		}
	}
	s.preparedStmts = nil
	s.preparedStmtsMu.Unlock()

	return s.db.Close()
}

// ─── GC ───────────────────────────────────────────────────────────────────────

func (s *Server) runGC() {
	t := time.NewTicker(gcInterval)
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

// evict removes expired entries from both maps. Batches are released after the
// lock is dropped to avoid holding it during allocator callbacks.
func (s *Server) evict() {
	s.queryHandlesMu.Lock()
	for k, h := range s.queryHandles {
		if h.isExpired() {
			delete(s.queryHandles, k)
		}
	}
	s.queryHandlesMu.Unlock()

	var toRelease []arrow.RecordBatch
	s.preparedStmtsMu.Lock()
	for k, e := range s.preparedStmts {
		if e.isExpired() {
			if e.paramBatch != nil {
				toRelease = append(toRelease, e.paramBatch)
			}
			delete(s.preparedStmts, k)
		}
	}
	s.preparedStmtsMu.Unlock()

	for _, b := range toRelease {
		b.Release()
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func newHandle() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate handle: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// preparedStatementTicket encodes handle as a CommandPreparedStatementQuery so
// that the fsql routing layer dispatches the subsequent DoGet call to
// DoGetPreparedStatement rather than DoGetStatement.
//
// There is no public helper in the flightsql package for this ticket type
// (CreateStatementQueryTicket produces TicketStatementQuery), so we construct
// it from the underlying protobuf types — the same encoding the framework uses
// when it decodes incoming tickets.
func preparedStatementTicket(handle []byte) ([]byte, error) {
	cmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: handle}
	a, err := anypb.New(cmd)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(a)
}

// ─── schema derivation ────────────────────────────────────────────────────────

// deriveSchema returns the Arrow schema for sql without materialising any rows.
//
// We wrap the query in "SELECT * … LIMIT 0". DuckDB will parse and plan the
// full statement but skip leaf operator execution, making this cheap for typical
// analytical queries. It is not execution-free: volatile functions, some UDFs,
// and certain subquery forms may still evaluate. Callers should sanitize
// untrusted SQL before passing it here.
//
// A stronger alternative — DuckDB's DESCRIBE statement plus Arrow type mapping,
// or an ADBC PrepareSchema API if one becomes available at the Go level — would
// eliminate residual side effects entirely.
func (s *Server) deriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	conn, err := s.db.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	probe := fmt.Sprintf("SELECT * FROM (%s) AS _schema_probe LIMIT 0", sql)
	if err := stmt.SetSqlQuery(probe); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	return reader.Schema(), nil
}

// ─── execution core ───────────────────────────────────────────────────────────

// startStream executes sql and returns the schema together with a channel of
// StreamChunks. See startStreamWithParams for details.
func (s *Server) startStream(ctx context.Context, sql string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.startStreamWithParams(ctx, sql, nil)
}

// startStreamWithParams opens a fresh ADBC connection, optionally prepares the
// statement and binds params, executes, and drains the reader into a buffered
// channel via a background goroutine.
//
// Ownership of params: the caller must have already called Retain on params
// before passing it here. startStreamWithParams owns exactly one Release —
// either via an error-path branch below, or inside the cleanup closure that the
// goroutine invokes when it finishes streaming.
func (s *Server) startStreamWithParams(
	ctx context.Context,
	sql string,
	params arrow.RecordBatch, // caller has pre-retained; this fn owns the release
) (*arrow.Schema, <-chan flight.StreamChunk, error) {

	releaseParams := func() {
		if params != nil {
			params.Release()
		}
	}

	conn, err := s.db.Open(ctx)
	if err != nil {
		releaseParams()
		return nil, nil, err
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		conn.Close()
		releaseParams()
		return nil, nil, err
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		conn.Close()
		releaseParams()
		return nil, nil, err
	}

	if params != nil {
		// Prepare must be called before Bind for parameterised execution.
		if err := stmt.Prepare(ctx); err != nil {
			stmt.Close()
			conn.Close()
			releaseParams()
			return nil, nil, err
		}
		// stmt.Bind does not retain params. We keep our reference alive through
		// the cleanup closure so the batch memory outlives the streaming goroutine.
		if err := stmt.Bind(ctx, params); err != nil {
			stmt.Close()
			conn.Close()
			releaseParams()
			return nil, nil, err
		}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		conn.Close()
		releaseParams()
		return nil, nil, err
	}

	schema := reader.Schema()
	ch := make(chan flight.StreamChunk, 4)

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			reader.Release()
			stmt.Close()
			conn.Close()
			releaseParams() // safe: called at most once via sync.Once
		})
	}

	go pumpRecords(ctx, reader, cleanup, ch)
	return schema, ch, nil
}

// pumpRecords drains reader into ch. Each record is Retained before sending so
// the framework's Release after writing to the stream does not race with reader
// advancing. The channel is closed and cleanup is called when the reader is
// exhausted, errors, or the context is cancelled.
func pumpRecords(
	ctx context.Context,
	reader array.RecordReader,
	cleanup func(),
	ch chan<- flight.StreamChunk,
) {
	defer close(ch)
	defer cleanup()

	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		select {
		case ch <- flight.StreamChunk{Data: rec}:
		case <-ctx.Done():
			rec.Release()
			return
		}
	}
	if err := reader.Err(); err != nil {
		ch <- flight.StreamChunk{Err: err}
	}
}

// ─── statement queries ────────────────────────────────────────────────────────

// GetFlightInfoStatement handles GetFlightInfo for a plain SQL query. It
// derives the schema eagerly, stores the SQL in a TTL-bounded one-shot handle,
// and returns a TicketStatementQuery that the framework routes to DoGetStatement.
func (s *Server) GetFlightInfoStatement(
	ctx context.Context,
	cmd fsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "empty query")
	}

	schema, err := s.deriveSchema(ctx, sql)
	if err != nil {
		return nil, err
	}

	handle, err := newHandle()
	if err != nil {
		return nil, err
	}

	s.queryHandlesMu.Lock()
	s.queryHandles[handle] = queryHandle{sql: sql, expires: time.Now().Add(s.handleTTL)}
	s.queryHandlesMu.Unlock()

	ticketBytes, err := fsql.CreateStatementQueryTicket([]byte(handle))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticketBytes}}},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// GetSchemaStatement returns the Arrow schema for a SQL query without executing
// it. This is the cheapest schema-only introspection path.
func (s *Server) GetSchemaStatement(
	ctx context.Context,
	cmd fsql.StatementQuery,
	_ *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "empty query")
	}
	schema, err := s.deriveSchema(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.Alloc)}, nil
}

// DoGetStatement streams the result set for a one-shot statement handle. The
// handle is deleted on first claim; subsequent calls with the same handle return
// NotFound.
func (s *Server) DoGetStatement(
	ctx context.Context,
	ticket fsql.StatementQueryTicket,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(ticket.GetStatementHandle())

	s.queryHandlesMu.Lock()
	h, ok := s.queryHandles[handle]
	if ok {
		delete(s.queryHandles, handle) // claim and remove atomically under the lock
	}
	s.queryHandlesMu.Unlock()

	switch {
	case !ok:
		return nil, nil, status.Errorf(codes.NotFound, "unknown or expired statement handle")
	case h.isExpired():
		// GC may race with DoGet; handle the rare case where we claimed an
		// already-expired entry.
		return nil, nil, status.Errorf(codes.DeadlineExceeded, "statement handle expired")
	}

	return s.startStream(ctx, h.sql)
}

// ─── prepared statements ──────────────────────────────────────────────────────

// CreatePreparedStatement stores the SQL under a random TTL-bounded handle
// and returns it to the client without requiring parameter binding.
// Parameter validation happens later during execution.
func (s *Server) CreatePreparedStatement(
	ctx context.Context,
	req fsql.ActionCreatePreparedStatementRequest,
) (fsql.ActionCreatePreparedStatementResult, error) {
	sql := req.GetQuery()
	if sql == "" {
		return fsql.ActionCreatePreparedStatementResult{},
			status.Error(codes.InvalidArgument, "empty query")
	}

	handle, err := newHandle()
	if err != nil {
		return fsql.ActionCreatePreparedStatementResult{}, err
	}

	s.preparedStmtsMu.Lock()
	s.preparedStmts[handle] = preparedEntry{
		sql:     sql,
		expires: time.Now().Add(s.handleTTL),
	}
	s.preparedStmtsMu.Unlock()

	log.Printf("CreatePreparedStatement handle=%s sql=%q", handle, sql)
	return fsql.ActionCreatePreparedStatementResult{Handle: []byte(handle)}, nil
}

func (s *Server) ClosePreparedStatement(
	_ context.Context,
	req fsql.ActionClosePreparedStatementRequest,
) error {
	handle := string(req.GetPreparedStatementHandle())

	s.preparedStmtsMu.Lock()
	e, ok := s.preparedStmts[handle]
	if ok {
		delete(s.preparedStmts, handle)
	}
	s.preparedStmtsMu.Unlock()

	log.Printf("ClosePreparedStatement handle=%s closed=%t", handle, ok)
	if ok && e.paramBatch != nil {
		e.paramBatch.Release()
	}
	return nil
}

// GetFlightInfoPreparedStatement returns a FlightInfo for an existing prepared
// statement. The ticket is encoded as a CommandPreparedStatementQuery so the
// framework routes the subsequent DoGet directly to DoGetPreparedStatement —
// no intermediate exec handle is created.
func (s *Server) GetFlightInfoPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	handle := string(cmd.GetPreparedStatementHandle())

	s.preparedStmtsMu.Lock()
	e, ok := s.preparedStmts[handle]
	if ok {
		e.expires = time.Now().Add(s.handleTTL) // reset TTL on use
		s.preparedStmts[handle] = e
	}
	s.preparedStmtsMu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "prepared statement not found")
	}

	log.Printf("GetFlightInfoPreparedStatement handle=%s sql=%q", handle, e.sql)
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

// GetSchemaPreparedStatement returns the dataset schema for a prepared statement.
func (s *Server) GetSchemaPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
	_ *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	handle := string(cmd.GetPreparedStatementHandle())

	s.preparedStmtsMu.Lock()
	e, ok := s.preparedStmts[handle]
	s.preparedStmtsMu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "prepared statement not found")
	}

	log.Printf("GetSchemaPreparedStatement handle=%s sql=%q", handle, e.sql)
	schema, err := s.deriveSchema(ctx, e.sql)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, s.Alloc)}, nil
}

// DoPutPreparedStatementQuery receives parameter bindings from the client and
// atomically replaces the stored batch on the prepared entry. The old batch is
// released after the lock is dropped. The new batch is Retained before storage
// so its lifetime is tied to the entry, not the MessageReader.
func (s *Server) DoPutPreparedStatementQuery(
	_ context.Context,
	cmd fsql.PreparedStatementQuery,
	reader flight.MessageReader,
	_ flight.MetadataWriter,
) ([]byte, error) {
	handle := string(cmd.GetPreparedStatementHandle())

	// Read the parameter batch before acquiring the lock to minimise lock
	// contention. If the handle is missing we release the batch and error.
	var newBatch arrow.RecordBatch
	if reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain() // our reference; released by ClosePreparedStatement / GC / next Put
		newBatch = rec
	}

	log.Printf("DoPutPreparedStatementQuery handle=%s paramRows=%d", handle, func() int {
		if newBatch == nil {
			return 0
		}
		return int(newBatch.NumRows())
	}())

	s.preparedStmtsMu.Lock()
	e, ok := s.preparedStmts[handle]
	var oldBatch arrow.RecordBatch
	if ok {
		oldBatch = e.paramBatch
		e.paramBatch = newBatch
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
	}
	s.preparedStmtsMu.Unlock()

	if !ok {
		if newBatch != nil {
			newBatch.Release()
		}
		return nil, status.Errorf(codes.NotFound, "prepared statement not found")
	}

	if oldBatch != nil {
		oldBatch.Release()
	}
	return cmd.GetPreparedStatementHandle(), nil
}

// DoGetPreparedStatement streams query results for a prepared statement,
// applying any parameter batch set by a prior DoPutPreparedStatementQuery.
//
// Reference counting: we Retain the stored batch before dropping the lock.
// Ownership of that extra reference is transferred to startStreamWithParams,
// which releases it exactly once through its cleanup closure.
func (s *Server) DoGetPreparedStatement(
	ctx context.Context,
	cmd fsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(cmd.GetPreparedStatementHandle())

	s.preparedStmtsMu.Lock()
	e, ok := s.preparedStmts[handle]
	if ok {
		if e.paramBatch != nil {
			e.paramBatch.Retain() // hand a reference to startStreamWithParams
		}
		e.expires = time.Now().Add(s.handleTTL)
		s.preparedStmts[handle] = e
	}
	s.preparedStmtsMu.Unlock()

	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "prepared statement not found")
	}

	log.Printf("DoGetPreparedStatement handle=%s boundParams=%t sql=%q", handle, e.paramBatch != nil, e.sql)
	// e.paramBatch is nil (no binding) or pre-retained; startStreamWithParams
	// owns the reference from this point forward.
	return s.startStreamWithParams(ctx, e.sql, e.paramBatch)
}

// ─── updates ──────────────────────────────────────────────────────────────────

// DoPutCommandStatementUpdate executes a DML or DDL statement (INSERT, UPDATE,
// DELETE, CREATE TABLE, …) and returns the number of affected rows.
func (s *Server) DoPutCommandStatementUpdate(
	ctx context.Context,
	cmd fsql.StatementUpdate,
) (int64, error) {
	sql := cmd.GetQuery()
	if sql == "" {
		return 0, status.Error(codes.InvalidArgument, "empty query")
	}

	conn, err := s.db.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return 0, err
	}
	return stmt.ExecuteUpdate(ctx)
}

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

func (s *Server) recvFlightData(ctx context.Context, stream flight.FlightService_DoExchangeServer) (*flight.FlightData, error) {
	type result struct {
		msg *flight.FlightData
		err error
	}

	ch := make(chan result, 1)
	go func() {
		msg, err := stream.Recv()
		ch <- result{msg: msg, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.msg, res.err
	}
}

func (s *Server) readFlightDataParameterBatch(firstMsg *flight.FlightData, stream flight.FlightService_DoExchangeServer) (arrow.RecordBatch, error) {
	if firstMsg == nil || (len(firstMsg.GetDataHeader()) == 0 && len(firstMsg.GetDataBody()) == 0) {
		return nil, nil
	}

	rdr, err := flight.NewRecordReader(&flightDataStreamReader{first: firstMsg, stream: stream}, ipc.WithAllocator(s.Alloc))
	if err != nil {
		return nil, err
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
		return nil, rdr.Err()
	}

	return params, nil
}

func (s *Server) DoExchange(
	ctx context.Context,
	stream flight.FlightService_DoExchangeServer,
) error {

	// ─────────────────────────────────────────────
	// Step 1: Read first message
	// ─────────────────────────────────────────────

	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "recv first message: %v", err)
	}
	if msg == nil || msg.GetFlightDescriptor() == nil {
		return status.Error(codes.InvalidArgument, "missing FlightDescriptor")
	}

	sql := string(msg.GetFlightDescriptor().GetCmd())
	if sql == "" {
		return status.Error(codes.InvalidArgument, "empty SQL command")
	}

	// ─────────────────────────────────────────────
	// Step 2: Read optional parameter batch (strict)
	// ─────────────────────────────────────────────

	var params arrow.RecordBatch

	hasInlineParams := len(msg.GetDataHeader()) > 0 || len(msg.GetDataBody()) > 0

	if hasInlineParams {
		params, err = s.readFlightDataParameterBatch(msg, stream)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "read params: %v", err)
		}
	} else {
		next, err := stream.Recv()

		switch {
		case err == io.EOF:
			// no params, valid
		case err != nil:
			return status.Errorf(codes.InvalidArgument, "recv param batch: %v", err)
		default:
			if len(next.GetDataHeader()) > 0 || len(next.GetDataBody()) > 0 {
				params, err = s.readFlightDataParameterBatch(next, stream)
				if err != nil {
					return err
				}
			}
		}
	}

	// ─────────────────────────────────────────────
	// Step 3: Execute
	// ─────────────────────────────────────────────

	var (
		schema *arrow.Schema
		ch     <-chan flight.StreamChunk
	)

	if params != nil {
		schema, ch, err = s.startStreamWithParams(ctx, sql, params)
	} else {
		schema, ch, err = s.startStream(ctx, sql)
	}
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema), ipc.WithAllocator(s.Alloc))

	for chunk := range ch {
		if chunk.Err != nil {
			return chunk.Err
		}

		if err := writer.Write(chunk.Data); err != nil {
			chunk.Data.Release()
			return err
		}
		chunk.Data.Release()

		// respect cancellation explicitly
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return writer.Close()
}

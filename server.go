package flightsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─────────────────────────────────────────────
// CORE MODEL
// ─────────────────────────────────────────────

// ExecutionPlan is the canonical internal representation of a parsed query.
// SQL is the source of truth; ID is its stable hash.
// schemaBytes is populated lazily on first use and is safe for concurrent reads.
type ExecutionPlan struct {
	ID  string
	SQL string

	schemaOnce  sync.Once
	schemaBytes []byte // serialized Arrow IPC schema
	schemaErr   error
}

// ─────────────────────────────────────────────
// PLAN CACHE
// ─────────────────────────────────────────────

type planCache struct {
	m sync.Map // map[planID string]*ExecutionPlan
}

func newPlanCache() *planCache { return &planCache{} }

func (c *planCache) getOrCreate(sql string) *ExecutionPlan {
	id := hashSQL(sql)
	if v, ok := c.m.Load(id); ok {
		return v.(*ExecutionPlan)
	}
	p := &ExecutionPlan{ID: id, SQL: sql}
	actual, _ := c.m.LoadOrStore(id, p)
	return actual.(*ExecutionPlan)
}

func (c *planCache) get(id string) (*ExecutionPlan, bool) {
	v, ok := c.m.Load(id)
	if !ok {
		return nil, false
	}
	return v.(*ExecutionPlan), true
}

func hashSQL(sql string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(sql)))
	return hex.EncodeToString(h[:])
}

// ─────────────────────────────────────────────
// TICKET (STRICT CONTRACT)
//
// Invariant: Ticket.Ticket bytes ≡ JSON-encoded ExecTicket{PlanID}.
// FlightDescriptor.Cmd ≡ raw SQL bytes (never a plan ID).
// ─────────────────────────────────────────────

type ExecTicket struct {
	PlanID string `json:"plan_id"`
}

func encodeTicket(t ExecTicket) []byte {
	b, _ := json.Marshal(t)
	return b
}

// decodeTicket is the single decode path for all Ticket bytes.
// It rejects unknown fields and requires a non-empty PlanID.
func decodeTicket(b []byte) (ExecTicket, error) {
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var t ExecTicket
	if err := dec.Decode(&t); err != nil {
		return ExecTicket{}, status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}
	if t.PlanID == "" {
		return ExecTicket{}, status.Error(codes.InvalidArgument, "ticket missing plan_id")
	}
	return t, nil
}

// ─────────────────────────────────────────────
// CONNECTION POOL
//
// One adbc.Database is opened once. The pool manages adbc.Connection objects
// drawn from that single database, ensuring a single logical DuckDB instance.
// ─────────────────────────────────────────────

type connPool struct {
	mu    sync.Mutex
	conns []adbc.Connection
	db    adbc.Database
}

func newPool(path string) (*connPool, error) {
	if path == "" {
		path = ":memory:"
	}
	drv := drivermgr.Driver{}
	db, err := drv.NewDatabase(map[string]string{
		"driver": "duckdb",
		"path":   path,
	})
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	return &connPool{db: db, conns: make([]adbc.Connection, 0, 8)}, nil
}

// get returns a pooled connection or opens a new one from the shared Database.
func (p *connPool) get(ctx context.Context) (adbc.Connection, error) {
	p.mu.Lock()
	if n := len(p.conns); n > 0 {
		c := p.conns[n-1]
		p.conns = p.conns[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return p.db.Open(ctx)
}

// put returns a connection to the pool. The caller MUST NOT use the connection
// after calling put, and MUST have closed all statements opened on it first.
func (p *connPool) put(c adbc.Connection) {
	p.mu.Lock()
	p.conns = append(p.conns, c)
	p.mu.Unlock()
}

// ─────────────────────────────────────────────
// SERVER
// ─────────────────────────────────────────────

type Server struct {
	flight.BaseFlightServer

	allocator memory.Allocator
	pool      *connPool
	plans     *planCache
}

func NewServer(dbPath string) (*Server, error) {
	pool, err := newPool(dbPath)
	if err != nil {
		return nil, err
	}
	return &Server{
		allocator: memory.NewGoAllocator(),
		pool:      pool,
		plans:     newPlanCache(),
	}, nil
}

// ─────────────────────────────────────────────
// INTERNAL HELPERS (single source of truth)
// ─────────────────────────────────────────────

// planFromSQL returns (or creates) the canonical ExecutionPlan for sql.
// FlightDescriptor.Cmd → SQL → planFromSQL → plan.
func (s *Server) planFromSQL(sql string) (*ExecutionPlan, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "empty SQL")
	}
	if strings.HasPrefix(sql, "{") {
		return nil, status.Error(codes.InvalidArgument, "FlightDescriptor.Cmd must be raw SQL, not JSON")
	}
	return s.plans.getOrCreate(sql), nil
}

// getPlanFromTicket decodes a flight.Ticket and resolves the cached plan.
// Ticket bytes → PlanID → plan.
func (s *Server) getPlanFromTicket(t *flight.Ticket) (*ExecutionPlan, error) {
	et, err := decodeTicket(t.Ticket)
	if err != nil {
		return nil, err
	}
	plan, ok := s.plans.get(et.PlanID)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plan %q not found; call GetFlightInfo first", et.PlanID)
	}
	return plan, nil
}

// buildTicket creates the canonical Ticket for a plan.
func (s *Server) buildTicket(plan *ExecutionPlan) *flight.Ticket {
	return &flight.Ticket{Ticket: encodeTicket(ExecTicket{PlanID: plan.ID})}
}

// buildFlightInfo is the single constructor for all FlightInfo responses.
// It derives (and caches) the Arrow schema by running a zero-row query.
func (s *Server) buildFlightInfo(ctx context.Context, plan *ExecutionPlan) (*flight.FlightInfo, error) {
	schemaBytes, err := s.schemaForPlan(ctx, plan)
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Schema: schemaBytes,
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(plan.SQL),
		},
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: s.buildTicket(plan)},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// schemaForPlan derives the Arrow schema for a plan, caching the result.
// It executes `SELECT * FROM (...) LIMIT 0` to interrogate DuckDB's type system.
func (s *Server) schemaForPlan(ctx context.Context, plan *ExecutionPlan) ([]byte, error) {
	plan.schemaOnce.Do(func() {
		schema, err := s.deriveSchema(ctx, plan.SQL)
		if err != nil {
			plan.schemaErr = err
			return
		}
		b := flight.SerializeSchema(schema, s.allocator)
		plan.schemaBytes = b
	})
	if plan.schemaErr != nil {
		return nil, plan.schemaErr
	}
	return plan.schemaBytes, nil
}

// deriveSchema runs a zero-row query to extract the result schema from DuckDB.
func (s *Server) deriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	limitSQL := fmt.Sprintf("SELECT * FROM (%s) __schema_probe LIMIT 0", sql)

	conn, err := s.pool.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		s.pool.put(conn)
		return nil, fmt.Errorf("new statement: %w", err)
	}

	if err := stmt.SetSqlQuery(limitSQL); err != nil {
		_ = stmt.Close()
		s.pool.put(conn)
		return nil, fmt.Errorf("set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		s.pool.put(conn)
		return nil, fmt.Errorf("execute schema probe: %w", err)
	}

	schema := reader.Schema()
	reader.Release()
	_ = stmt.Close() // stmt closed before conn is returned
	s.pool.put(conn) // conn returned only after stmt is closed

	return schema, nil
}

// ─────────────────────────────────────────────
// EXECUTION ENGINE
// ─────────────────────────────────────────────

// execute runs a plan and returns a RecordReader plus a deterministic cleanup
// function. The caller MUST call cleanup() exactly once when done reading.
//
// Cleanup ordering: reader.Release → stmt.Close → pool.put(conn).
// This ordering is strictly enforced; conn is never returned to pool before
// the statement that owns it is closed.
func (s *Server) execute(ctx context.Context, plan *ExecutionPlan) (array.RecordReader, func(), error) {
	conn, err := s.pool.get(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire connection: %w", err)
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		s.pool.put(conn)
		return nil, nil, fmt.Errorf("new statement: %w", err)
	}

	if err := stmt.SetSqlQuery(plan.SQL); err != nil {
		_ = stmt.Close()
		s.pool.put(conn)
		return nil, nil, fmt.Errorf("set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		s.pool.put(conn)
		return nil, nil, fmt.Errorf("execute query: %w", err)
	}

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			reader.Release()
			_ = stmt.Close()
			s.pool.put(conn)
		})
	}

	return reader, cleanup, nil
}

// ─────────────────────────────────────────────
// HANDSHAKE
// ─────────────────────────────────────────────

func (s *Server) Handshake(stream flight.FlightService_HandshakeServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.Send(&flight.HandshakeResponse{Payload: req.Payload}); err != nil {
			return err
		}
	}
}

// ─────────────────────────────────────────────
// LIST FLIGHTS
// ─────────────────────────────────────────────

func (s *Server) ListFlights(req *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	var retErr error
	s.plans.m.Range(func(_, value any) bool {
		plan := value.(*ExecutionPlan)
		info, err := s.buildFlightInfo(stream.Context(), plan)
		if err != nil {
			// Skip plans whose schema cannot be derived (e.g. DDL).
			return true
		}
		if err := stream.Send(info); err != nil {
			retErr = err
			return false
		}
		return true
	})
	return retErr
}

// ─────────────────────────────────────────────
// GET FLIGHT INFO
//
// Contract: FlightDescriptor.Cmd = raw SQL bytes.
// Returns a FlightInfo whose Ticket encodes the stable PlanID.
// ─────────────────────────────────────────────

func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	sql := strings.TrimSpace(string(desc.Cmd))
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "FlightDescriptor.Cmd must contain SQL")
	}

	plan, err := s.planFromSQL(sql)
	if err != nil {
		return nil, err
	}

	return s.buildFlightInfo(ctx, plan)
}

// ─────────────────────────────────────────────
// GET SCHEMA
//
// Contract: FlightDescriptor.Cmd = raw SQL bytes (same as GetFlightInfo).
// ─────────────────────────────────────────────

func (s *Server) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	sql := strings.TrimSpace(string(desc.Cmd))
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "FlightDescriptor.Cmd must contain SQL")
	}

	plan, err := s.planFromSQL(sql)
	if err != nil {
		return nil, err
	}

	schemaBytes, err := s.schemaForPlan(ctx, plan)
	if err != nil {
		return nil, err
	}

	return &flight.SchemaResult{Schema: schemaBytes}, nil
}

// ─────────────────────────────────────────────
// DO GET
//
// Contract: Ticket.Ticket = JSON ExecTicket{PlanID}.
// ─────────────────────────────────────────────

func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	plan, err := s.getPlanFromTicket(ticket)
	if err != nil {
		return err
	}

	reader, cleanup, err := s.execute(stream.Context(), plan)
	if err != nil {
		return err
	}
	defer cleanup()

	w := flight.NewRecordWriter(stream,
		ipc.WithAllocator(s.allocator),
		ipc.WithSchema(reader.Schema()),
	)
	defer w.Close()

	for reader.Next() {
		rec := reader.Record()
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	return reader.Err()
}

// ─────────────────────────────────────────────
// DO EXCHANGE
//
// Contract: first incoming FlightData.AppMetadata = JSON ExecTicket{PlanID}.
// Streams IPC records directly to the response stream (no intermediate buffer).
// ─────────────────────────────────────────────

func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "recv first message: %v", err)
	}

	// AppMetadata carries the ExecTicket on the first message.
	et, err := decodeTicket(first.AppMetadata)
	if err != nil {
		return err
	}

	plan, ok := s.plans.get(et.PlanID)
	if !ok {
		return status.Errorf(codes.NotFound, "plan %q not found", et.PlanID)
	}

	reader, cleanup, err := s.execute(stream.Context(), plan)
	if err != nil {
		return err
	}
	defer cleanup()

	// Stream IPC records directly to the response — no intermediate buffer.
	w := flight.NewRecordWriter(stream,
		ipc.WithAllocator(s.allocator),
		ipc.WithSchema(reader.Schema()),
	)
	defer w.Close()

	for reader.Next() {
		if err := w.Write(reader.Record()); err != nil {
			return err
		}
	}
	return reader.Err()
}

// ─────────────────────────────────────────────
// ACTIONS
// ─────────────────────────────────────────────

func (s *Server) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	for _, a := range []flight.ActionType{
		{Type: "CreatePreparedStatement", Description: "Register a SQL query and return its stable PlanID ticket"},
		{Type: "ClosePreparedStatement", Description: "Remove a prepared statement by PlanID"},
	} {
		a := a
		if err := stream.Send(&a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {

	case "CreatePreparedStatement":
		var req struct {
			SQL string `json:"sql"`
		}
		dec := json.NewDecoder(strings.NewReader(string(action.Body)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid CreatePreparedStatement body: %v", err)
		}
		if strings.TrimSpace(req.SQL) == "" {
			return status.Error(codes.InvalidArgument, "sql must not be empty")
		}
		plan, err := s.planFromSQL(req.SQL)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{
			Body: encodeTicket(ExecTicket{PlanID: plan.ID}),
		})

	case "ClosePreparedStatement":
		var req struct {
			PlanID string `json:"plan_id"`
		}
		_ = json.Unmarshal(action.Body, &req)
		// Plans are immutable and content-addressed; "closing" one is a no-op
		// in this server model. We acknowledge the request for protocol compliance.
		return stream.Send(&flight.Result{Body: []byte(`{"ok":true}`)})

	default:
		return status.Errorf(codes.Unimplemented, "unknown action %q", action.Type)
	}
}

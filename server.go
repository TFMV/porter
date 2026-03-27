package flightsql

// Production-hardened Apache Arrow Flight SQL server backed by DuckDB via ADBC.
//
// Wire contract (immutable):
//
//	GetFlightInfo  FlightDescriptor.Cmd = raw SQL bytes
//	GetSchema      FlightDescriptor.Cmd = raw SQL bytes
//	DoGet          Ticket.Ticket        = JSON {"plan_id":"<sha256>"}
//	DoExchange     first FlightData.FlightDescriptor.Cmd = raw SQL bytes
//	DoAction(CreatePreparedStatement) body = JSON {"sql":"..."}
//	DoAction(ClosePreparedStatement)  body = JSON {"plan_id":"..."}
//
// DoExchange uses FlightDescriptor.Cmd (not AppMetadata) so that any
// spec-compliant Arrow Flight client (Go, Python, Java, C++) can drive it
// without relying on client-extension fields.

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
	"time"

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

// schemaDerivationTimeout bounds the zero-row probe query used to derive a
// result schema. Applied on top of the caller's context so that an expensive
// query plan cannot stall the server indefinitely, and so that a short-lived
// caller context cannot leave the cache permanently empty on transient failure.
const schemaDerivationTimeout = 30 * time.Second

// ─────────────────────────────────────────────
// CORE MODEL
// ─────────────────────────────────────────────

// ExecutionPlan is the canonical internal representation of a registered query.
//
// ID is the SHA-256 hash of the trimmed SQL and is stable across restarts for
// the same query text.  schemaBytes is written at most once (under schemaMu)
// and is then read-only forever.
type ExecutionPlan struct {
	ID  string
	SQL string

	// schemaMu guards the lazy, retryable population of schemaBytes.
	// Once schemaBytes is non-nil it is never modified again.
	schemaMu    sync.RWMutex
	schemaBytes []byte // serialised Arrow IPC schema; nil until first success
}

// cachedSchema returns the schema bytes if they have already been derived,
// or nil otherwise. It never blocks.
func (p *ExecutionPlan) cachedSchema() []byte {
	p.schemaMu.RLock()
	defer p.schemaMu.RUnlock()
	return p.schemaBytes
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
// TICKET
//
// Invariant:
//
//	Ticket.Ticket bytes  ≡  JSON {"plan_id":"<hex>"}
//	FlightDescriptor.Cmd ≡  raw SQL (never JSON)
// ─────────────────────────────────────────────

type execTicket struct {
	PlanID string `json:"plan_id"`
}

func encodeTicket(t execTicket) []byte {
	b, _ := json.Marshal(t)
	return b
}

// decodeTicket is the single decode path for all Ticket payloads.
// Unknown fields are rejected; an empty PlanID is an error.
func decodeTicket(b []byte) (execTicket, error) {
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	var t execTicket
	if err := dec.Decode(&t); err != nil {
		return execTicket{}, status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}
	if t.PlanID == "" {
		return execTicket{}, status.Error(codes.InvalidArgument, "ticket missing plan_id")
	}
	return t, nil
}

// ─────────────────────────────────────────────
// DATABASE
//
// connDB wraps a single adbc.Database. Every call to conn() opens a fresh
// adbc.Connection that is closed by the caller when execution completes.
//
// There is NO connection pool. Opening a new connection per execution
// guarantees:
//   - No shared statement state between concurrent RPCs.
//   - No risk of returning a connection whose statement is still live.
//   - No concurrent-write corruption on DuckDB's internal structures.
//
// For a file-backed database the overhead is a lightweight handle
// acquisition; for :memory: it is negligible.
// ─────────────────────────────────────────────

type connDB struct {
	db adbc.Database
}

func newConnDB(path string) (*connDB, error) {
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
	return &connDB{db: db}, nil
}

// conn opens a new connection from the shared database.
// The caller MUST close the returned connection, and MUST do so only after
// closing all statements that were opened on it.
func (d *connDB) conn(ctx context.Context) (adbc.Connection, error) {
	return d.db.Open(ctx)
}

// ─────────────────────────────────────────────
// SERVER
// ─────────────────────────────────────────────

type Server struct {
	flight.BaseFlightServer

	allocator memory.Allocator
	db        *connDB
	plans     *planCache
}

func NewServer(dbPath string) (*Server, error) {
	db, err := newConnDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &Server{
		allocator: memory.NewGoAllocator(),
		db:        db,
		plans:     newPlanCache(),
	}, nil
}

// ─────────────────────────────────────────────
// INTERNAL HELPERS
// ─────────────────────────────────────────────

// planFromSQL validates sql, registers it in the plan cache, and returns the
// canonical ExecutionPlan.  Schema derivation happens separately so that
// errors are propagated to the caller rather than silently deferred.
func (s *Server) planFromSQL(sql string) (*ExecutionPlan, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument, "empty SQL")
	}
	if strings.HasPrefix(sql, "{") {
		return nil, status.Error(codes.InvalidArgument,
			"FlightDescriptor.Cmd must be raw SQL, not JSON")
	}
	return s.plans.getOrCreate(sql), nil
}

// getPlanFromTicket decodes ticket bytes and resolves the cached plan.
func (s *Server) getPlanFromTicket(t *flight.Ticket) (*ExecutionPlan, error) {
	et, err := decodeTicket(t.Ticket)
	if err != nil {
		return nil, err
	}
	plan, ok := s.plans.get(et.PlanID)
	if !ok {
		return nil, status.Errorf(codes.NotFound,
			"plan %q not found; call GetFlightInfo first", et.PlanID)
	}
	return plan, nil
}

// buildTicket returns the canonical flight.Ticket for plan.
func (s *Server) buildTicket(plan *ExecutionPlan) *flight.Ticket {
	return &flight.Ticket{Ticket: encodeTicket(execTicket{PlanID: plan.ID})}
}

// buildFlightInfo constructs a FlightInfo for plan.
// schemaBytes must already be available; call schemaForPlan first.
func (s *Server) buildFlightInfo(plan *ExecutionPlan, schemaBytes []byte) *flight.FlightInfo {
	return &flight.FlightInfo{
		Schema: schemaBytes,
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(plan.SQL),
		},
		Endpoint:     []*flight.FlightEndpoint{{Ticket: s.buildTicket(plan)}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
}

// schemaForPlan returns the serialised Arrow IPC schema for plan, deriving
// and caching it on the first successful call.
//
// Design properties:
//
//   - Errors are NOT permanently cached. A transient failure (context
//     cancellation, resource pressure) will be retried on the next call.
//     Permanent failures (SQL syntax errors) recur deterministically.
//
//   - Derivation is bounded by min(ctx.Deadline, schemaDerivationTimeout),
//     preventing expensive query-planning work from stalling the server.
//
//   - The write lock is held only during derivation; all subsequent calls
//     take the cheap read-lock fast path.
func (s *Server) schemaForPlan(ctx context.Context, plan *ExecutionPlan) ([]byte, error) {
	// Fast path: already cached.
	if b := plan.cachedSchema(); b != nil {
		return b, nil
	}

	// Slow path: derive schema under write lock.
	plan.schemaMu.Lock()
	defer plan.schemaMu.Unlock()

	// Re-check under write lock (another goroutine may have populated it).
	if plan.schemaBytes != nil {
		return plan.schemaBytes, nil
	}

	// Bound derivation time independently of the caller's deadline.
	deriveCtx, cancel := context.WithTimeout(ctx, schemaDerivationTimeout)
	defer cancel()

	schema, err := s.deriveSchema(deriveCtx, plan.SQL)
	if err != nil {
		return nil, err // not stored; caller can retry
	}

	plan.schemaBytes = flight.SerializeSchema(schema, s.allocator)
	return plan.schemaBytes, nil
}

// deriveSchema executes a zero-row probe query to obtain the result schema.
// It opens its own connection and closes it before returning.
//
// Cleanup order (LIFO defers): reader.Release → stmt.Close → conn.Close.
func (s *Server) deriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	probeSQL := fmt.Sprintf("SELECT * FROM (%s) __schema_probe LIMIT 0", sql)

	conn, err := s.db.conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("derive schema: open connection: %w", err)
	}
	defer conn.Close() //nolint:errcheck — best-effort; error not actionable here

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("derive schema: new statement: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	if err := stmt.SetSqlQuery(probeSQL); err != nil {
		return nil, fmt.Errorf("derive schema: set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("derive schema: execute: %w", err)
	}
	// reader.Release must precede stmt.Close (triggered by deferred stmt.Close above).
	schema := reader.Schema()
	reader.Release()

	return schema, nil
}

// ─────────────────────────────────────────────
// EXECUTION ENGINE
// ─────────────────────────────────────────────

// execute opens a fresh connection, runs plan.SQL, and returns the record
// reader together with a deterministic cleanup function.
//
// The caller MUST call cleanup() exactly once, even if they do not consume
// all records.  cleanup is idempotent (sync.Once-guarded).
//
// ADBC lifecycle contract
// ──────────────────────────────────────────────────────────────────────
// The RecordReader returned by ExecuteQuery may hold references into
// statement-owned buffers.  The statement MUST remain open until the
// reader is fully consumed.  Cleanup order is strictly:
//
//  1. reader.Release() — release reader (frees record references)
//  2. stmt.Close()     — close statement (frees driver-side buffers)
//  3. conn.Close()     — close connection
//
// ──────────────────────────────────────────────────────────────────────
func (s *Server) execute(ctx context.Context, plan *ExecutionPlan) (array.RecordReader, func(), error) {
	conn, err := s.db.conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("execute: open connection: %w", err)
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("execute: new statement: %w", err)
	}

	if err := stmt.SetSqlQuery(plan.SQL); err != nil {
		_ = stmt.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("execute: set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("execute: run query: %w", err)
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			reader.Release() // 1. reader before statement
			_ = stmt.Close() // 2. statement before connection
			_ = conn.Close() // 3. connection last
		})
	}
	return reader, cleanup, nil
}

// streamRecords is the shared inner loop for DoGet and DoExchange.
// It writes every record from reader into w and returns any reader error.
func streamRecords(reader array.RecordReader, w *flight.Writer) error {
	for reader.Next() {
		if err := w.Write(reader.RecordBatch()); err != nil {
			return err
		}
	}
	return reader.Err()
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
//
// Only plans whose schema has already been cached are emitted.
// This prevents ListFlights from triggering schema-derivation work across
// potentially stale or expensive plans in the cache.
// ─────────────────────────────────────────────

func (s *Server) ListFlights(_ *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	var retErr error

	s.plans.m.Range(func(_, value any) bool {
		plan := value.(*ExecutionPlan)

		schemaBytes := plan.cachedSchema() // non-blocking
		if schemaBytes == nil {
			return true // schema not yet available; skip silently
		}

		if err := stream.Send(s.buildFlightInfo(plan, schemaBytes)); err != nil {
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
// FlightDescriptor.Cmd = raw SQL bytes.
// ─────────────────────────────────────────────

func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	sql := strings.TrimSpace(string(desc.Cmd))
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument,
			"FlightDescriptor.Cmd must contain SQL")
	}

	plan, err := s.planFromSQL(sql)
	if err != nil {
		return nil, err
	}

	schemaBytes, err := s.schemaForPlan(ctx, plan)
	if err != nil {
		return nil, err
	}

	return s.buildFlightInfo(plan, schemaBytes), nil
}

// ─────────────────────────────────────────────
// GET SCHEMA
//
// FlightDescriptor.Cmd = raw SQL bytes (identical contract to GetFlightInfo).
// ─────────────────────────────────────────────

func (s *Server) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	sql := strings.TrimSpace(string(desc.Cmd))
	if sql == "" {
		return nil, status.Error(codes.InvalidArgument,
			"FlightDescriptor.Cmd must contain SQL")
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
// Ticket.Ticket = JSON {"plan_id":"<hex>"}.
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

	return streamRecords(reader, w)
}

// ─────────────────────────────────────────────
// DO EXCHANGE
//
// The first FlightData message MUST carry a FlightDescriptor with
// Cmd = raw SQL bytes.  This is the standard Flight initiation pattern and
// is supported by all spec-compliant Arrow Flight clients (Go, Python, Java,
// C++).
//
// No AppMetadata dependency.
// No prior GetFlightInfo round-trip required from the client.
//
// Execution and streaming are handled by the same execute() + streamRecords()
// functions used by DoGet; only plan resolution differs.
// ─────────────────────────────────────────────

func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument,
			"DoExchange: recv first message: %v", err)
	}

	if first.FlightDescriptor == nil {
		return status.Error(codes.InvalidArgument,
			"DoExchange: first FlightData must carry a FlightDescriptor")
	}

	sql := strings.TrimSpace(string(first.FlightDescriptor.Cmd))
	if sql == "" {
		return status.Error(codes.InvalidArgument,
			"DoExchange: FlightDescriptor.Cmd must contain SQL")
	}

	plan, err := s.planFromSQL(sql)
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

	return streamRecords(reader, w)
}

// ─────────────────────────────────────────────
// ACTIONS
// ─────────────────────────────────────────────

func (s *Server) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	for _, a := range []flight.ActionType{
		{
			Type:        "CreatePreparedStatement",
			Description: `Register SQL, derive schema eagerly, return plan ticket {"plan_id":"..."}`,
		},
		{
			Type:        "ClosePreparedStatement",
			Description: "Acknowledge ticket closure (no-op; plans are content-addressed and immutable)",
		},
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
			return status.Errorf(codes.InvalidArgument,
				"CreatePreparedStatement: invalid body: %v", err)
		}
		if strings.TrimSpace(req.SQL) == "" {
			return status.Error(codes.InvalidArgument,
				"CreatePreparedStatement: sql must not be empty")
		}

		plan, err := s.planFromSQL(req.SQL)
		if err != nil {
			return err
		}

		// Eagerly derive and cache schema so the plan is fully usable
		// in GetFlightInfo / DoGet without a separate round-trip.
		if _, err := s.schemaForPlan(stream.Context(), plan); err != nil {
			return status.Errorf(codes.Internal,
				"CreatePreparedStatement: schema derivation failed: %v", err)
		}

		return stream.Send(&flight.Result{
			Body: encodeTicket(execTicket{PlanID: plan.ID}),
		})

	case "ClosePreparedStatement":
		// Plans are content-addressed and immutable; there is no server-side
		// resource to release.  Validate the body for protocol correctness
		// and acknowledge.
		var req struct {
			PlanID string `json:"plan_id"`
		}
		_ = json.Unmarshal(action.Body, &req)
		return stream.Send(&flight.Result{Body: []byte(`{"ok":true}`)})

	default:
		return status.Errorf(codes.Unimplemented, "unknown action %q", action.Type)
	}
}

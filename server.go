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
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const schemaDerivationTimeout = 30 * time.Second

// ─────────────────────────────────────────────
// SQL EXTRACTION (KEY FIX)
// ─────────────────────────────────────────────

func extractSQL(desc *flight.FlightDescriptor) (string, error) {
	if desc == nil || len(desc.Cmd) == 0 {
		return "", status.Error(codes.InvalidArgument, "missing command")
	}

	// ─────────────────────────────────────────────
	// Try Flight SQL protobuf decode (ADBC path)
	// ─────────────────────────────────────────────
	cmd := &pb.CommandStatementQuery{}
	var anyMsg anypb.Any

	// ADBC/FlightSQL commonly wraps commands in google.protobuf.Any.
	if err := proto.Unmarshal(desc.Cmd, &anyMsg); err == nil {
		if err := anyMsg.UnmarshalTo(cmd); err == nil {
			sql := strings.TrimSpace(cmd.GetQuery())
			if sql == "" {
				return "", status.Error(codes.InvalidArgument, "empty SQL in Flight SQL command")
			}
			return sql, nil
		}
	}

	// Some clients may send CommandStatementQuery bytes directly.
	if err := proto.Unmarshal(desc.Cmd, cmd); err == nil {
		sql := strings.TrimSpace(cmd.GetQuery())
		if sql == "" {
			return "", status.Error(codes.InvalidArgument, "empty SQL in Flight SQL command")
		}
		return sql, nil
	}

	// ─────────────────────────────────────────────
	// Fallback: raw SQL (native Porter path)
	// ─────────────────────────────────────────────
	sql := strings.TrimSpace(string(desc.Cmd))

	// guard against protobuf Any payload misinterpreted as SQL
	if strings.Contains(sql, "type.googleapis.com/") {
		return "", status.Error(codes.InvalidArgument,
			"received protobuf Flight SQL command but failed to decode")
	}

	if sql == "" {
		return "", status.Error(codes.InvalidArgument, "empty SQL")
	}

	return sql, nil
}

// ─────────────────────────────────────────────
// PLAN
// ─────────────────────────────────────────────

type ExecutionPlan struct {
	ID  string
	SQL string

	schemaMu    sync.RWMutex
	schemaBytes []byte
}

func (p *ExecutionPlan) cachedSchema() []byte {
	p.schemaMu.RLock()
	defer p.schemaMu.RUnlock()
	return p.schemaBytes
}

// ─────────────────────────────────────────────
// CACHE
// ─────────────────────────────────────────────

type planCache struct {
	m sync.Map
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

func (c *planCache) delete(id string) {
	c.m.Delete(id)
}

func hashSQL(sql string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(sql)))
	return hex.EncodeToString(h[:])
}

// ─────────────────────────────────────────────
// TICKET
// ─────────────────────────────────────────────

type execTicket struct {
	PlanID string `json:"plan_id"`
}

func encodeTicket(t execTicket) []byte {
	b, _ := json.Marshal(t)
	return b
}

func decodeTicket(b []byte) (execTicket, error) {
	var t execTicket
	if err := json.Unmarshal(b, &t); err != nil {
		return t, status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}
	if t.PlanID == "" {
		return t, status.Error(codes.InvalidArgument, "missing plan_id")
	}
	return t, nil
}

// ─────────────────────────────────────────────
// DB
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
		return nil, err
	}
	return &connDB{db: db}, nil
}

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
// SCHEMA
// ─────────────────────────────────────────────

func (s *Server) schemaForPlan(ctx context.Context, plan *ExecutionPlan) ([]byte, error) {
	if b := plan.cachedSchema(); b != nil {
		return b, nil
	}

	plan.schemaMu.Lock()
	defer plan.schemaMu.Unlock()

	if plan.schemaBytes != nil {
		return plan.schemaBytes, nil
	}

	ctx, cancel := context.WithTimeout(ctx, schemaDerivationTimeout)
	defer cancel()

	schema, err := s.deriveSchema(ctx, plan.SQL)
	if err != nil {
		return nil, err
	}

	plan.schemaBytes = flight.SerializeSchema(schema, s.allocator)
	return plan.schemaBytes, nil
}

func (s *Server) deriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	// FIX: avoid wrapping broken SQL
	probeSQL := fmt.Sprintf("SELECT * FROM (%s) t LIMIT 0", sql)

	conn, err := s.db.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(probeSQL); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}

	schema := reader.Schema()
	reader.Release()
	return schema, nil
}

// ─────────────────────────────────────────────
// EXECUTION
// ─────────────────────────────────────────────

func (s *Server) execute(ctx context.Context, plan *ExecutionPlan) (array.RecordReader, func(), error) {
	conn, err := s.db.conn(ctx)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	if err := stmt.SetSqlQuery(plan.SQL); err != nil {
		stmt.Close()
		conn.Close()
		return nil, nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		conn.Close()
		return nil, nil, err
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			reader.Release()
			stmt.Close()
			conn.Close()
		})
	}

	return reader, cleanup, nil
}

func streamRecords(reader array.RecordReader, w *flight.Writer) error {
	for reader.Next() {
		if err := w.Write(reader.RecordBatch()); err != nil {
			return err
		}
	}
	return reader.Err()
}

func extractCreatePreparedSQL(body []byte) (string, bool, error) {
	var native struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &native); err == nil {
		sql := strings.TrimSpace(native.SQL)
		if sql == "" {
			return "", false, status.Error(codes.InvalidArgument, "missing sql")
		}
		return sql, false, nil
	}

	var anyMsg anypb.Any
	if err := proto.Unmarshal(body, &anyMsg); err != nil {
		return "", false, status.Error(codes.InvalidArgument, "invalid CreatePreparedStatement body")
	}
	var req pb.ActionCreatePreparedStatementRequest
	if err := anyMsg.UnmarshalTo(&req); err != nil {
		return "", false, status.Error(codes.InvalidArgument, "invalid CreatePreparedStatement request")
	}
	sql := strings.TrimSpace(req.GetQuery())
	if sql == "" {
		return "", false, status.Error(codes.InvalidArgument, "missing query")
	}
	return sql, true, nil
}

func extractClosePreparedPlanID(body []byte) (string, error) {
	var native struct {
		PlanID string `json:"plan_id"`
	}
	if err := json.Unmarshal(body, &native); err == nil {
		id := strings.TrimSpace(native.PlanID)
		if id == "" {
			return "", status.Error(codes.InvalidArgument, "missing plan_id")
		}
		return id, nil
	}

	var anyMsg anypb.Any
	if err := proto.Unmarshal(body, &anyMsg); err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid ClosePreparedStatement body")
	}
	var req pb.ActionClosePreparedStatementRequest
	if err := anyMsg.UnmarshalTo(&req); err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid ClosePreparedStatement request")
	}

	// ADBC/FlightSQL handles are opaque; this server encodes JSON tickets.
	if t, err := decodeTicket(req.GetPreparedStatementHandle()); err == nil {
		return t.PlanID, nil
	}

	id := strings.TrimSpace(string(req.GetPreparedStatementHandle()))
	if id == "" {
		return "", status.Error(codes.InvalidArgument, "missing prepared statement handle")
	}
	return id, nil
}

// ─────────────────────────────────────────────
// RPCS
// ─────────────────────────────────────────────

func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	sql, err := extractSQL(desc)
	if err != nil {
		return nil, err
	}

	plan := s.plans.getOrCreate(sql)

	schema, err := s.schemaForPlan(ctx, plan)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Schema: schema,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: encodeTicket(execTicket{PlanID: plan.ID})}},
		},
	}, nil
}

func (s *Server) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	sql, err := extractSQL(desc)
	if err != nil {
		return nil, err
	}

	plan := s.plans.getOrCreate(sql)

	schema, err := s.schemaForPlan(ctx, plan)
	if err != nil {
		return nil, err
	}

	return &flight.SchemaResult{Schema: schema}, nil
}

func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	t, err := decodeTicket(ticket.Ticket)
	if err != nil {
		return err
	}

	plan, ok := s.plans.get(t.PlanID)
	if !ok {
		return status.Error(codes.NotFound, "plan not found")
	}

	reader, cleanup, err := s.execute(stream.Context(), plan)
	if err != nil {
		return err
	}
	defer cleanup()

	w := flight.NewRecordWriter(stream,
		ipc.WithSchema(reader.Schema()),
	)
	defer w.Close()

	return streamRecords(reader, w)
}

func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}

	sql, err := extractSQL(first.FlightDescriptor)
	if err != nil {
		return err
	}

	plan := s.plans.getOrCreate(sql)

	reader, cleanup, err := s.execute(stream.Context(), plan)
	if err != nil {
		return err
	}
	defer cleanup()

	w := flight.NewRecordWriter(stream,
		ipc.WithSchema(reader.Schema()),
	)
	defer w.Close()

	return streamRecords(reader, w)
}

func (s *Server) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	actions := []*flight.ActionType{
		{Type: "CreatePreparedStatement", Description: "Create a prepared statement"},
		{Type: "ClosePreparedStatement", Description: "Close a prepared statement"},
	}
	for _, a := range actions {
		if err := stream.Send(a); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.GetType() {
	case "CreatePreparedStatement":
		sql, isProtoRequest, err := extractCreatePreparedSQL(action.GetBody())
		if err != nil {
			return err
		}

		plan := s.plans.getOrCreate(sql)
		ticket := encodeTicket(execTicket{PlanID: plan.ID})

		if !isProtoRequest {
			// Native Porter client path: return raw JSON ticket bytes directly.
			return stream.Send(&flight.Result{Body: ticket})
		}

		// ADBC/FlightSQL path: return Any(ActionCreatePreparedStatementResult).
		anyRes, err := anypb.New(&pb.ActionCreatePreparedStatementResult{
			PreparedStatementHandle: ticket,
		})
		if err != nil {
			return err
		}
		b, err := proto.Marshal(anyRes)
		if err != nil {
			return err
		}
		return stream.Send(&flight.Result{Body: b})

	case "ClosePreparedStatement":
		planID, err := extractClosePreparedPlanID(action.GetBody())
		if err != nil {
			return err
		}
		s.plans.delete(planID)
		return nil

	default:
		return status.Errorf(codes.Unimplemented, "unknown action type: %s", action.GetType())
	}
}

func (s *Server) Handshake(stream flight.FlightService_HandshakeServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		stream.Send(&flight.HandshakeResponse{Payload: req.Payload})
	}
}

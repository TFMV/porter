package flightsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//
// ─────────────────────────────────────────────
// TICKET
// ─────────────────────────────────────────────
//

type ExecTicket struct {
	Type       string `json:"type"`
	SQL        string `json:"sql,omitempty"`
	PreparedID string `json:"prepared_id,omitempty"`
	QueryID    string `json:"query_id"`
}

func encodeTicket(t ExecTicket) []byte {
	b, _ := json.Marshal(t)
	return b
}

func decodeTicket(b []byte) (ExecTicket, error) {
	var t ExecTicket
	err := json.Unmarshal(b, &t)
	return t, err
}

func hashSQL(sql string) string {
	h := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(h[:])
}

//
// ─────────────────────────────────────────────
// CONNECTION POOL
// ─────────────────────────────────────────────
//

type connPool struct {
	mu    sync.Mutex
	conns []adbc.Connection
	drv   adbc.Driver
	path  string
}

func newPool(path string) *connPool {
	if path == "" {
		path = ":memory:"
	}

	return &connPool{
		drv:   drivermgr.Driver{},
		conns: make([]adbc.Connection, 0, 8),
		path:  path,
	}
}

func (p *connPool) get(ctx context.Context) (adbc.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if n := len(p.conns); n > 0 {
		c := p.conns[n-1]
		p.conns = p.conns[:n-1]
		return c, nil
	}

	db, err := p.drv.NewDatabase(map[string]string{
		"driver": "duckdb",
		"path":   p.path,
	})
	if err != nil {
		return nil, err
	}

	return db.Open(ctx)
}

func (p *connPool) put(c adbc.Connection) {
	p.mu.Lock()
	p.conns = append(p.conns, c)
	p.mu.Unlock()
}

//
// ─────────────────────────────────────────────
// SERVER
// ─────────────────────────────────────────────
//

type Server struct {
	flight.BaseFlightServer

	allocator memory.Allocator
	pool      *connPool
	prepared  sync.Map

	dbPath string
}

func NewServer(dbPath string) *Server {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	return &Server{
		allocator: memory.NewGoAllocator(),
		pool:      newPool(dbPath),
		dbPath:    dbPath,
	}
}

//
// ─────────────────────────────────────────────
// HANDSHAKE
// ─────────────────────────────────────────────
//

func (s *Server) Handshake(stream flight.FlightService_HandshakeServer) error {
	ctx := stream.Context()

	for {
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, err.Error())
		}

		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return status.Errorf(codes.Internal, "handshake recv failed: %v", err)
		}

		if len(req.Payload) == 0 {
			return status.Error(codes.Unauthenticated, "empty handshake payload")
		}

		if err := stream.Send(&flight.HandshakeResponse{
			Payload: req.Payload,
		}); err != nil {
			return status.Errorf(codes.Internal, "handshake send failed: %v", err)
		}
	}
}

//
// ─────────────────────────────────────────────
// GET SCHEMA
// ─────────────────────────────────────────────
//

func (s *Server) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	t, err := decodeTicket(desc.Cmd)
	if err != nil {
		return nil, err
	}

	sql, err := s.resolveSQL(t)
	if err != nil {
		return nil, err
	}

	conn, err := s.pool.get(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.put(conn)

	stmt, _ := conn.NewStatement()
	defer stmt.Close()

	_ = stmt.SetSqlQuery(sql)

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	return &flight.SchemaResult{
		Schema: serializeSchema(reader.Schema()),
	}, nil
}

//
// ─────────────────────────────────────────────
// LIST ACTIONS
// ─────────────────────────────────────────────
//

func (s *Server) ListActions(
	_ *flight.Empty,
	stream flight.FlightService_ListActionsServer,
) error {

	actions := []*flight.ActionType{
		{Type: "CreatePreparedStatement"},
		{Type: "ClosePreparedStatement"},
	}

	for _, a := range actions {
		if err := stream.Send(a); err != nil {
			return err
		}
	}

	return nil
}

//
// ─────────────────────────────────────────────
// LIST FLIGHTS
// ─────────────────────────────────────────────
//

func (s *Server) ListFlights(
	_ *flight.Criteria,
	stream flight.FlightService_ListFlightsServer,
) error {
	return nil
}

//
// ─────────────────────────────────────────────
// GET FLIGHT INFO
// ─────────────────────────────────────────────
//

func (s *Server) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	t, err := decodeTicket(desc.Cmd)
	if err != nil {
		return nil, err
	}

	sql, err := s.resolveSQL(t)
	if err != nil {
		return nil, err
	}

	conn, err := s.pool.get(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.put(conn)

	stmt, _ := conn.NewStatement()
	defer stmt.Close()

	_ = stmt.SetSqlQuery(sql)

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	schema := serializeSchema(reader.Schema())
	queryID := hashSQL(sql) + fmt.Sprintf("-%d", time.Now().UnixNano())

	return &flight.FlightInfo{
		Schema: schema,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: encodeTicket(ExecTicket{
						Type:    t.Type,
						SQL:     sql,
						QueryID: queryID,
					}),
				},
			},
		},
	}, nil
}

//
// ─────────────────────────────────────────────
// DO EXCHANGE
// ─────────────────────────────────────────────
//

func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return status.Errorf(codes.Internal, "recv failed: %v", err)
	}

	if len(first.DataHeader) == 0 {
		return status.Error(codes.InvalidArgument, "missing ticket in DataHeader")
	}

	ticket, err := decodeTicket(first.DataHeader)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	sql, err := s.resolveSQL(ticket)
	if err != nil {
		return err
	}

	var lastParam arrow.Record

	for {
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, err.Error())
		}

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if len(msg.DataBody) == 0 {
			continue
		}

		rdr, err := ipc.NewReader(bytes.NewReader(msg.DataBody),
			ipc.WithAllocator(s.allocator),
		)
		if err != nil {
			return err
		}

		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain()

			if lastParam != nil {
				lastParam.Release()
			}
			lastParam = rec
		}

		rdr.Release()
	}

	_ = lastParam

	conn, err := s.pool.get(ctx)
	if err != nil {
		return err
	}
	defer s.pool.put(conn)

	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	_ = stmt.SetSqlQuery(sql)

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	var buf bytes.Buffer

	w := ipc.NewWriter(&buf,
		ipc.WithSchema(reader.Schema()),
		ipc.WithAllocator(s.allocator),
	)
	defer w.Close()

	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, err.Error())
		}

		if err := w.Write(reader.Record()); err != nil {
			return err
		}
	}

	if err := w.Close(); err != nil {
		return err
	}

	return stream.Send(&flight.FlightData{
		DataBody: buf.Bytes(),
	})
}

//
// ─────────────────────────────────────────────
// DO GET
// ─────────────────────────────────────────────
//

func (s *Server) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {

	t, err := decodeTicket(ticket.Ticket)
	if err != nil {
		return err
	}

	conn, err := s.pool.get(stream.Context())
	if err != nil {
		return err
	}
	defer s.pool.put(conn)

	stmt, _ := conn.NewStatement()
	defer stmt.Close()

	_ = stmt.SetSqlQuery(t.SQL)

	reader, _, err := stmt.ExecuteQuery(stream.Context())
	if err != nil {
		return err
	}
	defer reader.Release()

	writer := flight.NewRecordWriter(stream,
		ipc.WithAllocator(s.allocator),
		ipc.WithSchema(reader.Schema()),
	)
	defer writer.Close()

	for reader.Next() {
		if err := writer.Write(reader.Record()); err != nil {
			return err
		}
	}

	return reader.Err()
}

//
// ─────────────────────────────────────────────
// DO ACTION
// ─────────────────────────────────────────────
//

func (s *Server) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {

	switch action.Type {

	case "CreatePreparedStatement":
		sql := string(action.Body)
		id := hashSQL(sql)

		s.prepared.Store(id, sql)

		return stream.Send(&flight.Result{
			Body: encodeTicket(ExecTicket{
				Type:       "prepared",
				PreparedID: id,
			}),
		})

	case "ClosePreparedStatement":
		var req struct {
			PreparedID string `json:"prepared_id"`
		}
		_ = json.Unmarshal(action.Body, &req)

		s.prepared.Delete(req.PreparedID)

		return stream.Send(&flight.Result{
			Body: []byte(`{"ok":true}`),
		})
	}

	return status.Error(codes.InvalidArgument, "unknown action")
}

//
// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
//

func (s *Server) resolveSQL(t ExecTicket) (string, error) {
	switch t.Type {
	case "sql":
		return t.SQL, nil
	case "prepared":
		v, ok := s.prepared.Load(t.PreparedID)
		if !ok {
			return "", status.Error(codes.NotFound, "missing prepared")
		}
		return v.(string), nil
	}
	return "", status.Error(codes.InvalidArgument, "bad ticket type")
}

func serializeSchema(schema *arrow.Schema) []byte {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	_ = w.Close()
	return buf.Bytes()
}

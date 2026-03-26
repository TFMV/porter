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

	QueryID string `json:"query_id"`
	Shard   int    `json:"shard"`
	Total   int    `json:"total"`
}

func encodeTicket(t ExecTicket) []byte {
	b, _ := json.Marshal(t)
	return b
}

func decodeTicket(b []byte) (ExecTicket, error) {
	var t ExecTicket
	return t, json.Unmarshal(b, &t)
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
}

func newPool() *connPool {
	return &connPool{
		drv:   drivermgr.Driver{},
		conns: make([]adbc.Connection, 0, 8),
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
		"path":   ":memory:",
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

	shards int
}

func NewServer(shards int) *Server {
	if shards <= 0 {
		shards = 1
	}

	return &Server{
		allocator: memory.NewGoAllocator(),
		pool:      newPool(),
		shards:    shards,
	}
}

// Handshake implements a streaming authentication handshake.
// It supports multi-message negotiation, respects context cancellation,
// and returns structured errors suitable for production observability.
func (s *Server) Handshake(stream flight.FlightService_HandshakeServer) error {
	ctx := stream.Context()

	for {
		// Respect cancellation / deadline early (critical for gRPC hygiene)
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, err.Error())
		}

		req, err := stream.Recv()
		if err != nil {
			// Proper EOF handling = clean stream termination
			if errors.Is(err, io.EOF) {
				return nil
			}

			// Preserve transport semantics while wrapping for observability
			return status.Errorf(codes.Internal, "handshake recv failed: %v", err)
		}

		// Defensive check: empty payload should never silently pass
		if len(req.Payload) == 0 {
			return status.Error(codes.Unauthenticated, "empty handshake payload")
		}

		// ─────────────────────────────────────────────
		// AUTH / TOKEN PROCESSING HOOK
		// ─────────────────────────────────────────────
		token := req.Payload

		// (placeholder for real validation)
		// if err := s.auth.Validate(token); err != nil {
		//     return status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		// }

		resp := &flight.HandshakeResponse{
			Payload: token,
		}

		if err := stream.Send(resp); err != nil {
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
		{Type: "CreatePreparedStatement", Description: "create prepared statement"},
		{Type: "ClosePreparedStatement", Description: "close prepared statement"},
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
// LIST FLIGHTS (optional but required by client)
// ─────────────────────────────────────────────
//

func (s *Server) ListFlights(
	_ *flight.Criteria,
	stream flight.FlightService_ListFlightsServer,
) error {
	// minimal implementation
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

	var endpoints []*flight.FlightEndpoint

	for i := 0; i < s.shards; i++ {
		endpoints = append(endpoints, &flight.FlightEndpoint{
			Ticket: &flight.Ticket{
				Ticket: encodeTicket(ExecTicket{
					Type:    t.Type,
					SQL:     sql,
					QueryID: queryID,
					Shard:   i,
					Total:   s.shards,
				}),
			},
		})
	}

	return &flight.FlightInfo{
		Schema:   schema,
		Endpoint: endpoints,
	}, nil
}

//
// ─────────────────────────────────────────────
// DOGET
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
// DOEXCHANGE
// ─────────────────────────────────────────────
//

func (s *Server) DoExchange(stream flight.FlightService_DoExchangeServer) error {

	ctx := stream.Context()

	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		t, err := decodeTicket(in.DataHeader)
		if err != nil {
			return err
		}

		var wg sync.WaitGroup
		errCh := make(chan error, s.shards)

		for i := 0; i < s.shards; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				conn, err := s.pool.get(ctx)
				if err != nil {
					errCh <- err
					return
				}
				defer s.pool.put(conn)

				stmt, _ := conn.NewStatement()
				defer stmt.Close()

				_ = stmt.SetSqlQuery(t.SQL)

				reader, _, err := stmt.ExecuteQuery(ctx)
				if err != nil {
					errCh <- err
					return
				}
				defer reader.Release()

				for reader.Next() {
					data, _ := encodeRecord(reader.Record())

					if err := stream.Send(&flight.FlightData{
						DataBody: data,
					}); err != nil {
						errCh <- err
						return
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for e := range errCh {
			if e != nil {
				return e
			}
		}
	}
}

//
// ─────────────────────────────────────────────
// DOACTION
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

func encodeRecord(rec arrow.Record) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	defer w.Close()

	if err := w.Write(rec); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

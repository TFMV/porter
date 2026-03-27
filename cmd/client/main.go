// Package main provides a production-grade Apache Arrow Flight SQL client that
// exercises the full Flight wire protocol:
//
//   - GetFlightInfo  (sql + prepared paths)
//   - DoGet          (single-ticket and fanout/parallel)
//   - DoExchange     (bidirectional streaming)
//   - DoAction       (CreatePreparedStatement / ClosePreparedStatement / ListActions)
//   - ListFlights    (enumerate available datasets)
//   - GetSchema      (schema-only probe, no data transfer)
//   - Handshake      (token-based auth)
//
// Every public method is context-aware, returns structured errors, and is safe
// for concurrent use.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ─────────────────────────────────────────────────────────────────────────────
// Protocol  (must stay in sync with server-side ExecTicket / TicketType)
// ─────────────────────────────────────────────────────────────────────────────

type TicketType string

const (
	TicketTypeSQL      TicketType = "sql"
	TicketTypePrepared TicketType = "prepared"
)

// ExecTicket is the JSON payload carried in FlightDescriptor.Cmd,
// Ticket.Ticket, and FlightData.DataHeader.
type ExecTicket struct {
	Type       TicketType `json:"type"`
	SQL        string     `json:"sql,omitempty"`
	PreparedID string     `json:"prepared_id,omitempty"`
}

func marshalTicket(t ExecTicket) ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("marshalTicket: %w", err)
	}
	return b, nil
}

func unmarshalTicket(b []byte) (ExecTicket, error) {
	var t ExecTicket
	if err := json.Unmarshal(b, &t); err != nil {
		return t, fmt.Errorf("unmarshalTicket: %w", err)
	}
	return t, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ClientConfig
// ─────────────────────────────────────────────────────────────────────────────

// ClientConfig controls client-wide behaviour.
type ClientConfig struct {
	// Target is the gRPC dial target, e.g. "localhost:50051".
	Target string
	// DefaultTenant is sent as x-tenant-id on every call.
	DefaultTenant string
	// Token is an optional bearer token sent via Handshake and then attached
	// to every subsequent call as x-auth-token.
	Token string
	// Logger receives structured events.  Defaults to slog.Default().
	Logger *slog.Logger
	// Allocator is used when decoding Arrow IPC data.
	Allocator memory.Allocator
	// DialTimeout caps the gRPC dial phase.
	DialTimeout time.Duration
}

func (c *ClientConfig) setDefaults() {
	if c.DefaultTenant == "" {
		c.DefaultTenant = "default"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Allocator == nil {
		c.Allocator = memory.NewGoAllocator()
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RecordBatch result
// ─────────────────────────────────────────────────────────────────────────────

// QueryResult holds all record batches collected from a single DoGet stream.
// Call Release when done to free Arrow memory.
type QueryResult struct {
	Schema  *arrow.Schema
	Batches []arrow.RecordBatch
}

// Release frees all held Arrow memory.
func (r *QueryResult) Release() {
	for _, b := range r.Batches {
		b.Release()
	}
}

// TotalRows returns the sum of rows across all batches.
func (r *QueryResult) TotalRows() int64 {
	var n int64
	for _, b := range r.Batches {
		n += b.NumRows()
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// FlightSQLClient
// ─────────────────────────────────────────────────────────────────────────────

// FlightSQLClient is a production-grade Apache Arrow Flight SQL client.
// It is safe for concurrent use after construction.
type FlightSQLClient struct {
	cfg  ClientConfig
	log  *slog.Logger
	raw  flight.FlightServiceClient
	conn *grpc.ClientConn

	// authToken is populated after a successful Handshake.
	authToken string
	authMu    sync.RWMutex
}

// NewFlightSQLClient dials the server and returns a ready client.
// Call Close when done.
func NewFlightSQLClient(cfg ClientConfig) (*FlightSQLClient, error) {
	cfg.setDefaults()

	dialCtx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, cfg.Target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("FlightSQLClient: dial %q: %w", cfg.Target, err)
	}

	return &FlightSQLClient{
		cfg:  cfg,
		log:  cfg.Logger,
		raw:  flight.NewFlightServiceClient(conn),
		conn: conn,
	}, nil
}

// Close tears down the underlying gRPC connection.
func (c *FlightSQLClient) Close() error {
	return c.conn.Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Context helpers
// ─────────────────────────────────────────────────────────────────────────────

// baseCtx attaches session and tenant metadata to ctx, plus the auth token
// if one has been acquired via Handshake.
func (c *FlightSQLClient) baseCtx(ctx context.Context, sessionID string) context.Context {
	pairs := []string{
		"x-session-id", sessionID,
		"x-tenant-id", c.cfg.DefaultTenant,
	}

	c.authMu.RLock()
	tok := c.authToken
	c.authMu.RUnlock()

	if tok != "" {
		pairs = append(pairs, "x-auth-token", tok)
	}

	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

// ─────────────────────────────────────────────────────────────────────────────
// Handshake  (Flight auth)
// ─────────────────────────────────────────────────────────────────────────────

// Handshake performs the Flight auth handshake, sending token as the client
// payload.  On success the server's response token is stored and attached to
// all subsequent calls automatically.
//
// Handshake is optional; servers that don't require auth will return an empty
// token and no error.
func (c *FlightSQLClient) Handshake(ctx context.Context, token string) error {
	stream, err := c.raw.Handshake(ctx)
	if err != nil {
		return fmt.Errorf("Handshake: open stream: %w", err)
	}

	if err := stream.Send(&flight.HandshakeRequest{
		Payload: []byte(token),
	}); err != nil {
		return fmt.Errorf("Handshake: send: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("Handshake: CloseSend: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("Handshake: recv: %w", err)
	}

	if resp != nil && len(resp.Payload) > 0 {
		c.authMu.Lock()
		c.authToken = string(resp.Payload)
		c.authMu.Unlock()

		c.log.InfoContext(ctx, "handshake: token acquired")
	} else {
		c.log.InfoContext(ctx, "handshake: server returned no token (auth not required)")
	}

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// GetSchema  (schema-only probe – no data transfer)
// ─────────────────────────────────────────────────────────────────────────────

// GetSchema returns the Arrow schema for sql without executing the query or
// transferring any record batches.
func (c *FlightSQLClient) GetSchema(ctx context.Context, sessionID, sql string) (*arrow.Schema, error) {
	ctx = c.baseCtx(ctx, sessionID)

	cmd, err := marshalTicket(ExecTicket{Type: TicketTypeSQL, SQL: sql})
	if err != nil {
		return nil, err
	}

	resp, err := c.raw.GetSchema(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  cmd,
	})
	if err != nil {
		return nil, fmt.Errorf("GetSchema: %w", err)
	}

	schema, err := flight.DeserializeSchema(resp.Schema, c.cfg.Allocator)
	if err != nil {
		return nil, fmt.Errorf("GetSchema: deserialize: %w", err)
	}

	c.log.InfoContext(ctx, "GetSchema",
		slog.String("sql", sql),
		slog.Int("fields", schema.NumFields()),
	)

	return schema, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ListFlights
// ─────────────────────────────────────────────────────────────────────────────

// ListFlights enumerates all flights the server advertises that match
// criteria.  Pass an empty FlightCriteria to list everything.
func (c *FlightSQLClient) ListFlights(ctx context.Context, sessionID string, criteria *flight.Criteria) ([]*flight.FlightInfo, error) {
	ctx = c.baseCtx(ctx, sessionID)

	if criteria == nil {
		criteria = &flight.Criteria{}
	}

	stream, err := c.raw.ListFlights(ctx, criteria)
	if err != nil {
		return nil, fmt.Errorf("ListFlights: %w", err)
	}

	var infos []*flight.FlightInfo

	for {
		info, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// servers that don't implement ListFlights return Unimplemented
			if code := status.Code(err); code == codes.Unimplemented {
				c.log.InfoContext(ctx, "ListFlights: server does not implement ListFlights")
				return nil, nil
			}
			return nil, fmt.Errorf("ListFlights: recv: %w", err)
		}
		infos = append(infos, info)
	}

	c.log.InfoContext(ctx, "ListFlights", slog.Int("count", len(infos)))
	return infos, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// GetFlightInfo helpers
// ─────────────────────────────────────────────────────────────────────────────

// flightInfoForSQL calls GetFlightInfo with a sql-type command and returns all
// endpoint tickets.
func (c *FlightSQLClient) flightInfoForSQL(ctx context.Context, sql string) ([]*flight.Ticket, []byte, error) {
	cmd, err := marshalTicket(ExecTicket{Type: TicketTypeSQL, SQL: sql})
	if err != nil {
		return nil, nil, err
	}

	info, err := c.raw.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  cmd,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("GetFlightInfo(sql): %w", err)
	}

	return endpointTickets(info), info.Schema, nil
}

// flightInfoForPrepared calls GetFlightInfo for a prepared handle (raw ticket
// bytes as returned by Prepare).
func (c *FlightSQLClient) flightInfoForPrepared(ctx context.Context, prepTicket []byte) ([]*flight.Ticket, []byte, error) {
	info, err := c.raw.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  prepTicket,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("GetFlightInfo(prepared): %w", err)
	}

	return endpointTickets(info), info.Schema, nil
}

func endpointTickets(info *flight.FlightInfo) []*flight.Ticket {
	out := make([]*flight.Ticket, 0, len(info.Endpoint))
	for _, ep := range info.Endpoint {
		out = append(out, ep.Ticket)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// DoGet  – single ticket
// ─────────────────────────────────────────────────────────────────────────────

// doGet fetches a single ticket and collects all record batches.
func (c *FlightSQLClient) doGet(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	stream, err := c.raw.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("DoGet: %w", err)
	}

	reader, err := flight.NewRecordReader(stream,
		ipc.WithAllocator(c.cfg.Allocator),
	)
	if err != nil {
		return nil, fmt.Errorf("DoGet: NewRecordReader: %w", err)
	}
	defer reader.Release()

	result := &QueryResult{Schema: reader.Schema()}

	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain() // caller owns this reference
		result.Batches = append(result.Batches, rec)
	}

	if err := reader.Err(); err != nil {
		result.Release()
		return nil, fmt.Errorf("DoGet: read: %w", err)
	}

	return result, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Query  – GetFlightInfo + parallel DoGet fanout
// ─────────────────────────────────────────────────────────────────────────────

// shardResult carries the result or error from one shard.
type shardResult struct {
	idx    int
	result *QueryResult
	err    error
}

// Query executes sql, fans out across all endpoints returned by GetFlightInfo,
// and returns per-shard results in index order.
//
// Each QueryResult must be released by the caller.
func (c *FlightSQLClient) Query(ctx context.Context, sessionID, sql string) ([]*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	tickets, _, err := c.flightInfoForSQL(ctx, sql)
	if err != nil {
		return nil, err
	}

	return c.fanout(ctx, tickets)
}

// QueryPrepared executes a previously prepared statement handle across all
// endpoints and returns per-shard results.
func (c *FlightSQLClient) QueryPrepared(ctx context.Context, sessionID string, prepTicket []byte) ([]*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	tickets, _, err := c.flightInfoForPrepared(ctx, prepTicket)
	if err != nil {
		return nil, err
	}

	return c.fanout(ctx, tickets)
}

// fanout fires one DoGet goroutine per ticket and collects results in order.
func (c *FlightSQLClient) fanout(ctx context.Context, tickets []*flight.Ticket) ([]*QueryResult, error) {
	ch := make(chan shardResult, len(tickets))

	for i, t := range tickets {
		go func(idx int, ticket *flight.Ticket) {
			res, err := c.doGet(ctx, ticket)
			ch <- shardResult{idx: idx, result: res, err: err}
		}(i, t)
	}

	results := make([]*QueryResult, len(tickets))
	var errs []error

	for range tickets {
		sr := <-ch
		if sr.err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", sr.idx, sr.err))
		} else {
			results[sr.idx] = sr.result
		}
	}

	if len(errs) > 0 {
		// Release any results we did collect before returning the error.
		for _, r := range results {
			if r != nil {
				r.Release()
			}
		}
		return nil, errors.Join(errs...)
	}

	return results, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// DoExchange  – bidirectional streaming
// ─────────────────────────────────────────────────────────────────────────────

// Exchange opens a DoExchange stream, sends each sql query as a header-only
// FlightData frame, and collects IPC-encoded response batches.
//
// This is the correct path for streaming inserts, mutations with result sets,
// and any workload that requires request/response interleaving on a single RPC.
func (c *FlightSQLClient) Exchange(ctx context.Context, sessionID string, sqls ...string) ([]*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	stream, err := c.raw.DoExchange(ctx)
	if err != nil {
		return nil, fmt.Errorf("DoExchange: open: %w", err)
	}

	// -----------------------------
	// SEND REQUESTS
	// -----------------------------
	for _, sql := range sqls {
		header, err := marshalTicket(ExecTicket{
			Type: TicketTypeSQL,
			SQL:  sql,
		})
		if err != nil {
			_ = stream.CloseSend()
			return nil, err
		}

		if err := stream.Send(&flight.FlightData{
			DataHeader: header,
		}); err != nil {
			_ = stream.CloseSend()
			return nil, fmt.Errorf("DoExchange send: %w", err)
		}
	}

	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("DoExchange CloseSend: %w", err)
	}

	// -----------------------------
	// RECEIVE STREAMS (1:1 mapping)
	// -----------------------------
	var results []*QueryResult

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			for _, r := range results {
				r.Release()
			}
			return nil, fmt.Errorf("DoExchange recv: %w", err)
		}

		if len(msg.DataBody) == 0 {
			continue
		}

		reader, err := ipc.NewReader(
			bytes.NewReader(msg.DataBody),
			ipc.WithAllocator(c.cfg.Allocator),
		)
		if err != nil {
			return nil, fmt.Errorf("ipc reader: %w", err)
		}

		schema := reader.Schema()
		result := &QueryResult{Schema: schema}

		for reader.Next() {
			rec := reader.RecordBatch()
			rec.Retain()
			result.Batches = append(result.Batches, rec)
		}

		if err := reader.Err(); err != nil {
			reader.Release()
			for _, r := range results {
				r.Release()
			}
			return nil, err
		}

		reader.Release()
		results = append(results, result)
	}

	return results, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Prepared statement lifecycle
// ─────────────────────────────────────────────────────────────────────────────

// Prepare sends a CreatePreparedStatement action and returns the opaque handle
// (an ExecTicket JSON blob) that can be passed to QueryPrepared or
// ClosePrepared.
func (c *FlightSQLClient) Prepare(ctx context.Context, sessionID, sql string) ([]byte, error) {
	ctx = c.baseCtx(ctx, sessionID)

	stream, err := c.raw.DoAction(ctx, &flight.Action{
		Type: "CreatePreparedStatement",
		Body: []byte(sql),
	})
	if err != nil {
		return nil, fmt.Errorf("Prepare: DoAction: %w", err)
	}
	defer stream.CloseSend()

	var ticket []byte

	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Prepare: recv: %w", err)
		}
		ticket = res.Body
	}

	if len(ticket) == 0 {
		return nil, errors.New("Prepare: server returned empty ticket")
	}

	t, err := unmarshalTicket(ticket)
	if err != nil {
		return nil, fmt.Errorf("Prepare: bad ticket from server: %w", err)
	}

	c.log.InfoContext(ctx, "prepared statement created",
		slog.String("prepared_id", t.PreparedID),
	)

	return ticket, nil
}

// ClosePrepared sends a ClosePreparedStatement action to release server-side
// resources.  Always call this in a defer after Prepare succeeds.
func (c *FlightSQLClient) ClosePrepared(ctx context.Context, sessionID string, prepTicket []byte) error {
	ctx = c.baseCtx(ctx, sessionID)

	t, err := unmarshalTicket(prepTicket)
	if err != nil {
		return fmt.Errorf("ClosePrepared: %w", err)
	}

	body, err := json.Marshal(map[string]string{"prepared_id": t.PreparedID})
	if err != nil {
		return fmt.Errorf("ClosePrepared: marshal: %w", err)
	}

	stream, err := c.raw.DoAction(ctx, &flight.Action{
		Type: "ClosePreparedStatement",
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("ClosePrepared: DoAction: %w", err)
	}
	defer stream.CloseSend()

	// Drain the response stream.
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("ClosePrepared: recv: %w", err)
		}
	}

	c.log.InfoContext(ctx, "prepared statement closed",
		slog.String("prepared_id", t.PreparedID),
	)

	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ListActions
// ─────────────────────────────────────────────────────────────────────────────

// ListActions returns all action types the server supports.
func (c *FlightSQLClient) ListActions(ctx context.Context, sessionID string) ([]*flight.ActionType, error) {
	ctx = c.baseCtx(ctx, sessionID)

	stream, err := c.raw.ListActions(ctx, &flight.Empty{})
	if err != nil {
		return nil, fmt.Errorf("ListActions: %w", err)
	}

	var actions []*flight.ActionType

	for {
		at, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if status.Code(err) == codes.Unimplemented {
				c.log.InfoContext(ctx, "ListActions: server does not implement ListActions")
				return nil, nil
			}
			return nil, fmt.Errorf("ListActions: recv: %w", err)
		}
		actions = append(actions, at)
	}

	return actions, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// IPC decode helper
// ─────────────────────────────────────────────────────────────────────────────

// decodeIPCBatch reads the first record batch from a self-contained IPC stream
// (schema + batch + EOS) as written by encodeRecordBatch on the server.
// Returns (nil, schema, nil) for schema-only streams.
func decodeIPCBatch(data []byte, alloc memory.Allocator) (arrow.RecordBatch, *arrow.Schema, error) {
	reader, err := ipc.NewReader(
		readerFromBytes(data),
		ipc.WithAllocator(alloc),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("decodeIPCBatch: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, schema, fmt.Errorf("decodeIPCBatch: read: %w", err)
		}
		return nil, schema, nil
	}

	rec := reader.RecordBatch()
	rec.Retain()
	return rec, schema, nil
}

// byteReader wraps a []byte as an io.Reader for ipc.NewReader.
type byteReader struct {
	data []byte
	pos  int
}

func readerFromBytes(b []byte) io.Reader {
	return &byteReader{data: b}
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging helper
// ─────────────────────────────────────────────────────────────────────────────

// logResults prints a structured summary of a slice of QueryResults.
func logResults(log *slog.Logger, label string, results []*QueryResult) {
	var totalRows int64
	for _, r := range results {
		if r != nil {
			totalRows += r.TotalRows()
		}
	}

	log.Info("results",
		slog.String("label", label),
		slog.Int("shards", len(results)),
		slog.Int64("total_rows", totalRows),
	)

	for i, r := range results {
		if r == nil {
			continue
		}
		log.Info("shard",
			slog.String("label", label),
			slog.Int("shard", i),
			slog.Int64("rows", r.TotalRows()),
			slog.Int("batches", len(r.Batches)),
			slog.String("schema", r.Schema.String()),
		)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// main – exercises the full Flight spec
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// ── Dial ────────────────────────────────────────────────────────────────

	client, err := NewFlightSQLClient(ClientConfig{
		Target:        "localhost:50051",
		DefaultTenant: "default",
		Logger:        log,
		DialTimeout:   5 * time.Second,
	})
	if err != nil {
		log.Error("dial", slog.Any("err", err))
		os.Exit(1)
	}
	defer client.Close()

	ctx := context.Background()
	session := "sess-demo"

	// ── 1. Handshake ────────────────────────────────────────────────────────
	// Servers that don't require auth return an empty token; the client
	// silently skips attaching it.  This is a no-op against the reference
	// DuckDB server.

	log.Info("── 1. Handshake")
	if err := client.Handshake(ctx, "demo-token"); err != nil {
		log.Warn("handshake failed (server may not require auth)",
			slog.Any("err", err))
	}

	// ── 2. ListActions ──────────────────────────────────────────────────────

	log.Info("── 2. ListActions")
	actions, err := client.ListActions(ctx, session)
	if err != nil {
		log.Warn("ListActions", slog.Any("err", err))
	}
	for _, a := range actions {
		log.Info("action",
			slog.String("type", a.Type),
			slog.String("description", a.Description),
		)
	}

	// ── 3. ListFlights ──────────────────────────────────────────────────────

	log.Info("── 3. ListFlights")
	flights, err := client.ListFlights(ctx, session, nil)
	if err != nil {
		log.Warn("ListFlights", slog.Any("err", err))
	}
	log.Info("ListFlights", slog.Int("count", len(flights)))

	// ── 4. GetSchema  (schema-only, no data) ────────────────────────────────

	log.Info("── 4. GetSchema")
	schema, err := client.GetSchema(ctx, session, "SELECT 1 AS n, 'hello' AS s")
	if err != nil {
		log.Error("GetSchema", slog.Any("err", err))
		os.Exit(1)
	}
	log.Info("schema", slog.String("schema", schema.String()))

	// ── 5. Query  (GetFlightInfo → parallel DoGet fanout) ───────────────────

	log.Info("── 5. Query (sql fanout)")
	results, err := client.Query(ctx, session, "SELECT 1 AS n")
	if err != nil {
		log.Error("Query", slog.Any("err", err))
		os.Exit(1)
	}
	logResults(log, "sql-fanout", results)
	for _, r := range results {
		r.Release()
	}

	// ── 6. Prepared statement lifecycle ─────────────────────────────────────
	//      Prepare → GetFlightInfo → DoGet fanout → Close

	log.Info("── 6. Prepared statement")
	prepTicket, err := client.Prepare(ctx, session, "SELECT 42 AS answer")
	if err != nil {
		log.Error("Prepare", slog.Any("err", err))
		os.Exit(1)
	}

	// Always close prepared statements – use defer in real code.
	defer func() {
		if err := client.ClosePrepared(ctx, session, prepTicket); err != nil {
			log.Warn("ClosePrepared", slog.Any("err", err))
		}
	}()

	prepResults, err := client.QueryPrepared(ctx, session, prepTicket)
	if err != nil {
		log.Error("QueryPrepared", slog.Any("err", err))
		os.Exit(1)
	}
	logResults(log, "prepared-fanout", prepResults)
	for _, r := range prepResults {
		r.Release()
	}

	// ── 7. DoExchange  (bidirectional streaming, multiple queries) ──────────

	log.Info("── 7. DoExchange (bidirectional streaming)")
	exchangeResults, err := client.Exchange(ctx, session,
		"SELECT 100 AS x",
		"SELECT 200 AS y",
	)
	if err != nil {
		log.Error("Exchange", slog.Any("err", err))
		os.Exit(1)
	}
	logResults(log, "exchange", exchangeResults)
	for _, r := range exchangeResults {
		r.Release()
	}

	// ── 8. Close prepared statement (explicit, before defer fires) ───────────
	//      In production code you'd use `defer client.ClosePrepared(...)`.
	//      We exercise it explicitly here so the log shows the round-trip.

	log.Info("── done")
}

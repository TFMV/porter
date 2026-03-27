// Package main provides a production-grade Apache Arrow Flight SQL client that
// exercises the full Flight wire protocol against the DuckDB Flight SQL server:
//
//   - Handshake      (token-based auth echo)
//   - ListActions    (enumerate server capabilities)
//   - ListFlights    (enumerate cached plans)
//   - GetSchema      (schema-only probe, no data transfer)
//   - Query          (GetFlightInfo → parallel DoGet fanout)
//   - Prepare        (DoAction CreatePreparedStatement)
//   - QueryPrepared  (DoGet with plan ticket directly)
//   - ClosePrepared  (DoAction ClosePreparedStatement)
//   - Exchange       (DoExchange: GetFlightInfo → plan ticket → bidirectional stream)
//
// Server contract (must match server.go):
//
//	FlightDescriptor.Cmd  = raw SQL bytes            (GetFlightInfo, GetSchema)
//	Ticket.Ticket         = JSON {"plan_id":"..."}   (DoGet, DoExchange AppMetadata)
//	DoAction body         = JSON {"sql":"..."}       (CreatePreparedStatement)
//	DoAction body         = JSON {"plan_id":"..."}   (ClosePreparedStatement)

package main

import (
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
// Protocol types  (mirror server's wire format exactly)
// ─────────────────────────────────────────────────────────────────────────────

// planTicket is the JSON payload the server stores inside Ticket.Ticket.
// The client only ever receives or echoes these bytes; it never constructs one
// from scratch (plan IDs are server-assigned).
type planTicket struct {
	PlanID string `json:"plan_id"`
}

func decodePlanTicket(b []byte) (planTicket, error) {
	var t planTicket
	if err := json.Unmarshal(b, &t); err != nil {
		return t, fmt.Errorf("decodePlanTicket: %w", err)
	}
	if t.PlanID == "" {
		return t, errors.New("decodePlanTicket: missing plan_id")
	}
	return t, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ClientConfig
// ─────────────────────────────────────────────────────────────────────────────

// ClientConfig controls client-wide behaviour.
type ClientConfig struct {
	// Target is the gRPC dial target, e.g. "localhost:32010".
	Target string
	// DefaultTenant is sent as x-tenant-id on every call.
	DefaultTenant string
	// Token is an optional bearer token sent via Handshake and then attached
	// to every subsequent call as x-auth-token.
	Token string
	// Logger receives structured events. Defaults to slog.Default().
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
// QueryResult
// ─────────────────────────────────────────────────────────────────────────────

// QueryResult holds all record batches collected from a single stream.
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

// FlightSQLClient is a production-grade Arrow Flight SQL client.
// Safe for concurrent use after construction.
type FlightSQLClient struct {
	cfg ClientConfig
	log *slog.Logger
	raw flight.FlightServiceClient
	cc  *grpc.ClientConn

	authMu    sync.RWMutex
	authToken string // populated after Handshake succeeds
}

// NewFlightSQLClient dials the server and returns a ready client.
// Call Close when done.
func NewFlightSQLClient(cfg ClientConfig) (*FlightSQLClient, error) {
	cfg.setDefaults()

	dialCtx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	//nolint:staticcheck // grpc.DialContext is stable in the versions we target
	cc, err := grpc.DialContext(dialCtx, cfg.Target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("FlightSQLClient: dial %q: %w", cfg.Target, err)
	}

	return &FlightSQLClient{
		cfg: cfg,
		log: cfg.Logger,
		raw: flight.NewFlightServiceClient(cc),
		cc:  cc,
	}, nil
}

// Close tears down the underlying gRPC connection.
func (c *FlightSQLClient) Close() error { return c.cc.Close() }

// ─────────────────────────────────────────────────────────────────────────────
// Context helpers
// ─────────────────────────────────────────────────────────────────────────────

// baseCtx attaches session / tenant metadata and the auth token (if any).
func (c *FlightSQLClient) baseCtx(ctx context.Context, sessionID string) context.Context {
	pairs := []string{
		"x-session-id", sessionID,
		"x-tenant-id", c.cfg.DefaultTenant,
	}
	c.authMu.RLock()
	if tok := c.authToken; tok != "" {
		pairs = append(pairs, "x-auth-token", tok)
	}
	c.authMu.RUnlock()
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

// ─────────────────────────────────────────────────────────────────────────────
// Handshake
// ─────────────────────────────────────────────────────────────────────────────

// Handshake performs the Flight auth handshake. The server echoes the payload
// back unchanged; if the response is non-empty it is stored as the auth token
// and attached to all subsequent calls automatically.
func (c *FlightSQLClient) Handshake(ctx context.Context, token string) error {
	stream, err := c.raw.Handshake(ctx)
	if err != nil {
		return fmt.Errorf("Handshake: open stream: %w", err)
	}
	if err := stream.Send(&flight.HandshakeRequest{Payload: []byte(token)}); err != nil {
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
		c.log.InfoContext(ctx, "handshake: server requires no auth token")
	}
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
				c.log.InfoContext(ctx, "ListActions: not implemented by server")
				return nil, nil
			}
			return nil, fmt.Errorf("ListActions: recv: %w", err)
		}
		actions = append(actions, at)
	}
	return actions, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ListFlights
// ─────────────────────────────────────────────────────────────────────────────

// ListFlights enumerates all flights the server advertises.
// Pass nil criteria to list everything.
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
			if status.Code(err) == codes.Unimplemented {
				c.log.InfoContext(ctx, "ListFlights: not implemented by server")
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
// GetSchema  (schema-only probe – no data transfer)
// ─────────────────────────────────────────────────────────────────────────────

// GetSchema returns the Arrow schema for sql without executing the query or
// transferring any record batches.
//
// Server contract: FlightDescriptor.Cmd = raw SQL bytes.
func (c *FlightSQLClient) GetSchema(ctx context.Context, sessionID, sql string) (*arrow.Schema, error) {
	ctx = c.baseCtx(ctx, sessionID)

	resp, err := c.raw.GetSchema(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql), // raw SQL – NOT JSON
	})
	if err != nil {
		return nil, fmt.Errorf("GetSchema: %w", err)
	}
	schema, err := flight.DeserializeSchema(resp.Schema, c.cfg.Allocator)
	if err != nil {
		return nil, fmt.Errorf("GetSchema: deserialize schema: %w", err)
	}
	c.log.InfoContext(ctx, "GetSchema",
		slog.String("sql", sql),
		slog.Int("fields", schema.NumFields()),
	)
	return schema, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// getFlightInfo  (internal: GetFlightInfo for raw SQL)
// ─────────────────────────────────────────────────────────────────────────────

// getFlightInfo calls GetFlightInfo with raw SQL in Cmd and returns all
// endpoint tickets.
//
// Server contract: FlightDescriptor.Cmd = raw SQL bytes.
func (c *FlightSQLClient) getFlightInfo(ctx context.Context, sql string) ([]*flight.Ticket, error) {
	info, err := c.raw.GetFlightInfo(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte(sql), // raw SQL – NOT JSON
	})
	if err != nil {
		return nil, fmt.Errorf("GetFlightInfo: %w", err)
	}
	if len(info.Endpoint) == 0 {
		return nil, errors.New("GetFlightInfo: server returned no endpoints")
	}
	tickets := make([]*flight.Ticket, 0, len(info.Endpoint))
	for _, ep := range info.Endpoint {
		tickets = append(tickets, ep.Ticket)
	}
	return tickets, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// DoGet  – single ticket
// ─────────────────────────────────────────────────────────────────────────────

// doGet fetches a single plan ticket and collects all record batches.
func (c *FlightSQLClient) doGet(ctx context.Context, ticket *flight.Ticket) (*QueryResult, error) {
	stream, err := c.raw.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("DoGet: %w", err)
	}
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(c.cfg.Allocator))
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
		return nil, fmt.Errorf("DoGet: read records: %w", err)
	}
	return result, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Query  – GetFlightInfo → parallel DoGet fanout
// ─────────────────────────────────────────────────────────────────────────────

type shardResult struct {
	idx    int
	result *QueryResult
	err    error
}

// Query executes sql, fans out across all endpoints returned by GetFlightInfo,
// and returns per-shard results in index order.
// Each QueryResult must be Released by the caller.
func (c *FlightSQLClient) Query(ctx context.Context, sessionID, sql string) ([]*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	tickets, err := c.getFlightInfo(ctx, sql)
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
// Prepared statement lifecycle
// ─────────────────────────────────────────────────────────────────────────────

// Prepare sends a CreatePreparedStatement action and returns the opaque plan
// ticket bytes (JSON {"plan_id":"..."}) assigned by the server.
// Pass the returned bytes to QueryPrepared or ClosePrepared.
//
// Server contract: action Body = JSON {"sql":"..."}.
func (c *FlightSQLClient) Prepare(ctx context.Context, sessionID, sql string) ([]byte, error) {
	ctx = c.baseCtx(ctx, sessionID)

	body, err := json.Marshal(struct {
		SQL string `json:"sql"`
	}{SQL: sql})
	if err != nil {
		return nil, fmt.Errorf("Prepare: marshal body: %w", err)
	}

	stream, err := c.raw.DoAction(ctx, &flight.Action{
		Type: "CreatePreparedStatement",
		Body: body,
	})
	if err != nil {
		return nil, fmt.Errorf("Prepare: DoAction: %w", err)
	}
	defer stream.CloseSend() //nolint:errcheck

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

	// Validate the server returned a well-formed plan ticket.
	pt, err := decodePlanTicket(ticket)
	if err != nil {
		return nil, fmt.Errorf("Prepare: bad ticket from server: %w", err)
	}
	c.log.InfoContext(ctx, "prepared statement created", slog.String("plan_id", pt.PlanID))

	return ticket, nil
}

// QueryPrepared executes a previously prepared statement.
//
// The plan ticket returned by Prepare IS a valid DoGet ticket; no additional
// GetFlightInfo round-trip is required.
func (c *FlightSQLClient) QueryPrepared(ctx context.Context, sessionID string, prepTicket []byte) ([]*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	// The plan ticket is directly usable as a DoGet ticket.
	result, err := c.doGet(ctx, &flight.Ticket{Ticket: prepTicket})
	if err != nil {
		return nil, fmt.Errorf("QueryPrepared: %w", err)
	}
	return []*QueryResult{result}, nil
}

// ClosePrepared releases server-side resources for a prepared statement.
// Always call this (ideally via defer) after Prepare succeeds.
//
// Server contract: action Body = JSON {"plan_id":"..."}.
func (c *FlightSQLClient) ClosePrepared(ctx context.Context, sessionID string, prepTicket []byte) error {
	ctx = c.baseCtx(ctx, sessionID)

	pt, err := decodePlanTicket(prepTicket)
	if err != nil {
		return fmt.Errorf("ClosePrepared: %w", err)
	}

	body, err := json.Marshal(struct {
		PlanID string `json:"plan_id"` // server key is plan_id, not prepared_id
	}{PlanID: pt.PlanID})
	if err != nil {
		return fmt.Errorf("ClosePrepared: marshal body: %w", err)
	}

	stream, err := c.raw.DoAction(ctx, &flight.Action{
		Type: "ClosePreparedStatement",
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("ClosePrepared: DoAction: %w", err)
	}
	defer stream.CloseSend() //nolint:errcheck

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
	c.log.InfoContext(ctx, "prepared statement closed", slog.String("plan_id", pt.PlanID))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// DoExchange  – bidirectional streaming
// ─────────────────────────────────────────────────────────────────────────────

// Exchange executes sql via the DoExchange RPC and collects all result batches.
//
// Protocol:
//  1. Call GetFlightInfo(sql) to register the plan and receive its plan ticket.
//  2. Open DoExchange and send the plan ticket in AppMetadata of the first
//     FlightData frame (DataBody left empty).
//  3. CloseSend, then read IPC records from the response stream.
//
// One DoExchange call handles one SQL statement. For multiple statements,
// call Exchange once per SQL.
//
// Server contract: first message AppMetadata = plan ticket bytes ({"plan_id":"..."}).
func (c *FlightSQLClient) Exchange(ctx context.Context, sessionID, sql string) (*QueryResult, error) {
	ctx = c.baseCtx(ctx, sessionID)

	// Step 1 – register the SQL, get the plan ticket.
	tickets, err := c.getFlightInfo(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("Exchange: GetFlightInfo: %w", err)
	}
	// Use the first (and typically only) endpoint ticket as the plan identifier.
	planTicketBytes := tickets[0].Ticket

	// Step 2 – open the exchange stream and send the plan ticket.
	stream, err := c.raw.DoExchange(ctx)
	if err != nil {
		return nil, fmt.Errorf("Exchange: open stream: %w", err)
	}

	if err := stream.Send(&flight.FlightData{
		AppMetadata: planTicketBytes, // server reads AppMetadata, not DataHeader
	}); err != nil {
		_ = stream.CloseSend()
		return nil, fmt.Errorf("Exchange: send ticket: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("Exchange: CloseSend: %w", err)
	}

	// Step 3 – read IPC record stream from the server.
	reader, err := flight.NewRecordReader(stream, ipc.WithAllocator(c.cfg.Allocator))
	if err != nil {
		return nil, fmt.Errorf("Exchange: NewRecordReader: %w", err)
	}
	defer reader.Release()

	result := &QueryResult{Schema: reader.Schema()}
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		result.Batches = append(result.Batches, rec)
	}
	if err := reader.Err(); err != nil {
		result.Release()
		return nil, fmt.Errorf("Exchange: read records: %w", err)
	}
	return result, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging helper
// ─────────────────────────────────────────────────────────────────────────────

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
// main  – exercises the full Flight spec against the DuckDB server
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	client, err := NewFlightSQLClient(ClientConfig{
		Target:        "localhost:32010",
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
	// The reference server echoes the payload back; no real auth is enforced.

	log.Info("── 1. Handshake")
	if err := client.Handshake(ctx, "demo-token"); err != nil {
		log.Warn("handshake failed (server may not require auth)", slog.Any("err", err))
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
	// The plan cache is empty at startup; non-zero count only after queries.

	log.Info("── 3. ListFlights")
	flights, err := client.ListFlights(ctx, session, nil)
	if err != nil {
		log.Warn("ListFlights", slog.Any("err", err))
	}
	log.Info("ListFlights", slog.Int("count", len(flights)))

	// ── 4. GetSchema  (schema probe, no data) ───────────────────────────────

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

	// ── 6. ListFlights again  (plan cache now populated) ────────────────────

	log.Info("── 6. ListFlights (after queries)")
	flights, err = client.ListFlights(ctx, session, nil)
	if err != nil {
		log.Warn("ListFlights", slog.Any("err", err))
	}
	log.Info("ListFlights", slog.Int("count", len(flights)))

	// ── 7. Prepared statement lifecycle ─────────────────────────────────────
	//      Prepare → DoGet with plan ticket → ClosePrepared

	log.Info("── 7. Prepared statement")
	prepTicket, err := client.Prepare(ctx, session, "SELECT 42 AS answer")
	if err != nil {
		log.Error("Prepare", slog.Any("err", err))
		os.Exit(1)
	}
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
	logResults(log, "prepared", prepResults)
	for _, r := range prepResults {
		r.Release()
	}

	// ── 8. DoExchange  (GetFlightInfo → plan ticket → bidirectional stream) ─

	log.Info("── 8. DoExchange")
	for _, sql := range []string{
		"SELECT 100 AS x",
		"SELECT 200 AS y",
	} {
		result, err := client.Exchange(ctx, session, sql)
		if err != nil {
			log.Error("Exchange", slog.String("sql", sql), slog.Any("err", err))
			os.Exit(1)
		}
		logResults(log, "exchange:"+sql, []*QueryResult{result})
		result.Release()
	}

	log.Info("── done")
}

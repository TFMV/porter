package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	adbc_driver "github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/TFMV/porter/internal/adbc"
)

const (
	defaultSchemaProbeTimeout = 5 * time.Second
)

type StreamChunk = flight.StreamChunk

var (
	ErrInvalidSQL = status.Error(codes.InvalidArgument, "invalid SQL statement")
)

func wrapInternal(err error, msg string) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

func isInvalidArgument(err error) bool {
	var adbcErr adbc_driver.Error
	if ok := errors.As(err, &adbcErr); !ok {
		return false
	}
	return adbcErr.Code == adbc_driver.StatusInvalidArgument
}

type Config struct {
	DBPath               string
	MaxConcurrentQueries int
	SchemaProbeTimeout   time.Duration
	ReadOnly             bool
	Logger               *slog.Logger
	DevMode              bool
	ADBCManager          *adbc.Manager
}

type Engine interface {
	BuildStream(ctx context.Context, sql string, params arrow.RecordBatch) (*arrow.Schema, <-chan StreamChunk, error)
	IngestStream(ctx context.Context, table string, reader array.RecordReader, opts IngestOptions) (int64, error)
	AcquireQuerySlot(ctx context.Context) error
	ReleaseQuerySlot()
	DeriveSchema(ctx context.Context, sql string) (*arrow.Schema, error)
	ExecuteUpdate(ctx context.Context, sql string) (int64, error)
	Allocator() memory.Allocator
	Close() error
}

type engine struct {
	db                 adbc_driver.Database
	Alloc              memory.Allocator
	Logger             *slog.Logger
	schemaProbeTimeout time.Duration
	querySemaphore     chan struct{}

	activeOps       sync.WaitGroup
	activeQueries   map[int64]context.CancelFunc
	activeQueriesMu sync.Mutex
	nextQueryID     int64
	closeOnce       sync.Once
	tableLocks      sync.Map
}

type IngestOptions struct {
	Catalog      string
	DBSchema     string
	Temporary    bool
	ExtraOptions map[string]string
	// MaxUncommittedBytes bounds in-memory uncommitted segment storage.
	// Zero uses a safe default.
	MaxUncommittedBytes int64
}

func New(cfg Config) (Engine, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = ":memory:"
	}

	logger.Debug("initializing DuckDB database", slog.String("dbPath", dbPath), slog.Bool("readOnly", cfg.ReadOnly))

	if cfg.ADBCManager == nil {
		return nil, fmt.Errorf("adbc manager is required")
	}

	resolved, err := cfg.ADBCManager.EnsureRequiredDrivers()
	if err != nil {
		return nil, err
	}
	duckdb := resolved["duckdb"]
	logger.Info("resolved required adbc drivers", slog.String("duckdbPath", duckdb.LibPath), slog.String("flightsqlPath", resolved["flightsql"].LibPath))
	if err := os.Setenv("ADBC_DRIVER_PATH", duckdb.LibPath); err != nil {
		return nil, fmt.Errorf("set ADBC_DRIVER_PATH: %w", err)
	}

	drv := drivermgr.Driver{}

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

	schemaProbeTimeout := cfg.SchemaProbeTimeout
	if schemaProbeTimeout == 0 {
		schemaProbeTimeout = defaultSchemaProbeTimeout
	}

	eng := &engine{
		db:                 db,
		Alloc:              alloc,
		Logger:             logger,
		schemaProbeTimeout: schemaProbeTimeout,
		activeQueries:      make(map[int64]context.CancelFunc),
	}

	if cfg.MaxConcurrentQueries > 0 {
		eng.querySemaphore = make(chan struct{}, cfg.MaxConcurrentQueries)
	}

	return eng, nil
}

func (e *engine) Allocator() memory.Allocator {
	return e.Alloc
}

func (e *engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.activeQueriesMu.Lock()
		cancellers := make([]context.CancelFunc, 0, len(e.activeQueries))
		for _, cancel := range e.activeQueries {
			cancellers = append(cancellers, cancel)
		}
		e.activeQueriesMu.Unlock()

		for _, cancel := range cancellers {
			cancel()
		}

		e.activeOps.Wait()

		if e.db != nil {
			closeErr = e.db.Close()
			e.db = nil
		}
	})
	return closeErr
}

func (e *engine) trackQuery(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	id := atomic.AddInt64(&e.nextQueryID, 1)

	e.activeQueriesMu.Lock()
	e.activeQueries[id] = cancel
	e.activeQueriesMu.Unlock()

	e.activeOps.Add(1)

	cleanup := func() {
		e.activeQueriesMu.Lock()
		delete(e.activeQueries, id)
		e.activeQueriesMu.Unlock()
		cancel()
		e.activeOps.Done()
	}
	return ctx, cleanup
}

func (e *engine) AcquireQuerySlot(ctx context.Context) error {
	if e.querySemaphore == nil {
		return nil
	}
	select {
	case e.querySemaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *engine) ReleaseQuerySlot() {
	if e.querySemaphore == nil {
		return
	}
	<-e.querySemaphore
}

func (e *engine) DeriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	ctx, finishQuery := e.trackQuery(ctx)
	defer finishQuery()

	probeCtx, cancel := context.WithTimeout(ctx, e.schemaProbeTimeout)
	defer cancel()

	conn, err := e.db.Open(probeCtx)
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

	schema := reader.Schema()
	if e.Logger != nil {
		e.Logger.Debug("schema probe completed", slog.String("sql", sql))
	}
	return schema, nil
}

func (e *engine) BuildStream(
	ctx context.Context,
	sql string,
	params arrow.RecordBatch,
) (*arrow.Schema, <-chan StreamChunk, error) {
	ctx, finishQuery := e.trackQuery(ctx)

	cleanupParams := sync.Once{}
	releaseParams := func() {
		cleanupParams.Do(func() {
			if params != nil {
				params.Release()
			}
		})
	}

	slotReleased := false
	releaseSlot := func() {
		if slotReleased {
			return
		}
		slotReleased = true
		e.ReleaseQuerySlot()
	}

	abort := func() {
		releaseParams()
		releaseSlot()
		finishQuery()
	}

	if err := e.AcquireQuerySlot(ctx); err != nil {
		abort()
		return nil, nil, err
	}

	if e.Logger != nil {
		e.Logger.Debug("query slot acquired before execution", slog.String("sql", sql), slog.Bool("hasParams", params != nil))
	}

	conn, err := e.db.Open(ctx)
	if err != nil {
		abort()
		return nil, nil, wrapInternal(err, "open db connection")
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		conn.Close()
		abort()
		return nil, nil, wrapInternal(err, "create statement")
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		conn.Close()
		abort()
		if isInvalidArgument(err) {
			return nil, nil, ErrInvalidSQL
		}
		return nil, nil, wrapInternal(err, "set sql query")
	}

	if params != nil {
		if err := stmt.Prepare(ctx); err != nil {
			stmt.Close()
			conn.Close()
			abort()
			return nil, nil, wrapInternal(err, "prepare statement")
		}
		if err := stmt.Bind(ctx, params); err != nil {
			stmt.Close()
			conn.Close()
			abort()
			return nil, nil, wrapInternal(err, "bind parameters")
		}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		conn.Close()
		abort()
		return nil, nil, wrapInternal(err, "execute query")
	}

	schema := reader.Schema()

	ch := make(chan StreamChunk, 1)

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			reader.Release()
			stmt.Close()
			conn.Close()
			releaseParams()
			releaseSlot()
			finishQuery()
		})
	}

	go pumpRecords(ctx, reader, cleanup, ch)
	return schema, ch, nil
}

func (e *engine) ExecuteUpdate(ctx context.Context, sql string) (int64, error) {
	ctx, finishQuery := e.trackQuery(ctx)
	defer finishQuery()

	if err := e.AcquireQuerySlot(ctx); err != nil {
		return 0, err
	}
	defer e.ReleaseQuerySlot()

	conn, err := e.db.Open(ctx)
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
		if isInvalidArgument(err) {
			return 0, ErrInvalidSQL
		}
		return 0, wrapInternal(err, "set sql query")
	}
	return stmt.ExecuteUpdate(ctx)
}

func (e *engine) lockForTable(table string) func() {
	value, _ := e.tableLocks.LoadOrStore(table, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

const defaultMaxIngestUncommittedBytes int64 = 64 << 20 // 64MiB

// IngestStream is the single canonical ingestion path:
// Flight DoPut -> Engine.IngestStream -> SegmentWriter -> Commit.
//
// Memory ownership:
//   - The caller owns the record batches yielded by reader.
//   - SegmentWriter retains each appended batch as an immutable uncommitted segment.
//   - On commit/rollback/abort, SegmentWriter releases all retained segment references.
//
// Transaction semantics:
//   - One stream maps to one DB transaction.
//   - Uncommitted data lives only in SegmentWriter-owned segments.
//   - Commit incrementally applies segments inside one transaction and performs
//     one atomic visibility switch at connection commit.
//   - Rollback discards all uncommitted segment references.
//
// Backpressure model:
//   - Ingestion is pull-based from the incoming Arrow stream (no async buffering).
//   - SegmentWriter enforces MaxUncommittedBytes; on overflow ingestion fails fast
//     with ResourceExhausted and the stream transaction is rolled back.
func (e *engine) IngestStream(ctx context.Context, table string, reader array.RecordReader, opts IngestOptions) (int64, error) {
	if table == "" {
		return 0, status.Error(codes.InvalidArgument, "ingest table is required")
	}
	if reader == nil {
		return 0, status.Error(codes.InvalidArgument, "ingest reader is required")
	}

	ctx, finishQuery := e.trackQuery(ctx)
	defer finishQuery()

	if err := e.AcquireQuerySlot(ctx); err != nil {
		return 0, err
	}
	defer e.ReleaseQuerySlot()

	unlock := e.lockForTable(table)
	defer unlock()

	conn, err := e.db.Open(ctx)
	if err != nil {
		return 0, wrapInternal(err, "open db connection")
	}
	defer conn.Close()

	if connOpts, ok := conn.(adbc_driver.PostInitOptions); ok {
		if err := connOpts.SetOption(adbc_driver.OptionKeyAutoCommit, adbc_driver.OptionValueDisabled); err != nil {
			return 0, wrapInternal(err, "disable autocommit for ingest")
		}
		defer connOpts.SetOption(adbc_driver.OptionKeyAutoCommit, adbc_driver.OptionValueEnabled)
	}

	targetSchema, err := conn.GetTableSchema(ctx, nil, nil, table)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "resolve destination table schema: %v", err)
	}

	var firstBatch arrow.RecordBatch
	for reader.Next() {
		firstBatch = reader.RecordBatch()
		if firstBatch != nil {
			break
		}
	}
	if err := reader.Err(); err != nil {
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "read first ingest batch")
	}
	if firstBatch == nil {
		if err := conn.Commit(ctx); err != nil {
			_ = conn.Rollback(ctx)
			return 0, wrapInternal(err, "commit empty ingest stream")
		}
		return 0, nil
	}

	sessionSchema := firstBatch.Schema()
	if !sessionSchema.Equal(targetSchema) {
		_ = conn.Rollback(ctx)
		return 0, status.Errorf(codes.InvalidArgument, "ingest schema mismatch for table %q", table)
	}

	maxBytes := opts.MaxUncommittedBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxIngestUncommittedBytes
	}
	writer := newSegmentWriter(table, sessionSchema, opts, maxBytes)
	defer writer.Release()

	appendBatch := func(batch arrow.RecordBatch) error {
		if batch == nil {
			return nil
		}
		return writer.Append(batch)
	}

	if err := appendBatch(firstBatch); err != nil {
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "append first segment")
	}

	for reader.Next() {
		if err := appendBatch(reader.RecordBatch()); err != nil {
			_ = conn.Rollback(ctx)
			return 0, wrapInternal(err, "append ingest segment")
		}
	}
	if err := reader.Err(); err != nil {
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "read ingest stream")
	}

	ingested, err := writer.Commit(ctx, conn)
	if err != nil {
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "publish ingest segments")
	}
	if err := conn.Commit(ctx); err != nil {
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "commit ingest transaction")
	}
	return ingested, nil
}

// immutableSegment is the ingestion durability unit before commit.
//
// Contract:
//   - Wraps an Arrow RecordBatch retained at segment creation.
//   - Treated as immutable for lifetime of the ingest transaction.
//   - Never exposed to external callers, so it cannot be mutated by engine users.
//   - Safe to publish or discard by reference only.
type immutableSegment struct {
	batch arrow.RecordBatch
	rows  int64
	bytes int64
}

func newImmutableSegment(batch arrow.RecordBatch) immutableSegment {
	batch.Retain()
	return immutableSegment{
		batch: batch,
		rows:  batch.NumRows(),
		bytes: estimateBatchSizeBytes(batch),
	}
}

func (s immutableSegment) Release() {
	s.batch.Release()
}

type segmentWriter struct {
	table        string
	schema       *arrow.Schema
	opts         IngestOptions
	segments     []immutableSegment
	pendingBytes int64
	maxBytes     int64
}

func newSegmentWriter(table string, schema *arrow.Schema, opts IngestOptions, maxBytes int64) *segmentWriter {
	return &segmentWriter{
		table:    table,
		schema:   schema,
		opts:     opts,
		maxBytes: maxBytes,
	}
}

func (w *segmentWriter) Append(batch arrow.RecordBatch) error {
	if !batch.Schema().Equal(w.schema) {
		return status.Error(codes.InvalidArgument, "ingest stream schema changed mid-stream")
	}
	seg := newImmutableSegment(batch)
	segmentBytes := seg.bytes
	if w.pendingBytes+segmentBytes > w.maxBytes {
		seg.Release()
		return status.Errorf(codes.ResourceExhausted, "uncommitted ingest segments exceed memory limit (%d bytes)", w.maxBytes)
	}

	w.segments = append(w.segments, seg)
	w.pendingBytes += segmentBytes
	return nil
}

// Commit publishes immutable segments incrementally while preserving atomic
// visibility with the surrounding transaction commit.
func (w *segmentWriter) Commit(ctx context.Context, conn adbc_driver.Connection) (int64, error) {
	var totalRows int64
	for _, seg := range w.segments {
		n, err := ingestBatchToTable(ctx, conn, w.table, seg.batch, w.opts)
		if err != nil {
			return 0, err
		}
		if n < 0 {
			totalRows += seg.rows
			continue
		}
		totalRows += n
	}
	return totalRows, nil
}

func (w *segmentWriter) Release() {
	for _, seg := range w.segments {
		seg.Release()
	}
	w.segments = nil
	w.pendingBytes = 0
}

func ingestBatchToTable(ctx context.Context, conn adbc_driver.Connection, table string, batch arrow.RecordBatch, opts IngestOptions) (int64, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	if err := stmt.Bind(ctx, batch); err != nil {
		return 0, err
	}
	if err := stmt.SetOption(adbc_driver.OptionKeyIngestTargetTable, table); err != nil {
		return 0, err
	}
	if err := stmt.SetOption(adbc_driver.OptionKeyIngestMode, adbc_driver.OptionValueIngestModeAppend); err != nil {
		return 0, err
	}
	if opts.Catalog != "" {
		if err := stmt.SetOption(adbc_driver.OptionValueIngestTargetCatalog, opts.Catalog); err != nil {
			return 0, err
		}
	}
	if opts.DBSchema != "" {
		if err := stmt.SetOption(adbc_driver.OptionValueIngestTargetDBSchema, opts.DBSchema); err != nil {
			return 0, err
		}
	}
	if opts.Temporary {
		if err := stmt.SetOption(adbc_driver.OptionValueIngestTemporary, adbc_driver.OptionValueEnabled); err != nil {
			return 0, err
		}
	}
	for key, val := range opts.ExtraOptions {
		if err := stmt.SetOption(key, val); err != nil {
			return 0, err
		}
	}
	return stmt.ExecuteUpdate(ctx)
}

func estimateBatchSizeBytes(batch arrow.RecordBatch) int64 {
	var total int64
	for i := 0; i < int(batch.NumCols()); i++ {
		data := batch.Column(i).Data()
		if data == nil {
			continue
		}
		for _, b := range data.Buffers() {
			if b != nil {
				total += int64(b.Len())
			}
		}
	}
	return total
}

func pumpRecords(
	ctx context.Context,
	reader array.RecordReader,
	cleanup func(),
	ch chan<- StreamChunk,
) {
	defer close(ch)
	defer cleanup()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ok := reader.Next()
		if !ok {
			break
		}

		if ctx.Err() != nil {
			return
		}

		rec := reader.RecordBatch()
		if rec == nil {
			continue
		}

		rec.Retain()

		select {
		case ch <- StreamChunk{Data: rec}:
		case <-ctx.Done():
			rec.Release()
			return
		}
	}

	if err := reader.Err(); err != nil {
		select {
		case ch <- StreamChunk{Err: wrapInternal(err, "record reader error")}:
		case <-ctx.Done():
		}
	}
}

type BatchGuard struct {
	mu    sync.Mutex
	batch arrow.RecordBatch
}

func NewBatchGuard(b arrow.RecordBatch) *BatchGuard {
	if b == nil {
		return nil
	}
	return &BatchGuard{batch: b}
}

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

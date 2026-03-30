package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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
	"github.com/TFMV/porter/telemetry"
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

func wrapPreservingCode(err error, msg string) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return status.Errorf(st.Code(), "%s: %v", msg, err)
	}
	return wrapInternal(err, msg)
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
	Telemetry            telemetry.Publisher
	StartupSQL           []string
	ConnectionInitSQL    []string
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

	activeOps         sync.WaitGroup
	activeQueries     map[int64]context.CancelFunc
	activeQueriesMu   sync.Mutex
	nextQueryID       int64
	closeOnce         sync.Once
	tableLocks        sync.Map
	telemetry         telemetry.Publisher
	connectionInitSQL []string
}

type IngestOptions struct {
	Catalog       string
	DBSchema      string
	Temporary     bool
	TransactionID []byte
	IngestMode    string
	ExtraOptions  map[string]string
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

	if len(cfg.StartupSQL) > 0 {
		initCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		conn, err := db.Open(initCtx)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open database for startup initialization: %w", err)
		}

		if err := runStartupSQL(initCtx, conn, cfg.StartupSQL); err != nil {
			conn.Close()
			db.Close()
			return nil, fmt.Errorf("run startup SQL: %w", err)
		}
		if err := conn.Close(); err != nil {
			db.Close()
			return nil, fmt.Errorf("close startup initialization connection: %w", err)
		}
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
		telemetry:          cfg.Telemetry,
		connectionInitSQL:  append([]string(nil), cfg.ConnectionInitSQL...),
	}

	if cfg.MaxConcurrentQueries > 0 {
		eng.querySemaphore = make(chan struct{}, cfg.MaxConcurrentQueries)
	}

	return eng, nil
}

func (e *engine) Allocator() memory.Allocator {
	return e.Alloc
}

func runStartupSQL(ctx context.Context, conn adbc_driver.Connection, statements []string) error {
	for _, statement := range statements {
		sql := strings.TrimSpace(statement)
		if sql == "" {
			continue
		}

		stmt, err := conn.NewStatement()
		if err != nil {
			return fmt.Errorf("create startup statement for %q: %w", sql, err)
		}

		if err := stmt.SetSqlQuery(sql); err != nil {
			stmt.Close()
			return fmt.Errorf("set startup SQL %q: %w", sql, err)
		}

		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			stmt.Close()
			return fmt.Errorf("execute startup SQL %q: %w", sql, err)
		}

		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close startup statement for %q: %w", sql, err)
		}
	}
	return nil
}

func (e *engine) openConnection(ctx context.Context) (adbc_driver.Connection, error) {
	conn, err := e.db.Open(ctx)
	if err != nil {
		return nil, err
	}
	if len(e.connectionInitSQL) == 0 {
		return conn, nil
	}
	if err := runStartupSQL(ctx, conn, e.connectionInitSQL); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (e *engine) publish(ctx context.Context, evt telemetry.Event) {
	if e.telemetry == nil {
		return
	}
	e.telemetry.Publish(telemetry.ScopedEvent(ctx, evt))
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
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentTransport,
			Type:      telemetry.TypeQueueDepth,
			Metadata: map[string]any{
				"queue_depth":    len(e.querySemaphore),
				"queue_capacity": cap(e.querySemaphore),
				"queue_unit":     "slots",
			},
		})
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
	e.publish(context.Background(), telemetry.Event{
		Component: telemetry.ComponentTransport,
		Type:      telemetry.TypeQueueDepth,
		Metadata: map[string]any{
			"queue_depth":    len(e.querySemaphore),
			"queue_capacity": cap(e.querySemaphore),
			"queue_unit":     "slots",
		},
	})
}

func (e *engine) DeriveSchema(ctx context.Context, sql string) (*arrow.Schema, error) {
	ctx, finishQuery := e.trackQuery(ctx)
	defer finishQuery()

	probeCtx, cancel := context.WithTimeout(ctx, e.schemaProbeTimeout)
	defer cancel()

	conn, err := e.openConnection(probeCtx)
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
	started := time.Now()

	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"operation": "query",
		},
	})

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
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
		abort()
		return nil, nil, err
	}

	if e.Logger != nil {
		e.Logger.Debug("query slot acquired before execution", slog.String("sql", sql), slog.Bool("hasParams", params != nil))
	}

	conn, err := e.openConnection(ctx)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
		abort()
		return nil, nil, wrapInternal(err, "open db connection")
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
		conn.Close()
		abort()
		return nil, nil, wrapInternal(err, "create statement")
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
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
			e.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeError,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"operation": "query",
					"reason":    err.Error(),
				},
			})
			stmt.Close()
			conn.Close()
			abort()
			return nil, nil, wrapInternal(err, "prepare statement")
		}
		if err := stmt.Bind(ctx, params); err != nil {
			e.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeError,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"operation": "query",
					"reason":    err.Error(),
				},
			})
			stmt.Close()
			conn.Close()
			abort()
			return nil, nil, wrapInternal(err, "bind parameters")
		}
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
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

	go pumpRecords(ctx, reader, cleanup, ch, e.telemetry, started)
	return schema, ch, nil
}

func (e *engine) ExecuteUpdate(ctx context.Context, sql string) (int64, error) {
	ctx, finishQuery := e.trackQuery(ctx)
	defer finishQuery()
	started := time.Now()

	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"operation": "update",
		},
	})

	if err := e.AcquireQuerySlot(ctx); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "update",
				"reason":    err.Error(),
			},
		})
		return 0, err
	}
	defer e.ReleaseQuerySlot()

	conn, err := e.openConnection(ctx)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "update",
				"reason":    err.Error(),
			},
		})
		return 0, wrapInternal(err, "open db connection")
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "update",
				"reason":    err.Error(),
			},
		})
		return 0, wrapInternal(err, "create statement")
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "update",
				"reason":    err.Error(),
			},
		})
		if isInvalidArgument(err) {
			return 0, ErrInvalidSQL
		}
		return 0, wrapInternal(err, "set sql query")
	}
	rows, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "update",
				"reason":    err.Error(),
			},
		})
		return 0, err
	}
	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestEnd,
		Rows:      rows,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"operation": "update",
		},
	})
	return rows, nil
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
	started := time.Now()

	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestStart,
		Metadata: map[string]any{
			"operation": "ingest",
			"table":     table,
		},
	})

	if err := e.AcquireQuerySlot(ctx); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		return 0, err
	}
	defer e.ReleaseQuerySlot()

	unlock := e.lockForTable(table)
	defer unlock()

	conn, err := e.openConnection(ctx)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		return 0, wrapInternal(err, "open db connection")
	}
	defer conn.Close()

	if connOpts, ok := conn.(adbc_driver.PostInitOptions); ok {
		if err := connOpts.SetOption(adbc_driver.OptionKeyAutoCommit, adbc_driver.OptionValueDisabled); err != nil {
			return 0, wrapInternal(err, "disable autocommit for ingest")
		}
		defer connOpts.SetOption(adbc_driver.OptionKeyAutoCommit, adbc_driver.OptionValueEnabled)
	}

	var firstBatch arrow.RecordBatch
	for reader.Next() {
		firstBatch = reader.RecordBatch()
		if firstBatch != nil {
			break
		}
	}
	if err := reader.Err(); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "read first ingest batch")
	}
	if firstBatch == nil {
		if err := conn.Commit(ctx); err != nil {
			e.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeError,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"operation": "ingest",
					"table":     table,
					"reason":    err.Error(),
				},
			})
			_ = conn.Rollback(ctx)
			return 0, wrapInternal(err, "commit empty ingest stream")
		}
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeRequestEnd,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
			},
		})
		return 0, nil
	}

	sessionSchema := firstBatch.Schema()
	targetSchema, err := resolveIngestTargetSchema(ctx, conn, table, sessionSchema, opts)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, err
	}
	if !sessionSchema.Equal(targetSchema) {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    "schema mismatch",
			},
		})
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
		bytes := estimateBatchSizeBytes(batch)
		if err := writer.Append(batch); err != nil {
			return err
		}
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentTransport,
			Type:      telemetry.TypeBatch,
			Rows:      batch.NumRows(),
			Bytes:     bytes,
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
			},
		})
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentTransport,
			Type:      telemetry.TypeQueueDepth,
			Rows:      batch.NumRows(),
			Bytes:     bytes,
			Metadata: map[string]any{
				"operation":      "ingest",
				"table":          table,
				"queue_depth":    writer.pendingBytes,
				"queue_capacity": writer.maxBytes,
				"queue_unit":     "bytes",
			},
		})
		return nil
	}

	if err := appendBatch(firstBatch); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, wrapPreservingCode(err, "append first segment")
	}

	for reader.Next() {
		if err := appendBatch(reader.RecordBatch()); err != nil {
			e.publish(ctx, telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeError,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"operation": "ingest",
					"table":     table,
					"reason":    err.Error(),
				},
			})
			_ = conn.Rollback(ctx)
			return 0, wrapPreservingCode(err, "append ingest segment")
		}
	}
	if err := reader.Err(); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, wrapPreservingCode(err, "read ingest stream")
	}

	ingested, err := writer.Commit(ctx, conn)
	if err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, wrapPreservingCode(err, "publish ingest segments")
	}
	if err := conn.Commit(ctx); err != nil {
		e.publish(ctx, telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "ingest",
				"table":     table,
				"reason":    err.Error(),
			},
		})
		_ = conn.Rollback(ctx)
		return 0, wrapInternal(err, "commit ingest transaction")
	}
	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestEnd,
		Rows:      ingested,
		Bytes:     writer.committedBytes,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"operation": "ingest",
			"table":     table,
		},
	})
	e.publish(ctx, telemetry.Event{
		Component: telemetry.ComponentTransport,
		Type:      telemetry.TypeQueueDepth,
		Metadata: map[string]any{
			"operation":      "ingest",
			"table":          table,
			"queue_depth":    0,
			"queue_capacity": writer.maxBytes,
			"queue_unit":     "bytes",
		},
	})
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
	table          string
	schema         *arrow.Schema
	opts           IngestOptions
	segments       []immutableSegment
	pendingBytes   int64
	maxBytes       int64
	committedBytes int64
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
	for i, seg := range w.segments {
		n, err := ingestBatchToTable(ctx, conn, w.table, seg.batch, w.segmentOptions(i))
		if err != nil {
			return 0, err
		}
		w.committedBytes += seg.bytes
		if n < 0 {
			totalRows += seg.rows
			continue
		}
		totalRows += n
	}
	return totalRows, nil
}

func (w *segmentWriter) segmentOptions(index int) IngestOptions {
	if index == 0 {
		return w.opts
	}

	switch ingestMode(w.opts) {
	case adbc_driver.OptionValueIngestModeCreate,
		adbc_driver.OptionValueIngestModeCreateAppend,
		adbc_driver.OptionValueIngestModeReplace:
		next := w.opts
		next.IngestMode = adbc_driver.OptionValueIngestModeAppend
		return next
	default:
		return w.opts
	}
}

func (w *segmentWriter) Release() {
	for _, seg := range w.segments {
		seg.Release()
	}
	w.segments = nil
	w.pendingBytes = 0
}

func ingestMode(opts IngestOptions) string {
	if opts.IngestMode != "" {
		return opts.IngestMode
	}
	return adbc_driver.OptionValueIngestModeAppend
}

func resolveIngestTargetSchema(
	ctx context.Context,
	conn adbc_driver.Connection,
	table string,
	sessionSchema *arrow.Schema,
	opts IngestOptions,
) (*arrow.Schema, error) {
	mode := ingestMode(opts)
	targetSchema, err := conn.GetTableSchema(ctx, nil, nil, table)
	if err == nil {
		if mode == adbc_driver.OptionValueIngestModeReplace {
			return sessionSchema, nil
		}
		return targetSchema, nil
	}

	if ingestMissingTableAllowed(err, mode) {
		return sessionSchema, nil
	}

	return nil, status.Errorf(codes.InvalidArgument, "resolve destination table schema: %v", err)
}

func ingestMissingTableAllowed(err error, mode string) bool {
	var adbcErr adbc_driver.Error
	if errors.As(err, &adbcErr) && adbcErr.Code == adbc_driver.StatusNotFound {
		switch mode {
		case adbc_driver.OptionValueIngestModeCreate,
			adbc_driver.OptionValueIngestModeCreateAppend,
			adbc_driver.OptionValueIngestModeReplace:
			return true
		}
	}

	if mode != adbc_driver.OptionValueIngestModeCreate &&
		mode != adbc_driver.OptionValueIngestModeCreateAppend &&
		mode != adbc_driver.OptionValueIngestModeReplace {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found") ||
		(strings.Contains(msg, "catalog error") && strings.Contains(msg, "table with name"))
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
	if err := stmt.SetOption(adbc_driver.OptionKeyIngestMode, ingestMode(opts)); err != nil {
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
	publisher telemetry.Publisher,
	started time.Time,
) {
	defer close(ch)
	defer cleanup()
	var totalRows int64
	var totalBytes int64
	publish := func(evt telemetry.Event) {
		if publisher == nil {
			return
		}
		publisher.Publish(telemetry.ScopedEvent(ctx, evt))
	}

	for {
		select {
		case <-ctx.Done():
			if totalRows > 0 || totalBytes > 0 {
				publish(telemetry.Event{
					Component: telemetry.ComponentExecution,
					Type:      telemetry.TypeStall,
					Rows:      totalRows,
					Bytes:     totalBytes,
					Latency:   time.Since(started),
					Metadata: map[string]any{
						"operation": "query",
						"reason":    ctx.Err().Error(),
					},
				})
			}
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
		rows := rec.NumRows()
		bytes := estimateBatchSizeBytes(rec)
		totalRows += rows
		totalBytes += bytes

		select {
		case ch <- StreamChunk{Data: rec}:
			publish(telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeBatch,
				Rows:      rows,
				Bytes:     bytes,
				Metadata: map[string]any{
					"operation": "query",
				},
			})
		case <-ctx.Done():
			rec.Release()
			publish(telemetry.Event{
				Component: telemetry.ComponentExecution,
				Type:      telemetry.TypeStall,
				Rows:      totalRows,
				Bytes:     totalBytes,
				Latency:   time.Since(started),
				Metadata: map[string]any{
					"operation": "query",
					"reason":    ctx.Err().Error(),
				},
			})
			return
		}
	}

	if err := reader.Err(); err != nil {
		publish(telemetry.Event{
			Component: telemetry.ComponentExecution,
			Type:      telemetry.TypeError,
			Rows:      totalRows,
			Bytes:     totalBytes,
			Latency:   time.Since(started),
			Metadata: map[string]any{
				"operation": "query",
				"reason":    err.Error(),
			},
		})
		select {
		case ch <- StreamChunk{Err: wrapInternal(err, "record reader error")}:
		case <-ctx.Done():
		}
		return
	}

	publish(telemetry.Event{
		Component: telemetry.ComponentExecution,
		Type:      telemetry.TypeRequestEnd,
		Rows:      totalRows,
		Bytes:     totalBytes,
		Latency:   time.Since(started),
		Metadata: map[string]any{
			"operation": "query",
		},
	})
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

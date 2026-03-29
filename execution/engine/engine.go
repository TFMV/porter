package engine

import (
	"context"
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

	if cfg.ADBCManager != nil {
		driverPath, err := cfg.ADBCManager.EnsureDriver("duckdb", "latest")
		if err != nil {
			return nil, fmt.Errorf("ensure duckdb driver: %w", err)
		}
		logger.Debug("resolved duckdb driver via manager", slog.String("path", driverPath))
		existingPath := os.Getenv("ADBC_DRIVER_PATH")
		if existingPath != "" {
			driverPath = existingPath + string(os.PathListSeparator) + driverPath
		}
		os.Setenv("ADBC_DRIVER_PATH", driverPath)
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
	if e.db != nil {
		return e.db.Close()
	}
	return nil
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
		return nil, nil, ErrInvalidSQL
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
		return 0, ErrInvalidSQL
	}
	return stmt.ExecuteUpdate(ctx)
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

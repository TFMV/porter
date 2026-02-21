// Package pool provides database connection pooling for DuckDB.
package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"

	pkgerrors "github.com/TFMV/porter/pkg/errors"
)

// Config represents pool configuration.
type Config struct {
	DSN                string        `json:"dsn"`
	MaxOpenConnections int           `json:"max_open_connections"`
	MaxIdleConnections int           `json:"max_idle_connections"`
	ConnMaxLifetime    time.Duration `json:"conn_max_lifetime"`
	ConnMaxIdleTime    time.Duration `json:"conn_max_idle_time"`
	HealthCheckPeriod  time.Duration `json:"health_check_period"`
	ConnectionTimeout  time.Duration `json:"connection_timeout"`

	// Enterprise features
	EnableCircuitBreaker    bool          `json:"enable_circuit_breaker"`
	CircuitBreakerThreshold int           `json:"circuit_breaker_threshold"`
	CircuitBreakerTimeout   time.Duration `json:"circuit_breaker_timeout"`
	EnableConnectionRetry   bool          `json:"enable_connection_retry"`
	MaxRetryAttempts        int           `json:"max_retry_attempts"`
	RetryBackoffBase        time.Duration `json:"retry_backoff_base"`
	EnableSlowQueryLogging  bool          `json:"enable_slow_query_logging"`
	SlowQueryThreshold      time.Duration `json:"slow_query_threshold"`
	EnableMetrics           bool          `json:"enable_metrics"`
	MetricsNamespace        string        `json:"metrics_namespace"`
}

// ConnectionPool manages database connections.
type ConnectionPool interface {
	// Get returns a database connection.
	Get(ctx context.Context) (*sql.DB, error)
	// GetWithValidation returns a validated database connection.
	GetWithValidation(ctx context.Context) (*EnterpriseConnection, error)
	// Stats returns pool statistics.
	Stats() PoolStats
	// EnterpriseStats returns comprehensive enterprise statistics.
	EnterpriseStats() EnterprisePoolStats
	// HealthCheck performs a health check on the pool.
	HealthCheck(ctx context.Context) error
	// Close closes the connection pool.
	Close() error
	// SetMetricsCollector sets the metrics collector.
	SetMetricsCollector(collector MetricsCollector)
}

// MetricsCollector interface for collecting pool metrics.
type MetricsCollector interface {
	RecordConnectionAcquisition(duration time.Duration)
	RecordConnectionValidation(success bool, duration time.Duration)
	RecordQueryExecution(query string, duration time.Duration, success bool)
	UpdateActiveConnections(count int)
	IncrementCircuitBreakerTrip()
	IncrementRetryAttempt()
}

// PoolStats represents connection pool statistics.
type PoolStats struct {
	OpenConnections   int           `json:"open_connections"`
	InUse             int           `json:"in_use"`
	Idle              int           `json:"idle"`
	WaitCount         int64         `json:"wait_count"`
	WaitDuration      time.Duration `json:"wait_duration"`
	MaxIdleClosed     int64         `json:"max_idle_closed"`
	MaxLifetimeClosed int64         `json:"max_lifetime_closed"`
	LastHealthCheck   time.Time     `json:"last_health_check"`
	HealthCheckStatus string        `json:"health_check_status"`
}

// EnterprisePoolStats provides comprehensive pool statistics.
type EnterprisePoolStats struct {
	PoolStats
	CircuitBreakerState    string        `json:"circuit_breaker_state"`
	CircuitBreakerFailures int64         `json:"circuit_breaker_failures"`
	TotalRetryAttempts     int64         `json:"total_retry_attempts"`
	SlowQueries            int64         `json:"slow_queries"`
	ValidationFailures     int64         `json:"validation_failures"`
	AverageAcquisitionTime time.Duration `json:"average_acquisition_time"`
	PeakConnections        int           `json:"peak_connections"`
	ConnectionErrors       int64         `json:"connection_errors"`
}

// CircuitBreakerState represents the state of the circuit breaker.
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case CircuitBreakerClosed:
		return "closed"
	case CircuitBreakerOpen:
		return "open"
	case CircuitBreakerHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type connectionPool struct {
	db     *sql.DB
	config Config
	logger zerolog.Logger

	// Use atomic.Bool for lock-free closed state
	closed atomic.Bool

	// Health check state with atomic operations
	lastHealthCheck atomic.Int64 // Unix timestamp
	healthStatus    atomic.Value // string

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Metrics
	waitCount    atomic.Int64
	waitDuration atomic.Int64

	// Enterprise features
	circuitBreaker      *CircuitBreaker
	metricsCollector    MetricsCollector
	enterpriseStats     *EnterpriseStats
	connectionValidator *ConnectionValidator
	queryLogger         *QueryLogger
	mu                  sync.RWMutex
}

// CircuitBreaker implements the circuit breaker pattern for connection failures.
type CircuitBreaker struct {
	state           atomic.Int32 // CircuitBreakerState
	failures        atomic.Int64
	lastFailureTime atomic.Int64
	threshold       int
	timeout         time.Duration
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// CanExecute checks if the circuit breaker allows execution.
func (cb *CircuitBreaker) CanExecute() bool {
	state := CircuitBreakerState(cb.state.Load())

	switch state {
	case CircuitBreakerClosed:
		return true
	case CircuitBreakerOpen:
		// Check if timeout has passed
		if time.Since(time.Unix(cb.lastFailureTime.Load(), 0)) > cb.timeout {
			// Try to transition to half-open
			if cb.state.CompareAndSwap(int32(CircuitBreakerOpen), int32(CircuitBreakerHalfOpen)) {
				return true
			}
		}
		return false
	case CircuitBreakerHalfOpen:
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(CircuitBreakerClosed))
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	failures := cb.failures.Add(1)
	cb.lastFailureTime.Store(time.Now().Unix())

	if failures >= int64(cb.threshold) {
		cb.state.Store(int32(CircuitBreakerOpen))
	}
}

// GetState returns the current circuit breaker state.
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(cb.state.Load())
}

// GetFailures returns the current failure count.
func (cb *CircuitBreaker) GetFailures() int64 {
	return cb.failures.Load()
}

// EnterpriseStats tracks comprehensive statistics.
type EnterpriseStats struct {
	retryAttempts      atomic.Int64
	slowQueries        atomic.Int64
	validationFailures atomic.Int64
	acquisitionTimes   []time.Duration
	peakConnections    atomic.Int32
	connectionErrors   atomic.Int64
	mu                 sync.RWMutex
}

// NewEnterpriseStats creates new enterprise statistics tracker.
func NewEnterpriseStats() *EnterpriseStats {
	return &EnterpriseStats{
		acquisitionTimes: make([]time.Duration, 0, 1000), // Ring buffer for last 1000 acquisitions
	}
}

// RecordAcquisitionTime records connection acquisition time.
func (es *EnterpriseStats) RecordAcquisitionTime(duration time.Duration) {
	es.mu.Lock()
	defer es.mu.Unlock()

	// Maintain ring buffer
	if len(es.acquisitionTimes) >= 1000 {
		es.acquisitionTimes = es.acquisitionTimes[1:]
	}
	es.acquisitionTimes = append(es.acquisitionTimes, duration)
}

// GetAverageAcquisitionTime returns the average acquisition time.
func (es *EnterpriseStats) GetAverageAcquisitionTime() time.Duration {
	es.mu.RLock()
	defer es.mu.RUnlock()

	if len(es.acquisitionTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range es.acquisitionTimes {
		total += t
	}

	return total / time.Duration(len(es.acquisitionTimes))
}

// ConnectionValidator validates database connections.
type ConnectionValidator struct {
	logger zerolog.Logger
	config Config
}

// NewConnectionValidator creates a new connection validator.
func NewConnectionValidator(logger zerolog.Logger, config Config) *ConnectionValidator {
	return &ConnectionValidator{
		logger: logger,
		config: config,
	}
}

// ValidateConnection performs comprehensive connection validation.
func (cv *ConnectionValidator) ValidateConnection(ctx context.Context, db *sql.DB) error {
	start := time.Now()
	defer func() {
		cv.logger.Debug().
			Dur("validation_duration", time.Since(start)).
			Msg("Connection validation completed")
	}()

	// Basic ping test
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	// Query test
	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("query test failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("query test returned unexpected result: %d", result)
	}

	// Transaction test
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			cv.logger.Error().Err(err).Msg("failed to rollback transaction")
		}
	}()

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	return nil
}

// QueryLogger logs slow queries and query statistics.
type QueryLogger struct {
	logger    zerolog.Logger
	threshold time.Duration
	enabled   bool
}

// NewQueryLogger creates a new query logger.
func NewQueryLogger(logger zerolog.Logger, threshold time.Duration, enabled bool) *QueryLogger {
	return &QueryLogger{
		logger:    logger,
		threshold: threshold,
		enabled:   enabled,
	}
}

// LogQuery logs query execution details.
func (ql *QueryLogger) LogQuery(query string, duration time.Duration, err error) {
	if !ql.enabled {
		return
	}

	logEvent := ql.logger.Debug()
	if duration > ql.threshold {
		logEvent = ql.logger.Warn().Bool("slow_query", true)
	}

	logEvent.
		Dur("duration", duration).
		Str("query", truncateQuery(query)).
		Bool("success", err == nil).
		Msg("Query executed")

	if err != nil {
		ql.logger.Error().
			Err(err).
			Str("query", truncateQuery(query)).
			Msg("Query execution failed")
	}
}

// EnterpriseConnection wraps a database connection with enterprise features.
type EnterpriseConnection struct {
	db              *sql.DB
	pool            *connectionPool
	logger          zerolog.Logger
	validator       *ConnectionValidator
	queryLogger     *QueryLogger
	acquisitionTime time.Time
}

// NewEnterpriseConnection creates a new enterprise connection.
func NewEnterpriseConnection(db *sql.DB, pool *connectionPool) *EnterpriseConnection {
	return &EnterpriseConnection{
		db:              db,
		pool:            pool,
		logger:          pool.logger,
		validator:       pool.connectionValidator,
		queryLogger:     pool.queryLogger,
		acquisitionTime: time.Now(),
	}
}

// Execute executes a query with enterprise features.
func (ec *EnterpriseConnection) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return ec.executeWithRetry(ctx, func() (sql.Result, error) {
		start := time.Now()
		result, err := ec.db.ExecContext(ctx, query, args...)
		duration := time.Since(start)

		ec.queryLogger.LogQuery(query, duration, err)

		if ec.pool.metricsCollector != nil {
			ec.pool.metricsCollector.RecordQueryExecution(query, duration, err == nil)
		}

		if duration > ec.pool.config.SlowQueryThreshold {
			ec.pool.enterpriseStats.slowQueries.Add(1)
		}

		if err != nil {
			ec.pool.enterpriseStats.connectionErrors.Add(1)
			return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
		}

		return result, nil
	})
}

// Query executes a query and returns rows with enterprise features.
func (ec *EnterpriseConnection) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := ec.db.QueryContext(ctx, query, args...)
	duration := time.Since(start)

	ec.queryLogger.LogQuery(query, duration, err)

	if ec.pool.metricsCollector != nil {
		ec.pool.metricsCollector.RecordQueryExecution(query, duration, err == nil)
	}

	if duration > ec.pool.config.SlowQueryThreshold {
		ec.pool.enterpriseStats.slowQueries.Add(1)
	}

	if err != nil {
		ec.pool.enterpriseStats.connectionErrors.Add(1)
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
	}

	return rows, nil
}

// executeWithRetry executes an operation with retry logic.
func (ec *EnterpriseConnection) executeWithRetry(ctx context.Context, operation func() (sql.Result, error)) (sql.Result, error) {
	if !ec.pool.config.EnableConnectionRetry {
		return operation()
	}

	var lastErr error
	backoff := ec.pool.config.RetryBackoffBase

	for attempt := 0; attempt <= ec.pool.config.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			ec.pool.enterpriseStats.retryAttempts.Add(1)
			if ec.pool.metricsCollector != nil {
				ec.pool.metricsCollector.IncrementRetryAttempt()
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
			}
		}

		result, err := operation()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry certain types of errors
		if !isRetryableError(err) {
			break
		}
	}

	return nil, lastErr
}

// isRetryableError determines if an error is retryable.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"temporary failure",
		"network",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// New creates a new connection pool.
func New(cfg Config, logger zerolog.Logger) (ConnectionPool, error) {
	if cfg.DSN == "" {
		cfg.DSN = ":memory:" // Default to in-memory database
	}

	// Set defaults
	if cfg.MaxOpenConnections <= 0 {
		cfg.MaxOpenConnections = 25
	}
	if cfg.MaxIdleConnections <= 0 {
		cfg.MaxIdleConnections = 5
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 30 * time.Minute
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 10 * time.Minute
	}
	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = 30 * time.Second
	}

	// Enterprise defaults
	if cfg.CircuitBreakerThreshold <= 0 {
		cfg.CircuitBreakerThreshold = 5
	}
	if cfg.CircuitBreakerTimeout <= 0 {
		cfg.CircuitBreakerTimeout = 60 * time.Second
	}
	if cfg.MaxRetryAttempts <= 0 {
		cfg.MaxRetryAttempts = 3
	}
	if cfg.RetryBackoffBase <= 0 {
		cfg.RetryBackoffBase = 100 * time.Millisecond
	}
	if cfg.SlowQueryThreshold <= 0 {
		cfg.SlowQueryThreshold = 1 * time.Second
	}

	logger.Info().
		Str("dsn", maskDSN(cfg.DSN)).
		Int("max_open", cfg.MaxOpenConnections).
		Int("max_idle", cfg.MaxIdleConnections).
		Dur("conn_lifetime", cfg.ConnMaxLifetime).
		Dur("conn_idle_time", cfg.ConnMaxIdleTime).
		Bool("circuit_breaker", cfg.EnableCircuitBreaker).
		Bool("retry_enabled", cfg.EnableConnectionRetry).
		Msg("Creating enterprise DuckDB connection pool")

	db, err := sql.Open("duckdb", cfg.DSN)
	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeInternal, "failed to open database")
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConnections)
	db.SetMaxIdleConns(cfg.MaxIdleConnections)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	ctx, cancel := context.WithCancel(context.Background())

	pool := &connectionPool{
		db:     db,
		config: cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize enterprise features
	if cfg.EnableCircuitBreaker {
		pool.circuitBreaker = NewCircuitBreaker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerTimeout)
	}

	pool.enterpriseStats = NewEnterpriseStats()
	pool.connectionValidator = NewConnectionValidator(logger, cfg)
	pool.queryLogger = NewQueryLogger(logger, cfg.SlowQueryThreshold, cfg.EnableSlowQueryLogging)

	// Initialize health status
	pool.healthStatus.Store("unknown")

	// Verify connection
	connCtx, connCancel := context.WithTimeout(context.Background(), cfg.ConnectionTimeout)
	defer connCancel()

	if err := pool.HealthCheck(connCtx); err != nil {
		db.Close()
		cancel()
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "initial health check failed")
	}

	// Start health check routine if configured
	if cfg.HealthCheckPeriod > 0 {
		go pool.healthCheckRoutine(ctx)
	}

	logger.Info().Msg("Enterprise DuckDB connection pool created successfully")

	return pool, nil
}

// Get returns a database connection.
func (p *connectionPool) Get(ctx context.Context) (*sql.DB, error) {
	if p.closed.Load() {
		return nil, pkgerrors.New(pkgerrors.CodeUnavailable, "connection pool is closed")
	}

	// Check circuit breaker
	if p.circuitBreaker != nil && !p.circuitBreaker.CanExecute() {
		return nil, pkgerrors.New(pkgerrors.CodeUnavailable, "circuit breaker is open")
	}

	// Track wait time
	start := time.Now()
	p.waitCount.Add(1)
	defer func() {
		duration := time.Since(start)
		p.waitDuration.Add(int64(duration))
		p.enterpriseStats.RecordAcquisitionTime(duration)

		if p.metricsCollector != nil {
			p.metricsCollector.RecordConnectionAcquisition(duration)
		}
	}()

	// Verify connection is alive
	if err := p.db.PingContext(ctx); err != nil {
		p.logger.Error().Err(err).Msg("Database ping failed")

		if p.circuitBreaker != nil {
			p.circuitBreaker.RecordFailure()
		}

		return nil, pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "database connection failed")
	}

	if p.circuitBreaker != nil {
		p.circuitBreaker.RecordSuccess()
	}

	// Update peak connections
	stats := p.db.Stats()
	if int32(stats.OpenConnections) > p.enterpriseStats.peakConnections.Load() {
		p.enterpriseStats.peakConnections.Store(int32(stats.OpenConnections))
	}

	if p.metricsCollector != nil {
		p.metricsCollector.UpdateActiveConnections(stats.OpenConnections)
	}

	return p.db, nil
}

// GetWithValidation returns a validated database connection.
func (p *connectionPool) GetWithValidation(ctx context.Context) (*EnterpriseConnection, error) {
	db, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Validate connection
	start := time.Now()
	validationErr := p.connectionValidator.ValidateConnection(ctx, db)
	validationDuration := time.Since(start)

	if p.metricsCollector != nil {
		p.metricsCollector.RecordConnectionValidation(validationErr == nil, validationDuration)
	}

	if validationErr != nil {
		p.enterpriseStats.validationFailures.Add(1)
		return nil, pkgerrors.Wrap(validationErr, pkgerrors.CodeConnectionFailed, "connection validation failed")
	}

	return NewEnterpriseConnection(db, p), nil
}

// Stats returns pool statistics.
func (p *connectionPool) Stats() PoolStats {
	dbStats := p.db.Stats()

	return PoolStats{
		OpenConnections:   dbStats.OpenConnections,
		InUse:             dbStats.InUse,
		Idle:              dbStats.Idle,
		WaitCount:         p.waitCount.Load(),
		WaitDuration:      time.Duration(p.waitDuration.Load()),
		MaxIdleClosed:     dbStats.MaxIdleClosed,
		MaxLifetimeClosed: dbStats.MaxLifetimeClosed,
		LastHealthCheck:   time.Unix(p.lastHealthCheck.Load(), 0),
		HealthCheckStatus: p.getHealthStatus(),
	}
}

// EnterpriseStats returns comprehensive enterprise statistics.
func (p *connectionPool) EnterpriseStats() EnterprisePoolStats {
	stats := p.Stats()

	enterpriseStats := EnterprisePoolStats{
		PoolStats:              stats,
		TotalRetryAttempts:     p.enterpriseStats.retryAttempts.Load(),
		SlowQueries:            p.enterpriseStats.slowQueries.Load(),
		ValidationFailures:     p.enterpriseStats.validationFailures.Load(),
		AverageAcquisitionTime: p.enterpriseStats.GetAverageAcquisitionTime(),
		PeakConnections:        int(p.enterpriseStats.peakConnections.Load()),
		ConnectionErrors:       p.enterpriseStats.connectionErrors.Load(),
	}

	if p.circuitBreaker != nil {
		enterpriseStats.CircuitBreakerState = p.circuitBreaker.GetState().String()
		enterpriseStats.CircuitBreakerFailures = p.circuitBreaker.GetFailures()
	}

	return enterpriseStats
}

// SetMetricsCollector sets the metrics collector.
func (p *connectionPool) SetMetricsCollector(collector MetricsCollector) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metricsCollector = collector
}

// HealthCheck performs a health check on the pool.
func (p *connectionPool) HealthCheck(ctx context.Context) error {
	if p.closed.Load() {
		return pkgerrors.New(pkgerrors.CodeUnavailable, "connection pool is closed")
	}

	// Test connection
	if err := p.db.PingContext(ctx); err != nil {
		p.updateHealthStatus("unhealthy", err.Error())
		return pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "health check ping failed")
	}

	// Test query execution
	var result int
	err := p.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil || result != 1 {
		p.updateHealthStatus("unhealthy", "query test failed")
		return pkgerrors.Wrap(err, pkgerrors.CodeConnectionFailed, "health check query failed")
	}

	p.updateHealthStatus("healthy", "")
	return nil
}

// Close closes the connection pool.
func (p *connectionPool) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	p.logger.Info().Msg("Closing enterprise DuckDB connection pool")

	// Cancel the context to stop health check routine
	p.cancel()

	if err := p.db.Close(); err != nil {
		return pkgerrors.Wrap(err, pkgerrors.CodeInternal, "failed to close database")
	}

	return nil
}

// healthCheckRoutine performs periodic health checks until ctx is cancelled.
func (p *connectionPool) healthCheckRoutine(ctx context.Context) {
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	p.logger.Info().Dur("period", p.config.HealthCheckPeriod).Msg("Health check routine started")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info().Msg("Health check routine stopped")
			return
		case <-ticker.C:
			// Create a per-probe timeout
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := p.HealthCheck(probeCtx); err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Error().Err(err).Msg("Periodic health check failed")
			}
			cancel() // Immediate cleanup
		}
	}
}

// updateHealthStatus updates the health status using atomic operations.
func (p *connectionPool) updateHealthStatus(status, detail string) {
	p.lastHealthCheck.Store(time.Now().Unix())
	p.healthStatus.Store(status)

	if status == "unhealthy" && detail != "" {
		p.logger.Warn().
			Str("status", status).
			Str("detail", detail).
			Msg("Connection pool health status changed")
	}
}

// getHealthStatus safely retrieves the current health status.
func (p *connectionPool) getHealthStatus() string {
	if v := p.healthStatus.Load(); v != nil {
		return v.(string)
	}
	return "unknown"
}

// maskDSN hides sensitive information (passwords, tokens, secrets) but keeps
// enough of the string to be recognisable in logs.
//
// Behaviour:
//
//   - ":memory:" or empty → returned verbatim (special DuckDB value)
//   - URL‑like DSNs       → redact user‑password and sensitive query params
//   - Plain paths/files   → keep first/last 3 runes, mask the middle
//
// The function is intentionally conservative: if it cannot confidently parse
// the string as a URL it falls back to a simple middle‑mask so that nothing
// secret leaks.
func maskDSN(dsn string) string {
	if dsn == "" || dsn == ":memory:" {
		return dsn
	}

	u, err := url.Parse(dsn)
	if err == nil && looksLikeURL(u) {
		// ── mask user‑info ────────────────────────────────────────────────
		if ui := u.User; ui != nil {
			user := ui.Username()
			if _, hasPass := ui.Password(); hasPass {
				u.User = url.UserPassword(user, "*****")
			} else {
				u.User = url.User(user) // just keep the username
			}
		}

		// ── mask sensitive query params ──────────────────────────────────
		q := u.Query()
		for k := range q {
			if isSensitiveKey(k) {
				q.Set(k, "*****")
			}
		}
		u.RawQuery = q.Encode()
		return u.String()
	}

	// ── fallback: simple masking for non‑URL DSNs ────────────────────────
	runes := []rune(dsn)
	if len(runes) <= 10 {
		return "***"
	}
	return string(runes[:3]) + "***" + string(runes[len(runes)-3:])
}

// looksLikeURL returns true when the parsed value has enough URL structure to
// treat it as a DSN we can meaningfully redact.
func looksLikeURL(u *url.URL) bool {
	return u.Scheme != "" || u.Host != "" || u.User != nil || u.RawQuery != ""
}

// isSensitiveKey reports whether a query key should have its value masked.
func isSensitiveKey(key string) bool {
	key = strings.ToLower(key)
	switch {
	case strings.Contains(key, "pass"),
		strings.Contains(key, "token"),
		strings.Contains(key, "secret"),
		strings.HasSuffix(key, "key"):
		return true
	default:
		return false
	}
}

// ConnectionWrapper wraps a database connection with additional functionality.
type ConnectionWrapper struct {
	db     *sql.DB
	pool   *connectionPool
	logger zerolog.Logger
}

// NewConnectionWrapper creates a new connection wrapper.
func NewConnectionWrapper(db *sql.DB, pool *connectionPool, logger zerolog.Logger) *ConnectionWrapper {
	return &ConnectionWrapper{
		db:     db,
		pool:   pool,
		logger: logger,
	}
}

// Execute executes a query with retry logic.
func (w *ConnectionWrapper) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := w.db.ExecContext(ctx, query, args...)

	w.logger.Debug().
		Dur("duration", time.Since(start)).
		Str("query", truncateQuery(query)).
		Bool("success", err == nil).
		Msg("Executed query")

	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
	}

	return result, nil
}

// Query executes a query and returns rows.
func (w *ConnectionWrapper) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := w.db.QueryContext(ctx, query, args...)

	w.logger.Debug().
		Dur("duration", time.Since(start)).
		Str("query", truncateQuery(query)).
		Bool("success", err == nil).
		Msg("Executed query")

	if err != nil {
		return nil, pkgerrors.Wrap(err, pkgerrors.CodeQueryFailed, "query execution failed")
	}

	return rows, nil
}

// truncateQuery truncates long queries for logging.
func truncateQuery(query string) string {
	const maxLen = 100
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}

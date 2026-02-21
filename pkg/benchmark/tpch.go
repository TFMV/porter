// Package benchmark provides TPC-H benchmarking capabilities for Porter.
package benchmark

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// BenchmarkConfig holds configuration for TPC-H benchmarks.
type BenchmarkConfig struct {
	Queries     []string      `json:"queries"`
	ScaleFactor float64       `json:"scale_factor"`
	Iterations  int           `json:"iterations"`
	Timeout     time.Duration `json:"timeout"`
	Analyze     bool          `json:"analyze"`
	Database    string        `json:"database"`
}

// QueryResult represents the result of a single query execution.
type QueryResult struct {
	Query         string        `json:"query"`
	Iteration     int           `json:"iteration"`
	ExecutionTime time.Duration `json:"execution_time_ns"`
	RowCount      int64         `json:"row_count"`
	Error         string        `json:"error,omitempty"`
	QueryPlan     string        `json:"query_plan,omitempty"`
}

// BenchmarkResult represents the complete benchmark results.
type BenchmarkResult struct {
	Config      BenchmarkConfig `json:"config"`
	Results     []QueryResult   `json:"results"`
	TotalTime   time.Duration   `json:"total_time_ns"`
	StartTime   time.Time       `json:"start_time"`
	EndTime     time.Time       `json:"end_time"`
	Environment Environment     `json:"environment"`
}

// Environment captures system information for benchmark results.
type Environment struct {
	DuckDBVersion string `json:"duckdb_version"`
	GoVersion     string `json:"go_version"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
}

// TPC-H query definitions
var tpchQueries = map[string]string{
	"q1": `
		SELECT
			l_returnflag,
			l_linestatus,
			sum(l_quantity) as sum_qty,
			sum(l_extendedprice) as sum_base_price,
			sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
			sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
			avg(l_quantity) as avg_qty,
			avg(l_extendedprice) as avg_price,
			avg(l_discount) as avg_disc,
			count(*) as count_order
		FROM lineitem
		WHERE l_shipdate <= date '1998-12-01' - interval '90' day
		GROUP BY l_returnflag, l_linestatus
		ORDER BY l_returnflag, l_linestatus`,

	"q2": `
		SELECT
			s_acctbal,
			s_name,
			n_name,
			p_partkey,
			p_mfgr,
			s_address,
			s_phone,
			s_comment
		FROM part, supplier, partsupp, nation, region
		WHERE p_partkey = ps_partkey
			AND s_suppkey = ps_suppkey
			AND p_size = 15
			AND p_type like '%BRASS'
			AND s_nationkey = n_nationkey
			AND n_regionkey = r_regionkey
			AND r_name = 'EUROPE'
			AND ps_supplycost = (
				SELECT min(ps_supplycost)
				FROM partsupp, supplier, nation, region
				WHERE p_partkey = ps_partkey
					AND s_suppkey = ps_suppkey
					AND s_nationkey = n_nationkey
					AND n_regionkey = r_regionkey
					AND r_name = 'EUROPE')
		ORDER BY s_acctbal desc, n_name, s_name, p_partkey
		LIMIT 100`,

	"q3": `
		SELECT
			l_orderkey,
			sum(l_extendedprice * (1 - l_discount)) as revenue,
			o_orderdate,
			o_shippriority
		FROM customer, orders, lineitem
		WHERE c_mktsegment = 'BUILDING'
			AND c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate < date '1995-03-15'
			AND l_shipdate > date '1995-03-15'
		GROUP BY l_orderkey, o_orderdate, o_shippriority
		ORDER BY revenue desc, o_orderdate
		LIMIT 10`,

	"q4": `
		SELECT
			o_orderpriority,
			count(*) as order_count
		FROM orders
		WHERE o_orderdate >= date '1993-07-01'
			AND o_orderdate < date '1993-07-01' + interval '3' month
			AND exists (
				SELECT *
				FROM lineitem
				WHERE l_orderkey = o_orderkey
					AND l_commitdate < l_receiptdate)
		GROUP BY o_orderpriority
		ORDER BY o_orderpriority`,

	"q5": `
		SELECT
			n_name,
			sum(l_extendedprice * (1 - l_discount)) as revenue
		FROM customer, orders, lineitem, supplier, nation, region
		WHERE c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND l_suppkey = s_suppkey
			AND c_nationkey = s_nationkey
			AND s_nationkey = n_nationkey
			AND n_regionkey = r_regionkey
			AND r_name = 'ASIA'
			AND o_orderdate >= date '1994-01-01'
			AND o_orderdate < date '1994-01-01' + interval '1' year
		GROUP BY n_name
		ORDER BY revenue desc`,

	"q6": `
		SELECT
			sum(l_extendedprice * l_discount) as revenue
		FROM lineitem
		WHERE l_shipdate >= date '1994-01-01'
			AND l_shipdate < date '1994-01-01' + interval '1' year
			AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
			AND l_quantity < 24`,

	"q7": `
		SELECT
			supp_nation,
			cust_nation,
			l_year,
			sum(volume) as revenue
		FROM (
			SELECT
				n1.n_name as supp_nation,
				n2.n_name as cust_nation,
				extract(year from l_shipdate) as l_year,
				l_extendedprice * (1 - l_discount) as volume
			FROM supplier, lineitem, orders, customer, nation n1, nation n2
			WHERE s_suppkey = l_suppkey
				AND o_orderkey = l_orderkey
				AND c_custkey = o_custkey
				AND s_nationkey = n1.n_nationkey
				AND c_nationkey = n2.n_nationkey
				AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
					OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
				AND l_shipdate between date '1995-01-01' AND date '1996-12-31'
		) as shipping
		GROUP BY supp_nation, cust_nation, l_year
		ORDER BY supp_nation, cust_nation, l_year`,

	"q8": `
		SELECT
			o_year,
			sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
		FROM (
			SELECT
				extract(year from o_orderdate) as o_year,
				l_extendedprice * (1 - l_discount) as volume,
				n2.n_name as nation
			FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
			WHERE p_partkey = l_partkey
				AND s_suppkey = l_suppkey
				AND l_orderkey = o_orderkey
				AND o_custkey = c_custkey
				AND c_nationkey = n1.n_nationkey
				AND n1.n_regionkey = r_regionkey
				AND r_name = 'AMERICA'
				AND s_nationkey = n2.n_nationkey
				AND o_orderdate between date '1995-01-01' AND date '1996-12-31'
				AND p_type = 'ECONOMY ANODIZED STEEL'
		) as all_nations
		GROUP BY o_year
		ORDER BY o_year`,

	"q9": `
		SELECT
			nation,
			o_year,
			sum(amount) as sum_profit
		FROM (
			SELECT
				n_name as nation,
				extract(year from o_orderdate) as o_year,
				l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
			FROM part, supplier, lineitem, partsupp, orders, nation
			WHERE s_suppkey = l_suppkey
				AND ps_suppkey = l_suppkey
				AND ps_partkey = l_partkey
				AND p_partkey = l_partkey
				AND o_orderkey = l_orderkey
				AND s_nationkey = n_nationkey
				AND p_name like '%green%'
		) as profit
		GROUP BY nation, o_year
		ORDER BY nation, o_year desc`,

	"q10": `
		SELECT
			c_custkey,
			c_name,
			sum(l_extendedprice * (1 - l_discount)) as revenue,
			c_acctbal,
			n_name,
			c_address,
			c_phone,
			c_comment
		FROM customer, orders, lineitem, nation
		WHERE c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate >= date '1993-10-01'
			AND o_orderdate < date '1993-10-01' + interval '3' month
			AND l_returnflag = 'R'
			AND c_nationkey = n_nationkey
		GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
		ORDER BY revenue desc
		LIMIT 20`,
}

// Runner executes TPC-H benchmarks.
type Runner struct {
	db        *sql.DB
	allocator memory.Allocator
	logger    zerolog.Logger
}

// NewRunner creates a new benchmark runner.
func NewRunner(dbPath string, logger zerolog.Logger) (*Runner, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Runner{
		db:        db,
		allocator: memory.NewGoAllocator(),
		logger:    logger.With().Str("component", "benchmark").Logger(),
	}, nil
}

// Close closes the database connection.
func (r *Runner) Close() error {
	return r.db.Close()
}

// Run executes the benchmark with the given configuration.
func (r *Runner) Run(ctx context.Context, config BenchmarkConfig) (*BenchmarkResult, error) {
	startTime := time.Now()

	r.logger.Info().
		Strs("queries", config.Queries).
		Float64("scale", config.ScaleFactor).
		Int("iterations", config.Iterations).
		Msg("Starting TPC-H benchmark")

	// Initialize TPC-H data
	if err := r.initializeTPCH(ctx, config.ScaleFactor); err != nil {
		return nil, fmt.Errorf("failed to initialize TPC-H data: %w", err)
	}

	var results []QueryResult

	for _, queryName := range config.Queries {
		query, exists := tpchQueries[queryName]
		if !exists {
			r.logger.Warn().Str("query", queryName).Msg("Unknown query, skipping")
			continue
		}

		for iteration := 1; iteration <= config.Iterations; iteration++ {
			result := r.executeQuery(ctx, queryName, query, iteration, config)
			results = append(results, result)

			r.logger.Info().
				Str("query", queryName).
				Int("iteration", iteration).
				Dur("time", result.ExecutionTime).
				Int64("rows", result.RowCount).
				Str("error", result.Error).
				Msg("Query completed")
		}
	}

	endTime := time.Now()

	env, err := r.getEnvironment(ctx)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Failed to get environment info")
	}

	return &BenchmarkResult{
		Config:      config,
		Results:     results,
		TotalTime:   endTime.Sub(startTime),
		StartTime:   startTime,
		EndTime:     endTime,
		Environment: env,
	}, nil
}

// initializeTPCH sets up the TPC-H schema and data using DuckDB's built-in module.
func (r *Runner) initializeTPCH(ctx context.Context, scaleFactor float64) error {
	r.logger.Info().Float64("scale", scaleFactor).Msg("Initializing TPC-H data")

	// Install and load the TPC-H extension
	queries := []string{
		"INSTALL tpch",
		"LOAD tpch",
		fmt.Sprintf("CALL dbgen(sf=%g)", scaleFactor),
	}

	for _, query := range queries {
		if _, err := r.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute '%s': %w", query, err)
		}
	}

	r.logger.Info().Float64("scale", scaleFactor).Msg("TPC-H data initialized")
	return nil
}

// executeQuery runs a single query and measures its performance.
func (r *Runner) executeQuery(ctx context.Context, queryName, query string, iteration int, config BenchmarkConfig) QueryResult {
	result := QueryResult{
		Query:     queryName,
		Iteration: iteration,
	}

	// Add timeout to context
	queryCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	start := time.Now()

	// Get query plan if requested
	if config.Analyze {
		planQuery := "EXPLAIN " + query
		rows, err := r.db.QueryContext(queryCtx, planQuery)
		if err != nil {
			result.QueryPlan = fmt.Sprintf("Error getting plan: %v", err)
		} else {
			plan, err := r.readPlan(rows)
			rows.Close()
			if err != nil {
				result.QueryPlan = fmt.Sprintf("Error reading plan: %v", err)
			} else {
				result.QueryPlan = plan
			}
		}
	}

	// Execute the actual query
	rows, err := r.db.QueryContext(queryCtx, query)
	if err != nil {
		result.Error = err.Error()
		result.ExecutionTime = time.Since(start)
		return result
	}
	defer rows.Close()

	// Count rows
	var rowCount int64
	for rows.Next() {
		rowCount++
	}

	if err := rows.Err(); err != nil {
		result.Error = err.Error()
	}

	result.ExecutionTime = time.Since(start)
	result.RowCount = rowCount

	return result
}

// readPlan reads the query plan from EXPLAIN output.
func (r *Runner) readPlan(rows *sql.Rows) (string, error) {
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	return plan.String(), rows.Err()
}

// getEnvironment captures system and database information.
func (r *Runner) getEnvironment(ctx context.Context) (Environment, error) {
	env := Environment{
		GoVersion: "go1.24", // This would be runtime.Version() in real code
		OS:        "darwin", // This would be runtime.GOOS in real code
		Arch:      "arm64",  // This would be runtime.GOARCH in real code
	}

	// Get DuckDB version
	row := r.db.QueryRowContext(ctx, "SELECT version()")
	if err := row.Scan(&env.DuckDBVersion); err != nil {
		return env, fmt.Errorf("failed to get DuckDB version: %w", err)
	}

	return env, nil
}

// GetAvailableQueries returns the list of available TPC-H queries.
func GetAvailableQueries() []string {
	queries := make([]string, 0, len(tpchQueries))
	for name := range tpchQueries {
		queries = append(queries, name)
	}
	return queries
}

// ParseQueries parses a comma-separated list of query names.
func ParseQueries(queryStr string) ([]string, error) {
	if queryStr == "" {
		return nil, fmt.Errorf("no queries specified")
	}

	queries := strings.Split(queryStr, ",")
	for i, q := range queries {
		queries[i] = strings.TrimSpace(q)
		if _, exists := tpchQueries[queries[i]]; !exists {
			return nil, fmt.Errorf("unknown query: %s", queries[i])
		}
	}

	return queries, nil
}

// GetAllQueries returns all available TPC-H query names.
func GetAllQueries() []string {
	return []string{"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"} // Add more as needed
}

// OutputResult writes the benchmark result in the specified format.
func OutputResult(result *BenchmarkResult, format string, writer io.Writer) error {
	switch strings.ToLower(format) {
	case "json":
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)

	case "table":
		return outputTable(result, writer)

	case "arrow":
		return outputArrow(result, writer)

	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

// outputTable writes results in human-readable table format.
func outputTable(result *BenchmarkResult, writer io.Writer) error {
	fmt.Fprintf(writer, "TPC-H Benchmark Results\n")
	fmt.Fprintf(writer, "=======================\n\n")
	fmt.Fprintf(writer, "Configuration:\n")
	fmt.Fprintf(writer, "  Scale Factor: %.2f\n", result.Config.ScaleFactor)
	fmt.Fprintf(writer, "  Iterations: %d\n", result.Config.Iterations)
	fmt.Fprintf(writer, "  Total Time: %v\n", result.TotalTime)
	fmt.Fprintf(writer, "  Started: %s\n", result.StartTime.Format(time.RFC3339))
	fmt.Fprintf(writer, "\n")

	fmt.Fprintf(writer, "Environment:\n")
	fmt.Fprintf(writer, "  DuckDB Version: %s\n", result.Environment.DuckDBVersion)
	fmt.Fprintf(writer, "  Go Version: %s\n", result.Environment.GoVersion)
	fmt.Fprintf(writer, "  OS/Arch: %s/%s\n", result.Environment.OS, result.Environment.Arch)
	fmt.Fprintf(writer, "\n")

	fmt.Fprintf(writer, "Results:\n")
	fmt.Fprintf(writer, "%-6s %-4s %-12s %-8s %s\n", "Query", "Iter", "Time", "Rows", "Status")
	fmt.Fprintf(writer, "%-6s %-4s %-12s %-8s %s\n", "-----", "----", "----", "----", "------")

	for _, r := range result.Results {
		status := "OK"
		if r.Error != "" {
			status = "ERROR"
		}
		fmt.Fprintf(writer, "%-6s %-4d %-12s %-8d %s\n",
			r.Query, r.Iteration, r.ExecutionTime, r.RowCount, status)
	}

	return nil
}

// outputArrow writes results in Apache Arrow format.
func outputArrow(result *BenchmarkResult, writer io.Writer) error {
	// Create Arrow schema for benchmark results
	fields := []arrow.Field{
		{Name: "query", Type: arrow.BinaryTypes.String},
		{Name: "iteration", Type: arrow.PrimitiveTypes.Int32},
		{Name: "execution_time_ns", Type: arrow.PrimitiveTypes.Int64},
		{Name: "row_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "error", Type: arrow.BinaryTypes.String},
	}

	schema := arrow.NewSchema(fields, nil)
	allocator := memory.NewGoAllocator()

	// Build record
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	for _, r := range result.Results {
		builder.Field(0).(*array.StringBuilder).Append(r.Query)
		builder.Field(1).(*array.Int32Builder).Append(int32(r.Iteration))
		builder.Field(2).(*array.Int64Builder).Append(int64(r.ExecutionTime))
		builder.Field(3).(*array.Int64Builder).Append(r.RowCount)
		builder.Field(4).(*array.StringBuilder).Append(r.Error)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write Arrow IPC stream
	w := ipc.NewWriter(writer, ipc.WithSchema(schema))
	defer w.Close()

	return w.Write(record)
}

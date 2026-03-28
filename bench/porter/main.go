package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var (
	scaleFlag = flag.Float64("scale", 0.1, "TPC-H scale factor (SF). Common values: 0.1, 1, 10, 100")
	regenFlag = flag.Bool("regen", true, "Run CALL dbgen to (re)generate the dataset")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	fmt.Printf("== Porter FlightSQL TPC-H Benchmark ==\n")
	fmt.Printf("Scale factor: %.2f | Regenerate: %v\n\n", *scaleFlag, *regenFlag)

	// Connect via FlightSQL ADBC driver
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver": "flightsql",
		"uri":    "grpc+tcp://127.0.0.1:32010",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// === TPC-H SETUP ===
	if *regenFlag {
		fmt.Println("Setting up / regenerating TPC-H at SF =", *scaleFlag)

		exec(ctx, conn, "INSTALL tpch")
		exec(ctx, conn, "LOAD tpch")

		// Add this right after connecting (before dbgen)
		exec(ctx, conn, "SET memory_limit = '64GB';") // or whatever your machine can handle
		exec(ctx, conn, "SET threads = 16;")          // match your CPU cores

		start := time.Now()
		exec(ctx, conn, fmt.Sprintf("CALL dbgen(sf=%.2f)", *scaleFlag))
		fmt.Printf("dbgen completed in %s\n", time.Since(start))
	} else {
		fmt.Println("Skipping data generation (using existing dataset)")
	}

	// === Queries ===
	queries := []struct {
		name string
		sql  string
	}{
		{"Q1 - Scan + Aggregate", `SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice) FROM lineitem GROUP BY l_returnflag, l_linestatus`},
		{"Q3 - Join", `SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey GROUP BY l_orderkey ORDER BY revenue DESC LIMIT 10`},
		{"Q6 - Filter", `SELECT SUM(l_extendedprice * l_discount) FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24`},
	}

	for _, q := range queries {
		fmt.Printf("\nRunning %s...\n", q.name)
		runQuery(ctx, conn, q.name, q.sql)
	}

	// === Throughput benchmark (exact format you requested) ===
	benchThroughput(ctx, conn)

	fmt.Println("\n✅ Done.")
}

func runQuery(ctx context.Context, conn adbc.Connection, name, sql string) {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(normalizeSQL(sql)); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatalf("query %s failed: %v", name, err)
	}

	rows, batches := drain(stream)
	elapsed := time.Since(start)

	fmt.Printf("Rows: %d | Batches: %d | Time: %s\n", rows, batches, elapsed)
}

func benchThroughput(ctx context.Context, conn adbc.Connection) {
	fmt.Println("\n== Throughput Benchmark (lineitem scan) ==")

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	sql := `
		SELECT
			l_orderkey,
			l_partkey,
			l_suppkey,
			l_quantity,
			l_extendedprice,
			l_discount
		FROM lineitem
	`

	if err := stmt.SetSqlQuery(normalizeSQL(sql)); err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}

	rows, batches := drain(stream)
	elapsed := time.Since(start)

	// rough row width estimate (kept from your original benchmark)
	bytesPerRow := 8 * 6
	totalBytes := int64(rows) * int64(bytesPerRow)

	rowsPerSec := float64(rows) / elapsed.Seconds()
	mbPerSec := float64(totalBytes) / (1024.0 * 1024.0) / elapsed.Seconds()

	// ─────────────────────────────────────
	// EXACT PRINT BLOCK YOU REQUESTED
	// ─────────────────────────────────────
	fmt.Println("\n--- Throughput Results ---")
	fmt.Printf("Rows:        %d\n", rows)
	fmt.Printf("Batches:     %d\n", batches)
	fmt.Printf("Time:        %s\n", elapsed)
	fmt.Printf("Rows/sec:    %.2f\n", rowsPerSec)
	fmt.Printf("MB/sec:      %.2f\n", mbPerSec)
}

// ─────────────────────────────────────
// Helpers
// ─────────────────────────────────────
func exec(ctx context.Context, conn adbc.Connection, sql string) {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(normalizeSQL(sql)); err != nil {
		log.Fatal(err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		log.Fatalf("exec failed: %v", err)
	}
}

func normalizeSQL(sql string) string {
	return strings.TrimSpace(strings.TrimSuffix(sql, ";"))
}

func drain(stream array.RecordReader) (int, int) {
	defer stream.Release()
	var rows, batches int
	for stream.Next() {
		rows += int(stream.Record().NumRows())
		batches++
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
	return rows, batches
}

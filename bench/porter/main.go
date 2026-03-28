package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func main() {
	ctx := context.Background()

	var drv drivermgr.Driver

	fmt.Println("== Porter FlightSQL Benchmark ==")

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

	fmt.Println("Setting up TPC-H...")

	exec(ctx, conn, "INSTALL tpch")
	exec(ctx, conn, "LOAD tpch")

	scale := 0.1
	exec(ctx, conn, fmt.Sprintf("CALL dbgen(sf=%.1f)", scale))

	// ----------------------------
	// TPC-H style queries
	// ----------------------------
	queries := []struct {
		name string
		sql  string
	}{
		{
			"Q1 - Scan + Aggregate",
			`
			SELECT
				l_returnflag,
				l_linestatus,
				SUM(l_quantity),
				SUM(l_extendedprice)
			FROM lineitem
			GROUP BY l_returnflag, l_linestatus
			`,
		},
		{
			"Q3 - Join",
			`
			SELECT
				l_orderkey,
				SUM(l_extendedprice * (1 - l_discount)) as revenue
			FROM customer, orders, lineitem
			WHERE
				c_mktsegment = 'BUILDING'
				AND c_custkey = o_custkey
				AND l_orderkey = o_orderkey
			GROUP BY l_orderkey
			ORDER BY revenue DESC
			LIMIT 10
			`,
		},
		{
			"Q6 - Filter",
			`
			SELECT
				SUM(l_extendedprice * l_discount)
			FROM lineitem
			WHERE
				l_shipdate >= DATE '1994-01-01'
				AND l_shipdate < DATE '1995-01-01'
				AND l_discount BETWEEN 0.05 AND 0.07
				AND l_quantity < 24
			`,
		},
	}

	for _, q := range queries {
		fmt.Printf("\nRunning %s...\n", q.name)

		stmt, err := conn.NewStatement()
		if err != nil {
			log.Fatal(err)
		}

		sql := normalizeSQL(q.sql)

		if err := stmt.SetSqlQuery(sql); err != nil {
			log.Fatal(err)
		}

		start := time.Now()

		stream, _, err := stmt.ExecuteQuery(ctx)
		if err != nil {
			log.Fatal(err)
		}

		rows, batches := drain(stream)
		elapsed := time.Since(start)

		fmt.Printf("Rows: %d | Batches: %d | Time: %s\n", rows, batches, elapsed)

		stmt.Close()
	}

	// ----------------------------
	// NEW: Throughput benchmark
	// ----------------------------
	benchThroughput(ctx, conn)

	fmt.Println("\nDone.")
}

// ----------------------------
// EXEC helper (more robust)
// ----------------------------
func exec(ctx context.Context, conn adbc.Connection, sql string) {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	sql = normalizeSQL(sql)

	if err := stmt.SetSqlQuery(sql); err != nil {
		log.Fatal(err)
	}

	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		log.Fatalf("exec failed [%s]: %v", sql, err)
	}
}

// ----------------------------
// FIX: strip semicolons safely
// ----------------------------
func normalizeSQL(sql string) string {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	return sql
}

// ----------------------------
// drain with batch tracking
// ----------------------------
func drain(stream array.RecordReader) (int, int) {
	defer stream.Release()

	var rows int
	var batches int

	for stream.Next() {
		rec := stream.Record()
		rows += int(rec.NumRows())
		batches++
	}

	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}

	return rows, batches
}

// ----------------------------
// NEW: throughput scan test
// ----------------------------
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

	// rough row width estimate (very useful signal)
	bytesPerRow := 8 * 6
	totalBytes := int64(rows) * int64(bytesPerRow)

	rowsPerSec := float64(rows) / elapsed.Seconds()
	mbPerSec := float64(totalBytes) / (1024.0 * 1024.0) / elapsed.Seconds()

	fmt.Println("\n--- Throughput Results ---")
	fmt.Printf("Rows:        %d\n", rows)
	fmt.Printf("Batches:     %d\n", batches)
	fmt.Printf("Time:        %s\n", elapsed)
	fmt.Printf("Rows/sec:    %.2f\n", rowsPerSec)
	fmt.Printf("MB/sec:      %.2f\n", mbPerSec)
}

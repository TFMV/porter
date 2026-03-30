// (same imports as before)
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// =========================
// Types
// =========================

type QueryRequest struct {
	Query string `json:"query"`
}

type runConfig struct {
	transport     string
	query         string
	clients       int
	duration      time.Duration
	timeout       time.Duration
	flightURI     string
	flightAdbcURI string
	wsURL         string
}

type opMetric struct {
	latency       time.Duration
	rows          int64
	payloadBytes  int64
	batches       int64
	errorOccurred bool
}

type aggregate struct {
	ops          int64
	success      int64
	errors       int64
	rows         int64
	payloadBytes int64
	latencies    []time.Duration
}

// =========================
// main
// =========================

func main() {
	transport := flag.String("transport", "both", "ws|flight|ingest|both")
	query := flag.String("query", "SELECT i FROM generate_series(1, 500000) as i", "SQL query")
	clients := flag.Int("clients", 4, "Concurrent clients per transport")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	timeout := flag.Duration("timeout", 2*time.Minute, "Per-op timeout")
	wsPort := flag.String("ws-port", "9090", "")
	flightPort := flag.String("flight-port", "32010", "")
	flag.Parse()

	cfg := runConfig{
		transport:     strings.ToLower(*transport),
		query:         *query,
		clients:       *clients,
		duration:      *duration,
		timeout:       *timeout,
		flightURI:     "127.0.0.1:" + *flightPort,
		flightAdbcURI: "grpc+tcp://127.0.0.1:" + *flightPort,
		wsURL:         (&url.URL{Scheme: "ws", Host: "127.0.0.1:" + *wsPort, Path: "/"}).String(),
	}

	run(cfg)
}

// =========================
// runner (CONCURRENT)
// =========================

func run(cfg runConfig) {
	var wg sync.WaitGroup

	runOne := func(name string, fn func(context.Context, *runConfig) opMetric) {
		defer wg.Done()
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // jitter
		agg := runLoad(cfg, fn)
		printSummary(name, agg, cfg.duration)
	}

	if cfg.transport == "ws" || cfg.transport == "both" {
		wg.Add(1)
		go runOne("WebSocket", runWSQuery)
	}

	if cfg.transport == "flight" || cfg.transport == "both" {
		wg.Add(1)
		go runOne("FlightSQL", runFlightQuery)
	}

	if cfg.transport == "ingest" || cfg.transport == "both" {
		wg.Add(1)
		go runOne("FlightSQL Ingest", runFlightIngest)
	}

	wg.Wait()
}

// =========================
// CORE LOAD LOOP (TIME-BASED)
// =========================

func runLoad(cfg runConfig, fn func(context.Context, *runConfig) opMetric) aggregate {
	var agg aggregate
	var mu sync.Mutex

	end := time.Now().Add(cfg.duration)

	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

			for time.Now().Before(end) {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				start := time.Now()

				m := fn(ctx, &cfg)
				cancel()

				m.latency = time.Since(start)

				mu.Lock()
				agg.ops++
				if m.errorOccurred {
					agg.errors++
					mu.Unlock()
					continue
				}

				agg.success++
				agg.rows += m.rows
				agg.payloadBytes += m.payloadBytes
				agg.latencies = append(agg.latencies, m.latency)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return agg
}

// =========================
// WebSocket
// =========================

func runWSQuery(ctx context.Context, cfg *runConfig) opMetric {
	conn, _, err := websocket.Dial(ctx, cfg.wsURL, nil)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err := wsjson.Write(ctx, conn, QueryRequest{Query: cfg.query}); err != nil {
		return opMetric{errorOccurred: true}
	}

	var rows int64

	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			break
		}

		r, _, _, err := decodeIPC(payload)
		if err != nil {
			return opMetric{errorOccurred: true}
		}
		rows += int64(r)
	}

	return opMetric{
		rows:         rows,
		payloadBytes: rows * 8,
	}
}

// =========================
// FlightSQL Query
// =========================

func runFlightQuery(ctx context.Context, cfg *runConfig) opMetric {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{"driver": "flightsql", "uri": cfg.flightAdbcURI})
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer conn.Close()

	stmt, _ := conn.NewStatement()
	defer stmt.Close()

	stmt.SetSqlQuery(cfg.query)

	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer stream.Release()

	var rows int64

	for stream.Next() {
		rows += stream.RecordBatch().NumRows()
	}

	return opMetric{
		rows:         rows,
		payloadBytes: rows * 8,
	}
}

// =========================
// FlightSQL Ingest
// =========================

func runFlightIngest(ctx context.Context, cfg *runConfig) opMetric {
	reader, _, err := buildIngestReader()
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer reader.Release()

	client, err := flightsql.NewClient(cfg.flightURI, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer client.Close()

	rows, err := client.ExecuteIngest(ctx, reader, &flightsql.ExecuteIngestOpts{
		Table: "benchmark_ingest",
	})
	if err != nil {
		return opMetric{errorOccurred: true}
	}

	return opMetric{
		rows:         rows,
		payloadBytes: rows * 16,
	}
}

// =========================
// Arrow helpers
// =========================

func buildIngestReader() (array.RecordReader, int, error) {
	const batchSize = 32768
	const numBatches = 32

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	var batches []arrow.Record

	for i := 0; i < numBatches; i++ {
		b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		ids := make([]int64, batchSize)
		for j := range ids {
			ids[j] = int64(i*batchSize + j)
		}
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		batches = append(batches, b.NewRecord())
		b.Release()
	}

	r, err := array.NewRecordReader(schema, batches)
	return r, numBatches, err
}

func decodeIPC(payload []byte) (int, int, int64, error) {
	r, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return 0, 0, 0, err
	}
	defer r.Release()

	var rows int
	for r.Next() {
		rows += int(r.RecordBatch().NumRows())
	}
	return rows, 0, 0, r.Err()
}

// =========================
// Reporting
// =========================

func printSummary(name string, agg aggregate, dur time.Duration) {
	if len(agg.latencies) == 0 {
		fmt.Println(name, "no data")
		return
	}

	sort.Slice(agg.latencies, func(i, j int) bool {
		return agg.latencies[i] < agg.latencies[j]
	})

	p50 := percentile(agg.latencies, 0.5)
	p99 := percentile(agg.latencies, 0.99)

	fmt.Printf("\n=== %s ===\n", name)
	fmt.Printf("Ops: %d success=%d errors=%d\n", agg.ops, agg.success, agg.errors)
	fmt.Printf("Rows/sec: %.2f\n", float64(agg.rows)/dur.Seconds())
	fmt.Printf("Throughput: %.2f MB/s\n", float64(agg.payloadBytes)/dur.Seconds()/1024/1024)
	fmt.Printf("Latency p50/p99: %s / %s\n", p50, p99)
}

func percentile(values []time.Duration, p float64) time.Duration {
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

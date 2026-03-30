package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
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
	iterations    int
	timeout       time.Duration
	flightURI     string
	flightAdbcURI string
	wsURL         string
	cpuProfile    string
	heapProfile   string
}

type opMetric struct {
	latency       time.Duration
	firstByte     time.Duration
	readDuration  time.Duration
	decodeTime    time.Duration
	rows          int64
	payloadBytes  int64
	logicalBytes  int64
	batches       int64
	errorOccurred bool
}

type aggregate struct {
	ops          int64
	success      int64
	errors       int64
	rows         int64
	batches      int64
	payloadBytes int64
	logicalBytes int64
	decodeNanos  int64
	readNanos    int64

	latencies  []time.Duration
	firstBytes []time.Duration
}

// =========================
// main
// =========================

func main() {
	transport := flag.String("transport", "both", "ws|flight|ingest|both")
	query := flag.String("query", "SELECT i FROM generate_series(1, 1000000) t(i)", "SQL query")
	clients := flag.Int("clients", 2, "Concurrent clients")
	iterations := flag.Int("iterations", 5, "Queries per client")
	timeout := flag.Duration("timeout", 2*time.Minute, "Per-query timeout")
	wsPort := flag.String("ws-port", "8080", "WS port")
	flightPort := flag.String("flight-port", "32010", "FlightSQL port")
	cpuProfile := flag.String("cpu-profile", "", "CPU profile")
	heapProfile := flag.String("heap-profile", "", "Heap profile")
	flag.Parse()

	cfg := runConfig{
		transport:     strings.ToLower(*transport),
		query:         *query,
		clients:       *clients,
		iterations:    *iterations,
		timeout:       *timeout,
		flightURI:     "127.0.0.1:" + *flightPort,
		flightAdbcURI: "grpc+tcp://127.0.0.1:" + *flightPort,
		wsURL:         (&url.URL{Scheme: "ws", Host: "127.0.0.1:" + *wsPort, Path: "/"}).String(),
		cpuProfile:    *cpuProfile,
		heapProfile:   *heapProfile,
	}

	if cfg.cpuProfile != "" {
		f, _ := os.Create(cfg.cpuProfile)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	run(cfg)

	if cfg.heapProfile != "" {
		f, _ := os.Create(cfg.heapProfile)
		defer f.Close()
		runtime.GC()
		pprof.WriteHeapProfile(f)
	}
}

// =========================
// runner
// =========================

func run(cfg runConfig) {
	if cfg.transport == "ws" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runWSQuery)
		printSummary("WebSocket", agg, wall)
	}

	if cfg.transport == "flight" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runFlightQuery)
		printSummary("FlightSQL", agg, wall)
	}

	if cfg.transport == "ingest" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runFlightIngest)
		printSummary("FlightSQL Ingest", agg, wall)
	}
}

// =========================
// Core benchmark harness
// =========================

func runConcurrent(cfg runConfig, fn func(context.Context, *runConfig) opMetric) (aggregate, time.Duration) {
	var agg aggregate
	agg.latencies = make([]time.Duration, 0, cfg.clients*cfg.iterations)
	agg.firstBytes = make([]time.Duration, 0, cfg.clients*cfg.iterations)

	var mu sync.Mutex
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.clients; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			local := make([]opMetric, 0, cfg.iterations)

			for j := 0; j < cfg.iterations; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				m := fn(ctx, &cfg)
				cancel()
				local = append(local, m)
			}

			mu.Lock()
			defer mu.Unlock()

			for _, m := range local {
				agg.ops++
				if m.errorOccurred {
					agg.errors++
					continue
				}

				agg.success++
				agg.rows += m.rows
				agg.batches += m.batches
				agg.payloadBytes += m.payloadBytes
				agg.logicalBytes += m.logicalBytes
				agg.decodeNanos += int64(m.decodeTime)
				agg.readNanos += int64(m.readDuration)

				agg.latencies = append(agg.latencies, m.latency)
				agg.firstBytes = append(agg.firstBytes, m.firstByte)
			}
		}()
	}

	wg.Wait()
	return agg, time.Since(start)
}

// =========================
// WebSocket benchmark
// =========================

func runWSQuery(ctx context.Context, cfg *runConfig) opMetric {
	start := time.Now()

	conn, _, err := websocket.Dial(ctx, cfg.wsURL, nil)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	conn.SetReadLimit(100 * 1024 * 1024)

	if err := wsjson.Write(ctx, conn, QueryRequest{Query: cfg.query}); err != nil {
		return opMetric{errorOccurred: true}
	}

	var (
		firstByte time.Duration
		firstSeen bool
		m         opMetric
		readStart = time.Now()
	)

	for {
		msgType, payload, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
				errors.Is(err, context.Canceled) {
				break
			}
			return opMetric{errorOccurred: true}
		}

		if !firstSeen {
			firstByte = time.Since(start)
			firstSeen = true
		}

		if msgType != websocket.MessageBinary {
			continue
		}

		decodeStart := time.Now()
		rows, batches, logicalBytes, err := decodeIPC(payload)
		if err != nil {
			return opMetric{errorOccurred: true}
		}
		m.decodeTime += time.Since(decodeStart)

		m.rows += int64(rows)
		m.batches += int64(batches)
		m.payloadBytes += int64(len(payload))
		m.logicalBytes += logicalBytes
	}

	m.latency = time.Since(start)
	m.readDuration = time.Since(readStart)
	m.firstByte = firstByte

	return m
}

// =========================
// FlightSQL QUERY benchmark
// =========================

func runFlightQuery(ctx context.Context, cfg *runConfig) opMetric {
	start := time.Now()

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

	stmt, err := conn.NewStatement()
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(cfg.query); err != nil {
		return opMetric{errorOccurred: true}
	}

	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer stream.Release()

	var m opMetric
	readStart := time.Now()

	for stream.Next() {
		rec := stream.RecordBatch()
		m.rows += rec.NumRows()
		m.batches++
		m.logicalBytes += logicalRecordBytes(rec)
	}

	m.latency = time.Since(start)
	m.readDuration = time.Since(readStart)
	m.payloadBytes = m.logicalBytes

	return m
}

// =========================
// 🚀 NEW: FlightSQL INGEST benchmark
// =========================

func runFlightIngest(ctx context.Context, cfg *runConfig) opMetric {
	start := time.Now()

	reader, batchCount, err := buildIngestReader()
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer reader.Release()

	flightClient, err := flightsql.NewClient(cfg.flightURI, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("FlightSQL: failed to create client: %v\n", err)
		return opMetric{errorOccurred: true}
	}
	defer flightClient.Close()

	ingestStart := time.Now()

	rows, err := flightClient.ExecuteIngest(ctx, reader, &flightsql.ExecuteIngestOpts{
		Table: "benchmark_ingest",
		TableDefinitionOptions: &flightsql.TableDefinitionOptions{
			IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionReplace,
		},
	})

	if err != nil {
		fmt.Printf("FlightSQL ingest error: %v\n", err)
		return opMetric{errorOccurred: true}
	}

	return opMetric{
		latency:      time.Since(start),
		readDuration: time.Since(ingestStart),
		rows:         rows,
		batches:      int64(batchCount),
		payloadBytes: estimateReaderBytes(),
		logicalBytes: estimateReaderBytes(),
	}
}

// =========================
// Arrow ingest dataset
// =========================

func buildIngestReader() (array.RecordReader, int, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.String},
	}

	schema := arrow.NewSchema(fields, nil)

	makeBatch := func(start, size int64) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		defer b.Release()

		ids := make([]int64, size)
		vals := make([]string, size)

		for i := int64(0); i < size; i++ {
			ids[i] = start + i
			vals[i] = "v"
		}

		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(vals, nil)

		return b.NewRecordBatch()
	}

	b1 := makeBatch(0, 5000)
	b2 := makeBatch(5000, 5000)
	b3 := makeBatch(10000, 5000)

	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{b1, b2, b3})
	if err != nil {
		return nil, 0, err
	}

	return reader, 3, nil
}

func estimateReaderBytes() int64 {
	return 5000 * 3 * 16
}

// =========================
// Decode helpers
// =========================

func decodeIPC(payload []byte) (int, int, int64, error) {
	r, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return 0, 0, 0, err
	}
	defer r.Release()

	var rows, batches int
	var logical int64

	for r.Next() {
		rec := r.RecordBatch()
		rows += int(rec.NumRows())
		batches++
		logical += logicalRecordBytes(rec)
	}

	return rows, batches, logical, r.Err()
}

func logicalRecordBytes(rec arrow.RecordBatch) int64 {
	var total int64
	for i := 0; i < int(rec.NumCols()); i++ {
		total += logicalArrayBytes(rec.Column(i))
	}
	return total
}

func logicalArrayBytes(arr arrow.Array) int64 {
	if arr == nil {
		return 0
	}

	var total int64
	for _, b := range arr.Data().Buffers() {
		if b != nil {
			total += int64(b.Len())
		}
	}
	return total
}

// =========================
// Reporting
// =========================

func printSummary(name string, agg aggregate, wall time.Duration) {
	if len(agg.latencies) == 0 {
		fmt.Printf("\n%s: no successful ops\n", name)
		return
	}

	sort.Slice(agg.latencies, func(i, j int) bool { return agg.latencies[i] < agg.latencies[j] })

	p50 := percentile(agg.latencies, 0.5)
	p95 := percentile(agg.latencies, 0.95)
	p99 := percentile(agg.latencies, 0.99)

	throughput := float64(agg.payloadBytes) / wall.Seconds() / 1024 / 1024
	rowsPerSec := float64(agg.rows) / wall.Seconds()

	fmt.Printf("\n=== %s ===\n", name)
	fmt.Printf("Ops: %d success=%d errors=%d\n", agg.ops, agg.success, agg.errors)
	fmt.Printf("Rows/sec: %.2f\n", rowsPerSec)
	fmt.Printf("Throughput: %.2f MB/s\n", throughput)
	fmt.Printf("Latency p50/p95/p99: %s / %s / %s\n",
		p50.Round(time.Millisecond),
		p95.Round(time.Millisecond),
		p99.Round(time.Millisecond),
	)
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

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
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type runConfig struct {
	transport   string
	query       string
	clients     int
	iterations  int
	timeout     time.Duration
	flightURI   string
	wsURL       string
	cpuProfile  string
	heapProfile string
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
	latencies    []time.Duration
	firstBytes   []time.Duration
}

func main() {
	transport := flag.String("transport", "both", "Transport to benchmark: ws|flight|both")
	query := flag.String("query", "SELECT i AS id, i*2 AS v1, i*3 AS v2, i*4 AS v3 FROM generate_series(1, 2000000) AS t(i)", "SQL query")
	clients := flag.Int("clients", 1, "Concurrent clients")
	iterations := flag.Int("iterations", 5, "Queries per client")
	timeout := flag.Duration("timeout", 2*time.Minute, "Per-query timeout")
	wsPort := flag.String("ws-port", "8080", "WebSocket server port")
	flightPort := flag.String("flight-port", "32010", "FlightSQL server port")
	batchSizes := flag.String("batch-sizes", "", "Comma-separated generate_series row counts to benchmark")
	cpuProfile := flag.String("cpu-profile", "", "Write CPU profile to file")
	heapProfile := flag.String("heap-profile", "", "Write heap profile to file")
	flag.Parse()

	cfg := runConfig{
		transport:   strings.ToLower(strings.TrimSpace(*transport)),
		query:       *query,
		clients:     *clients,
		iterations:  *iterations,
		timeout:     *timeout,
		flightURI:   "grpc+tcp://127.0.0.1:" + *flightPort,
		wsURL:       (&url.URL{Scheme: "ws", Host: "127.0.0.1:" + *wsPort, Path: "/"}).String(),
		cpuProfile:  *cpuProfile,
		heapProfile: *heapProfile,
	}

	if cfg.cpuProfile != "" {
		f, err := os.Create(cfg.cpuProfile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	queries := []string{cfg.query}
	if strings.TrimSpace(*batchSizes) != "" {
		queries = queries[:0]
		for _, raw := range strings.Split(*batchSizes, ",") {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			queries = append(queries, fmt.Sprintf("SELECT i AS id, i*2 AS v1, i*3 AS v2, i*4 AS v3 FROM generate_series(1, %s) AS t(i)", raw))
		}
	}

	for i, q := range queries {
		fmt.Printf("\n===== Benchmark %d/%d =====\n", i+1, len(queries))
		fmt.Printf("Query: %s\n", q)
		cfg.query = q
		runSet(cfg)
	}

	if cfg.heapProfile != "" {
		f, err := os.Create(cfg.heapProfile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(err)
		}
	}
}

func runSet(cfg runConfig) {
	if cfg.transport == "ws" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runWSQuery)
		printSummary("WebSocket", agg, wall)
	}
	if cfg.transport == "flight" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runFlightQuery)
		printSummary("FlightSQL(gRPC)", agg, wall)
	}
}

func runConcurrent(cfg runConfig, fn func(context.Context, *runConfig) opMetric) (aggregate, time.Duration) {
	var agg aggregate
	agg.latencies = make([]time.Duration, 0, cfg.clients*cfg.iterations)
	agg.firstBytes = make([]time.Duration, 0, cfg.clients*cfg.iterations)
	var mu sync.Mutex

	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < cfg.clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < cfg.iterations; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				m := fn(ctx, &cfg)
				cancel()

				atomic.AddInt64(&agg.ops, 1)
				if m.errorOccurred {
					atomic.AddInt64(&agg.errors, 1)
					continue
				}
				atomic.AddInt64(&agg.success, 1)
				atomic.AddInt64(&agg.rows, m.rows)
				atomic.AddInt64(&agg.batches, m.batches)
				atomic.AddInt64(&agg.payloadBytes, m.payloadBytes)
				atomic.AddInt64(&agg.logicalBytes, m.logicalBytes)
				atomic.AddInt64(&agg.decodeNanos, int64(m.decodeTime))
				atomic.AddInt64(&agg.readNanos, int64(m.readDuration))

				mu.Lock()
				agg.latencies = append(agg.latencies, m.latency)
				agg.firstBytes = append(agg.firstBytes, m.firstByte)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return agg, time.Since(start)
}

func runWSQuery(ctx context.Context, cfg *runConfig) opMetric {
	start := time.Now()
	conn, _, err := websocket.Dial(ctx, cfg.wsURL, nil)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err := wsjson.Write(ctx, conn, QueryRequest{Query: cfg.query}); err != nil {
		return opMetric{errorOccurred: true}
	}

	var schemaRead bool
	readStart := time.Now()
	m := opMetric{}

	for {
		msgType, payload, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure || errors.Is(err, context.Canceled) {
				break
			}
			return opMetric{errorOccurred: true}
		}
		if m.firstByte == 0 {
			m.firstByte = time.Since(start)
		}
		if msgType == websocket.MessageText {
			schemaRead = true
			continue
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
		break
	}

	if !schemaRead {
		return opMetric{errorOccurred: true}
	}
	m.latency = time.Since(start)
	m.readDuration = time.Since(readStart)
	return m
}

func runFlightQuery(ctx context.Context, cfg *runConfig) opMetric {
	start := time.Now()
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{"driver": "flightsql", "uri": cfg.flightURI})
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

	m := opMetric{}
	readStart := time.Now()
	for stream.Next() {
		if m.firstByte == 0 {
			m.firstByte = time.Since(start)
		}
		rec := stream.RecordBatch()
		decodeStart := time.Now()
		m.rows += rec.NumRows()
		m.batches++
		m.logicalBytes += logicalRecordBytes(rec)
		m.decodeTime += time.Since(decodeStart)
	}
	if err := stream.Err(); err != nil {
		return opMetric{errorOccurred: true}
	}
	m.readDuration = time.Since(readStart)
	m.latency = time.Since(start)
	m.payloadBytes = m.logicalBytes
	return m
}

func decodeIPC(payload []byte) (rows int, batches int, logicalBytes int64, err error) {
	r, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		return 0, 0, 0, err
	}
	defer r.Release()

	for r.Next() {
		rec := r.Record()
		if rec == nil {
			continue
		}
		rows += int(rec.NumRows())
		batches++
		logicalBytes += logicalRecordBytes(rec)
		rec.Release()
	}
	return rows, batches, logicalBytes, r.Err()
}

func logicalRecordBytes(rec arrow.Record) int64 {
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
	data := arr.Data()
	var total int64
	for _, b := range data.Buffers() {
		if b != nil {
			total += int64(b.Len())
		}
	}
	return total
}

func printSummary(name string, agg aggregate, wall time.Duration) {
	if len(agg.latencies) == 0 {
		fmt.Printf("\n%s: no successful operations\n", name)
		return
	}
	sort.Slice(agg.latencies, func(i, j int) bool { return agg.latencies[i] < agg.latencies[j] })
	sort.Slice(agg.firstBytes, func(i, j int) bool { return agg.firstBytes[i] < agg.firstBytes[j] })

	p50 := percentile(agg.latencies, 0.50)
	p95 := percentile(agg.latencies, 0.95)
	p99 := percentile(agg.latencies, 0.99)
	firstP50 := percentile(agg.firstBytes, 0.50)
	throughputMB := float64(agg.payloadBytes) / wall.Seconds() / 1024.0 / 1024.0
	rowsPerSec := float64(agg.rows) / wall.Seconds()
	avgBatchesPerQuery := float64(agg.batches) / float64(agg.success)
	decodePct := 100 * (float64(agg.decodeNanos) / float64(int64(wall)))
	readPct := 100 * (float64(agg.readNanos) / float64(int64(wall)))
	logicalMB := float64(agg.logicalBytes) / 1024.0 / 1024.0

	fmt.Printf("\n=== %s ===\n", name)
	fmt.Printf("Ops: %d (success=%d errors=%d)\n", agg.ops, agg.success, agg.errors)
	fmt.Printf("Rows: %d | Batches: %d | Avg batches/query: %.2f\n", agg.rows, agg.batches, avgBatchesPerQuery)
	fmt.Printf("Elapsed: %s\n", wall.Round(time.Millisecond))
	fmt.Printf("Rows/sec: %.2f\n", rowsPerSec)
	fmt.Printf("Payload throughput: %.2f MB/s\n", throughputMB)
	fmt.Printf("Logical bytes: %.2f MB\n", logicalMB)
	fmt.Printf("Latency p50/p95/p99: %s / %s / %s\n", p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))
	fmt.Printf("First-byte latency p50: %s\n", firstP50.Round(time.Millisecond))
	fmt.Printf("Decode CPU wall ratio: %.1f%%\n", decodePct)
	fmt.Printf("Read-time wall ratio: %.1f%%\n", readPct)
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

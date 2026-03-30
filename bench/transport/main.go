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

	latencies  []time.Duration
	firstBytes []time.Duration
}

func main() {
	transport := flag.String("transport", "both", "ws|flight|both")
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
		transport:   strings.ToLower(*transport),
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

func run(cfg runConfig) {
	if cfg.transport == "ws" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runWSQuery)
		printSummary("WebSocket", agg, wall)
	}
	if cfg.transport == "flight" || cfg.transport == "both" {
		agg, wall := runConcurrent(cfg, runFlightQuery)
		printSummary("FlightSQL", agg, wall)
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
// (schema logic preserved exactly)
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
		schemaRead bool
		firstByte  time.Duration
		firstSeen  bool
		m          opMetric
		readStart  = time.Now()
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
	}

	if !schemaRead {
		return opMetric{errorOccurred: true}
	}

	m.latency = time.Since(start)
	m.readDuration = time.Since(readStart)
	m.firstByte = firstByte

	return m
}

// =========================
// FlightSQL benchmark
// =========================

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

	_ = stmt.SetSqlQuery(cfg.query)

	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return opMetric{errorOccurred: true}
	}
	defer stream.Release()

	var m opMetric
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
		if rec == nil {
			continue
		}
		rows += int(rec.NumRows())
		batches++
		logical += logicalRecordBytes(rec)
		rec.Release()
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
	sort.Slice(agg.firstBytes, func(i, j int) bool { return agg.firstBytes[i] < agg.firstBytes[j] })

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

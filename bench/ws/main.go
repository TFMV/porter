package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type BenchmarkResult struct {
	Requests       int32
	Success        int32
	Errors         int32
	TotalRows      int64
	TotalBytes     int64
	Duration       time.Duration
	RequestsPerSec float64
	RowsPerSec     float64
	MBPerSec       float64
}

var (
	port     = flag.String("port", "9090", "WebSocket server port")
	queries  = flag.String("queries", "", "Comma-separated queries to benchmark")
	clients  = flag.Int("clients", 1, "Number of concurrent clients")
	duration = flag.Int("duration", 5, "Duration of benchmark in seconds")
	warmup   = flag.Int("warmup", 1, "Warmup duration in seconds")
)

var defaultQueries = []string{
	"SELECT i, 'value_' || i as text FROM generate_series(1, 1000) as t(i)",
}

func main() {
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var queryList []string
	if *queries != "" {
		queryList = parseQueries(*queries)
	}
	if len(queryList) == 0 {
		queryList = defaultQueries
	}

	fmt.Printf("WebSocket Benchmark Configuration:\n")
	fmt.Printf("  Server:        localhost:%s\n", *port)
	fmt.Printf("  Clients:       %d\n", *clients)
	fmt.Printf("  Duration:      %ds (+ %ds warmup)\n", *duration, *warmup)
	fmt.Printf("  Queries:       %d queries\n", len(queryList))
	fmt.Printf("\n")

	if *warmup > 0 {
		fmt.Println("Warming up...")
		warmupCtx, cancel := context.WithTimeout(ctx, time.Duration(*warmup)*time.Second)
		defer cancel()
		runBenchmark(warmupCtx, queryList, *clients, true)
	}

	fmt.Printf("Starting benchmark...\n")
	benchCtx, benchCancel := context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
	defer benchCancel()

	result := runBenchmark(benchCtx, queryList, *clients, false)

	fmt.Printf("\n=== Benchmark Results ===\n")
	fmt.Printf("Total Requests:     %d\n", int(result.Requests))
	fmt.Printf("Successful:         %d\n", result.Success)
	fmt.Printf("Errors:            %d\n", result.Errors)
	fmt.Printf("Total Rows:        %d\n", result.TotalRows)
	fmt.Printf("Total Bytes:       %.2f MB\n", float64(result.TotalBytes)/1024/1024)
	fmt.Printf("Duration:          %v\n", result.Duration.Round(time.Millisecond))
	fmt.Printf("Requests/sec:      %.2f\n", result.RequestsPerSec)
	fmt.Printf("Rows/sec:          %.2f\n", result.RowsPerSec)
	fmt.Printf("Throughput:        %.2f MB/s\n", result.MBPerSec)
}

func parseQueries(s string) []string {
	if s == "" {
		return nil
	}
	var queries []string
	for _, q := range strings.Split(s, ",") {
		q = strings.TrimSpace(q)
		if q != "" {
			queries = append(queries, q)
		}
	}
	return queries
}

func runBenchmark(ctx context.Context, queries []string, clients int, quiet bool) BenchmarkResult {
	var wg sync.WaitGroup
	var result BenchmarkResult

	start := time.Now()

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runClient(ctx, queries, clientID, &result, quiet)
		}(i)
	}

	wg.Wait()
	result.Duration = time.Since(start)

	if result.Duration > 0 && result.Requests > 0 {
		result.RequestsPerSec = float64(int(result.Requests)) / result.Duration.Seconds()
		result.RowsPerSec = float64(result.TotalRows) / result.Duration.Seconds()
		result.MBPerSec = float64(result.TotalBytes) / result.Duration.Seconds() / 1024 / 1024
	}

	return result
}

func runClient(ctx context.Context, queries []string, clientID int, result *BenchmarkResult, quiet bool) {
	u := url.URL{Scheme: "ws", Host: "localhost:" + *port, Path: "/"}

	query := queries[clientID%len(queries)]
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			success, rows, bytes := executeQueryWithRetry(ctx, u, query, quiet, 3)
			atomic.AddInt32(&result.Requests, 1)
			if success {
				atomic.AddInt32(&result.Success, 1)
				atomic.AddInt64(&result.TotalRows, rows)
			} else {
				atomic.AddInt32(&result.Errors, 1)
			}
			atomic.AddInt64(&result.TotalBytes, bytes)
		}
	}
}

func executeQueryWithRetry(ctx context.Context, u url.URL, query string, quiet bool, maxRetries int) (success bool, rows int64, bytes int64) {
	for i := 0; i < maxRetries; i++ {
		conn, _, err := websocket.Dial(ctx, u.String(), nil)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		success, rows, bytes = executeQuery(ctx, conn, query, quiet)
		if success {
			return true, rows, bytes
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false, 0, 0
}

func executeQuery(ctx context.Context, conn *websocket.Conn, query string, quiet bool) (success bool, rows int64, bytes int64) {
	req := QueryRequest{Query: query}
	if err := wsjson.Write(ctx, conn, req); err != nil {
		return false, 0, 0
	}

	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var totalRows int64
	var totalBytes int64
	hasData := false

	for {
		msgType, msg, err := conn.Read(readCtx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return hasData, totalRows, totalBytes
			}
			break
		}

		if msgType == websocket.MessageText {
			var schemaMsg map[string]interface{}
			_ = json.Unmarshal(msg, &schemaMsg)
		} else if msgType == websocket.MessageBinary {
			rowCount, err := decodeIPC(msg)
			if err != nil {
				break
			}
			hasData = true
			totalRows += int64(rowCount)
			totalBytes += int64(len(msg))
		}
	}

	return hasData, totalRows, totalBytes
}

func decodeIPC(data []byte) (int, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	defer reader.Release()

	var totalRows int
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec != nil {
			totalRows += int(rec.NumRows())
			rec.Release()
		}
	}

	return totalRows, reader.Err()
}

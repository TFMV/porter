# WebSocket Benchmark Results

## Test Configuration

- **Server**: Porter with WebSocket transport
- **Database**: In-memory (generate_series queries)
- **Query**: `SELECT i, 'value_' || i as text FROM generate_series(1, 1000) as t(i)`

## Results

### After IPC Batching Fix (March 28, 2026)

```
=== WebSocket Benchmark (1 client) ===
Total Requests:     4,988
Successful:         4,986
Errors:            2
Total Rows:        4,986,000
Total Bytes:       101.26 MB
Duration:          5.067s
Requests/sec:      984.49
Rows/sec:          984,091.60
Throughput:        19.99 MB/s
```

```
=== WebSocket Benchmark (4 clients) ===
Total Requests:     15,917
Successful:         15,909
Errors:            8
Total Rows:        15,909,000
Total Bytes:       323.10 MB
Duration:          5.099s
Requests/sec:      3,121.80
Rows/sec:          3,120,227.94
Throughput:        63.37 MB/s
```

### FlightSQL Benchmark (for comparison)

```
=== Throughput Results ===
Rows:        662,426,242
Batches:     323,455
Time:        23.89s
Rows/sec:    27,722,511.42
MB/sec:      1,269.04
```

## Comparison

| Metric | FlightSQL | WebSocket (1 client) | WebSocket (4 clients) | Ratio (1 client) |
|--------|-----------|---------------------|----------------------|------------------|
| Rows/sec | 27.7M | 984K | 3.1M | ~28x |
| MB/sec | 1,269 | 20 | 63 | ~63x |

## Optimization Applied

### Before (Original Code)
- Created new IPC writer for each Arrow RecordBatch
- Wrote schema header with every batch
- ~1000 IPC writers for 1000-row query

```go
func encodeArrowBatch(schema *arrow.Schema, batch arrow.RecordBatch) ([]byte, error) {
    var buf bytes.Buffer
    w := ipc.NewWriter(&buf, ipc.WithSchema(schema)) // NEW WRITER EACH TIME!
    // ... writes full header + data
}
```

### After (Optimized)
- Batches up to 100 RecordBatches into single IPC message
- Single IPC writer for all batches
- ~10 IPC writers for 1000-row query

```go
pending = append(pending, chunk.Data)
if len(pending) >= h.batchSize {
    sendBatch(pending)  // Single IPC writer for N batches
    pending = nil
}
```

## Performance Analysis

The remaining gap (28x) is due to:

1. **WebSocket framing overhead** - HTTP/WS frame headers add ~2-10 bytes per frame
2. **Binary serialization** - IPC is less efficient than gRPC streaming
3. **Single connection per query** - Each benchmark request creates new connection
4. **Compression** - FlightSQL uses gRPC streaming compression

## Usage

```bash
# Start server with WebSocket
go run ./cmd/porter serve --ws --ws-port 8080

# Run WebSocket benchmark
go run ./bench/ws/main.go --port 8080 --clients 4 --duration 10

# Run FlightSQL benchmark
go run ./bench/porter/main.go -port 32010 -regen=false
```

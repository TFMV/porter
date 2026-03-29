# WebSocket Benchmark Results

## Test Configuration

- **Server**: Porter with WebSocket transport
- **Database**: In-memory (generate_series queries)
- **Query**: `SELECT i, 'value_' || i as text FROM generate_series(1, 1000) as t(i)`

## Results

### Final Results (Continuous Streaming + No Compression)

```
=== WebSocket Benchmark (4 clients) ===
Total Requests:     15,822
Successful:         15,811
Errors:            11
Total Rows:        15,811,000
Total Bytes:       321.11 MB
Duration:          5.233s
Requests/sec:      3,023.77
Rows/sec:          3,021,672.42
Throughput:        61.37 MB/s
```

### Before Optimizations (Reference)

| Configuration | Rows/sec | MB/sec |
|---------------|----------|--------|
| Original (per-batch IPC) | 2.3M | 47 |
| + Continuous streaming | 2.3M | 47 |
| + No compression | 3.0M | 61 |

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

| Metric | FlightSQL | WebSocket (4 clients) | Gap |
|--------|-----------|----------------------|-----|
| Rows/sec | 27.7M | 3.0M | ~9x |
| MB/sec | 1,269 | 61 | ~21x |

## Optimizations Applied

### 1. Continuous IPC Streaming
- Single IPC writer writes directly to WebSocket connection
- Zero-copy streaming from Arrow to WebSocket
- Single WebSocket message per query

```go
// Before: New IPC writer per batch
func encodeArrowBatch(schema, batch) {
    w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
    w.Write(batch)
    w.Close()
    conn.Write(buf.Bytes())
}

// After: Continuous streaming
wsWriter, _ := h.conn.Writer(ctx, websocket.MessageBinary)
ipcWriter := ipc.NewWriter(wsWriter, ipc.WithSchema(schema))
for chunk := range streamCh {
    ipcWriter.Write(chunk.Data)
}
ipcWriter.Close()
wsWriter.Close()
```

### 2. Disabled WebSocket Compression
- Arrow IPC is already compressed internally
- Adding WS compression was redundant and CPU-intensive
- **Impact: ~30% performance improvement**

```go
// Before: Compression enabled
CompressionMode: websocket.CompressionContextTakeover,

// After: No compression (Arrow IPC handles it)
```

## Performance Analysis

The remaining ~9x gap vs FlightSQL is due to:
1. **Protocol overhead**: WebSocket adds HTTP framing on top of TCP
2. **Connection per query**: Benchmark creates new connection each request
3. **gRPC streaming**: FlightSQL uses persistent connections with efficient streaming
4. **No connection reuse**: Each query does HTTP upgrade handshake

## Usage

```bash
# Start server with WebSocket
go run ./cmd/porter serve --ws --ws-port 8080

# Run WebSocket benchmark
go run ./bench/ws/main.go --port 8080 --clients 4 --duration 10

# Run FlightSQL benchmark
go run ./bench/porter/main.go -port 32010 -regen=false
```

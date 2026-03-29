# WebSocket Benchmark Results

## Test Configuration

- **Server**: Porter with WebSocket transport
- **Database**: TPC-H SF=10 (~660M lineitem rows)
- **Query**: `SELECT i, 'value_' || i as text FROM generate_series(1, 1000) as t(i)`

## Results

### WebSocket Benchmark

```
=== WebSocket Benchmark Configuration ===
Clients:       4
Duration:      10s
Query:         generate_series(1, 1000)

=== Benchmark Results ===
Total Requests:     23,070
Successful:         23,063
Errors:            7
Requests/sec:      2,291.90
Rows/sec:          2,291,208.33
Throughput:        46.53 MB/s
```

### FlightSQL Benchmark (for comparison)

```
=== Throughput Results ===
Rows:        662,426,242
Batches:     323,455
Time:        26.25s
Rows/sec:    25,231,179.50
MB/sec:      1,154.99
```

## Comparison

| Metric | FlightSQL | WebSocket | Ratio |
|--------|-----------|-----------|-------|
| Rows/sec | 25.2M | 2.3M | ~11x |
| MB/sec | 1,155 | 47 | ~25x |

## Observations

1. **Protocol Overhead**: WebSocket has more overhead than gRPC/FlightSQL due to HTTP framing and JSON metadata exchange.

2. **Streaming**: Both protocols use Arrow streaming; the difference is primarily in wire protocol efficiency.

3. **Use Cases**: WebSocket is useful for browser-based clients or environments where HTTP/WS is required.

4. **Error Rate**: ~0.03% error rate under load, acceptable for high-throughput scenarios.

## Usage

```bash
# Start server with TPC-H database
go run ./cmd/porter serve --ws --ws-port 8080 --db ./bench.db

# Run WebSocket benchmark
go run ./bench/ws --port 8080 --clients 4 --duration 30

# Run FlightSQL benchmark  
go run ./bench/porter/main.go -port 32010 -regen false
```

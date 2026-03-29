# Transport Benchmark Report (WebSocket vs Flight SQL)

## What was changed

This report now uses a **single benchmark harness** (`bench/transport/main.go`) that runs identical SQL workloads against both transports and reports:

- throughput (rows/sec + payload MB/sec)
- latency (p50/p95/p99)
- first-byte latency
- batching behavior (batches/query)
- decode cost and read-time ratios
- optional CPU/heap profiles for pprof (`--cpu-profile`, `--heap-profile`)

## Benchmark integrity fixes

The previous comparison was not apples-to-apples:

1. **Different workloads** (WebSocket used `generate_series`, Flight used TPC-H lineitem scan).
2. **Connection lifecycle mismatch** (WebSocket benchmark established a new socket per query).
3. **Different benchmark clients and output metrics**.

The new harness removes those mismatches by using the same query text, concurrency, iteration count, and result accounting for both transports.

## Root-cause bottlenecks identified

### WebSocket bottlenecks

1. **Per-query connection setup overhead** in benchmark/client behavior.
2. **Many small writes from Arrow IPC writer into WebSocket writer**, increasing syscall and framing overhead.
3. **Connection forced closed after each query** in server path, preventing request multiplexing/reuse on one socket.

### Flight behavior baseline

Flight SQL/gRPC already streams efficiently with persistent connections and optimized backpressure in gRPC transport.

## Optimizations implemented

1. **Persistent WebSocket session support in server**
   - Server now handles multiple query requests on one connection.
2. **Buffered Arrow IPC write path for WebSocket**
   - Added `bufio.Writer` (1 MiB buffer).
   - Flushes periodically to coalesce writes (`8` batches) and reduce small-frame churn.
3. **Unified transport benchmark harness**
   - Added `bench/transport/main.go` with identical workload execution for ws/flight.
   - Added batch-size sensitivity mode (`--batch-sizes`).
   - Added pprof artifact support (`--cpu-profile`, `--heap-profile`).

## How to run

```bash
# Start Porter with both transports enabled
go run ./cmd/porter serve --ws --ws-port 8080 --port 32010 --db :memory:

# Throughput + latency, identical query, both transports
go run ./bench/transport/main.go --transport both --clients 2 --iterations 5

# Batch-size sensitivity sweep
go run ./bench/transport/main.go --transport both --clients 2 --iterations 3 --batch-sizes 100000,500000,1000000,2000000

# With pprof artifacts
go run ./bench/transport/main.go --transport ws --clients 2 --iterations 5 --cpu-profile ws_cpu.pprof --heap-profile ws_heap.pprof
```

## Remaining gaps to investigate (if needed)

- Eliminate text schema side-channel in ws protocol (Arrow stream already includes schema metadata).
- Add optional binary metadata/control frames for query-complete markers and errors.
- Reuse Flight client/connection object in benchmark hot loop to isolate server-side deltas even further.

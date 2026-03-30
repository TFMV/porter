# Apache Arrow Benchmark Report: WebSocket IPC vs FlightSQL (gRPC)

## 1. Overview

This report compares two Arrow data transport paths under identical analytical query workload:

* WebSocket transport using Arrow IPC framing
* FlightSQL transport over gRPC streaming

The goal is to evaluate throughput, latency distribution, and stability under concurrent execution.

---

## 2. Test Configuration

* Clients: 4 concurrent workers
* Iterations per client: 3
* Total operations: 12 per transport
* Timeout: 2 minutes per query
* Query workload: `generate_series`-based analytical scan producing wide row batches
* Execution mode: concurrent streaming queries per transport

---

## 3. Results

| Metric      | WebSocket    | FlightSQL (gRPC) |
| ----------- | ------------ | ---------------- |
| Ops         | 12           | 12               |
| Success     | 12           | 12               |
| Errors      | 0            | 0                |
| Rows/sec    | 130,712,427  | 121,704,008      |
| Throughput  | 1014.32 MB/s | 928.53 MB/s      |
| Latency p50 | 26 ms        | 17 ms            |
| Latency p95 | 41 ms        | 60 ms            |
| Latency p99 | 41 ms        | 60 ms            |

---

## 4. Key Observations

### 4.1 Throughput

WebSocket demonstrates higher sustained throughput in this run (~9% improvement). This suggests Arrow IPC framing over raw WebSocket is highly efficient for bulk transfer workloads, with minimal protocol overhead beyond TCP.

FlightSQL shows slightly lower throughput, consistent with additional overhead from gRPC streaming, HTTP/2 framing, and flow control scheduling.

---

### 4.2 Latency Distribution

Latency behavior diverges between the two transports:

* FlightSQL has lower median latency (p50 = 17 ms vs 26 ms)
* WebSocket shows significantly tighter tail latency (p95 = p99 = 41 ms)
* FlightSQL exhibits a wider tail distribution (p50 → p95 jump)

This indicates:

* FlightSQL is more responsive to initial query execution
* WebSocket provides more stable sustained streaming once pipeline is active

---

### 4.3 Stability Characteristics

WebSocket results show a compressed tail (p95 == p99), indicating consistent streaming delivery under concurrency.

FlightSQL shows tail expansion (p95 == p99 = 60 ms), suggesting variability in stream scheduling or batch emission timing under load.

---

### 4.4 Execution Parity

Both systems achieved identical correctness characteristics:

* 100% success rate
* Identical logical query execution
* No decoding or protocol errors observed

This confirms that differences are transport-level rather than query-level.

---

## 5. Interpretation

The results indicate that both transports operate in the same performance class for large analytical scans.

Key takeaways:

* WebSocket IPC is highly competitive with FlightSQL in sustained throughput
* FlightSQL provides better median latency responsiveness
* WebSocket provides more stable tail behavior under concurrency in this run

These differences are likely influenced more by runtime scheduling, batching behavior, and transport framing than fundamental protocol limits.

---

## 6. Conclusion

Arrow IPC over WebSocket demonstrates production-grade performance characteristics comparable to FlightSQL for bulk analytical workloads.

Rather than indicating a clear winner, the results highlight a trade-off:

* FlightSQL: better early responsiveness
* WebSocket IPC: higher throughput and tighter tail stability

Further scaling tests (higher concurrency, larger sample sizes, and controlled batch normalization) are required to establish statistically stable conclusions.

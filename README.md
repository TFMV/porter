```markdown
<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="150" height="150">
  
# Porter

# 🦆 FlightSQL Server for DuckDB

**An experimental Apache Arrow FlightSQL server** built on:

- 🏹 Apache Arrow Flight (`arrow-go/v18`)
- 🔌 ADBC (Arrow Database Connectivity)
- 🦆 DuckDB
- 🧵 gRPC streaming

Porter speaks **Flight natively**, processes data in **Arrow batches**, and delivers high-performance SQL execution over the wire.

---

## What is Porter?

Porter is a correct, streaming SQL execution engine implemented as a full Apache Arrow FlightSQL server.

It is **not** a full distributed database — yet. Think of it as:

> “DuckDB wearing a Flight helmet.”

---

## Sharding Reality Check

Porter includes configurable `shards` for testing and simulation.

**Important:** Sharding is **simulated** — there is no data partitioning.

### Current Behavior

Every shard:
- Runs the **same SQL**
- Hits the **same DuckDB instance**
- Returns the **same logical dataset**
- Differs only by metadata (`Shard`, `QueryID`)

### Why it exists

- Concurrency and load testing
- Fan-out simulation
- Stress testing Flight streams
- Safe experimentation with distributed-style patterns

---

## Core Concepts

All operations revolve around the `ExecTicket`:

```json
{
  "type": "sql" | "prepared",
  "sql": "...",
  "handle": "...",
  "bindings": ...
}
```

---

## Flight API

### Handshake
Standard Flight handshake with token validation.

### GetSchema
Returns the Arrow schema for a query (executes just enough to discover structure).

### GetFlightInfo
- Executes the query plan to obtain schema
- Generates one endpoint per shard (when configured)
- Returns redeemable ticket(s) for `DoGet`

### DoGet
The core streaming endpoint:

1. Decode ticket
2. Resolve SQL (raw or prepared)
3. Execute via ADBC + DuckDB
4. Stream Arrow RecordBatches

**Guarantees:**
- Batch-by-batch delivery
- Schema-preserving IPC writer
- Full context cancellation support

### DoExchange
Parallel execution simulator:
- Spawns one goroutine per shard
- All shards stream results into a single Flight pipe

**Note:** This is **not** true distributed execution or partitioning — it is a concurrency stress-test tool.

### DoAction
Control-plane operations:
- `CreatePreparedStatement`
- `ExecutePrepared`
- `ClosePreparedStatement`

---

## Prepared Statements

- Stored per-connection in memory
- Thread-safe (`sync.Mutex`)
- Ephemeral (lost on server restart)

---

## Connection Management

- Lazy creation via ADBC
- Configuration-based pooling (`memory` or file-based DuckDB)
- Thread-safe with `sync.RWMutex`

---

## Arrow Integration

- **Schemas** are serialized as standard Arrow IPC binary
- **Records** are streamed batch-by-batch using `flight.NewRecordWriter`

---

## Concurrency Model

- `DoGet`: disciplined single-stream execution
- `DoExchange`: goroutine-based parallelism
- Pools and statements are fully mutex-protected

---

## Known Limitations

- Sharding is simulated (no predicate pushdown or data locality)
- No built-in distributed query planner (DuckDB handles planning)
- No automatic result merging in `DoExchange`
- Prepared statements are in-memory only
- DuckDB defaults to in-memory mode (file mode is supported)

---

## Design Philosophy

Porter prioritizes:
- Correctness and streaming stability
- Observability and testability
- Simplicity over premature distribution

> “Arrow Flight made easy to reason about — and fast enough to be useful.”

---

## Future Roadmap

- Real predicate-pushdown sharding
- True distributed execution with coordinator
- Persistent catalog for prepared statements and query history
- Telemetry, metrics, and observability
- Advanced auth and TLS support

---

## License

Released under the MIT License. See [LICENSE](LICENSE) for details.
```

**Changes made:**
- Removed duplicate headings and cleaned up the top section
- More consistent, professional tone while keeping the fun DuckDB personality
- Better structure and flow with clear section hierarchy
- Concise language (removed rambling/jokey phrases)
- Fixed formatting and markdown consistency
- Improved readability for users and contributors

Just replace your existing README with this version. Let me know if you want an "Installation & Quick Start" section added or any other tweaks!
<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="140" height="140">
  <h1>Porter</h1>
  <p>A streaming-first Arrow Flight SQL server for DuckDB — simple, sharp, and built for motion.</p>
</div>

---

## 🧭 Overview

Porter is a DuckDB-backed Arrow Flight SQL server designed around one idea:

> SQL goes in. Arrow streams out. Everything else is detail.

It sits directly on top of Apache Arrow Flight SQL and exposes a clean execution surface for both raw SQL and prepared statements.

No orchestration layer. No distributed query engine. No abstraction sprawl.

Just a tight execution loop between Flight and DuckDB.

---

## ⚡ Key Characteristics

* Streaming-first execution model (Arrow RecordBatch streams)
* Native DuckDB execution via ADBC
* Full prepared statement lifecycle with parameter binding
* TTL-based handle management with background GC
* Minimal, explicit Flight SQL surface area

---

## 🧱 Architecture

Porter keeps the control flow linear:

```
           +-------------------+
           |   Flight Client   |
           +-------------------+
                     |
               gRPC / Flight
                     |
           +-------------------+
           |   Porter Server   |
           |-------------------|
           | Flight SQL Layer  |
           | Handle Manager    |
           | Prepared Stmts    |
           | Stream Engine     |
           +-------------------+
                     |
           +-------------------+
           |     DuckDB        |
           |   (via ADBC)      |
           +-------------------+
                     |
           +-------------------+
           | Arrow RecordBatches|
           +-------------------+
```

The server is intentionally thin: routing, lifecycle, and streaming glue only.
DuckDB does the heavy lifting.

---

## 🚀 Getting Started

### 0. Install DuckDB driver

Before anything else:

```bash
./install_duckdb.sh
```

This sets up the required DuckDB ADBC driver environment.

---

### 1. Run the Server

```bash
go run ./cmd/server
```

Defaults:

* Address: `localhost:32010`
* Database: in-memory DuckDB (`:memory:`)

---

### 2. Run a Client

You have two ways to exercise the system:

#### Native client

```bash
go run ./cmd/client
```

#### Example harness

```bash
go run ./example
```

Both will issue queries and stream Arrow record batches back from Flight.

---

## 🧠 Execution Model

Porter supports two execution paths:

### 1. One-shot SQL

* `GetFlightInfoStatement` → plan + handle
* `DoGetStatement` → stream results

Ephemeral handles, auto-expire under TTL.

---

### 2. Prepared Statements

* `CreatePreparedStatement` → persistent handle
* `DoPutPreparedStatementQuery` → bind parameters
* `DoGetPreparedStatement` → execute + stream
* `ClosePreparedStatement` → cleanup

Parameter batches are real Arrow RecordBatches, reference-counted and safely transferred across execution boundaries.

---

## 🧬 Design Rules

Porter is built on strict invariants:

* Flight SQL owns protocol routing (via `fsql.NewFlightServer`)
* Porter only implements execution semantics
* Handles are in-memory and TTL-bound
* GC runs in the background (no inline eviction logic)
* Arrow memory is explicitly retained/released

Nothing implicit. Nothing magical.

---

## 🌊 Streaming Core

All query results flow through a single pattern:

```
DuckDB → Arrow RecordReader → Channel → Flight StreamChunks
```

Records are retained per batch and released after network write completion.
This keeps backpressure and memory usage predictable.

---

## 🌐 Wire Contract

Porter supports both raw and Flight SQL-native flows:

| Operation            | Behavior                            |
| -------------------- | ----------------------------------- |
| SQL Query            | Raw SQL → FlightInfo → DoGet stream |
| Prepared Statements  | Handle-based execution with binding |
| Schema Introspection | Lightweight probe execution         |

Both converge on the same execution engine.

---

## 🔌 WebSockets (Coming Soon)

A WebSocket transport layer is in progress.

Planned capabilities:

* Bi-directional streaming query sessions
* Low-latency Arrow batch push over WS frames
* Browser-native Flight-like client
* Session-based prepared statement lifecycle

Think of it as Flight SQL without the gRPC boundary.

---

## 🛣️ Roadmap

* [x] Streaming Flight SQL execution
* [x] Prepared statements with parameter binding
* [x] TTL-based handle lifecycle
* [x] Background garbage collection
* [ ] WebSocket transport layer
* [ ] Session-aware execution context
* [ ] Improved schema introspection (reduce probe execution)
* [ ] Performance benchmarking suite

---

## 🧪 Philosophy

Porter is intentionally narrow:

> No distributed illusions. No unnecessary abstraction layers. Just a fast path from query to stream.

It is a system designed for hacking, embedding, and evolving.

---

## 🤝 Contributing

If you’ve ever looked at a data system and thought:

> “Why is this so complicated?”

you already understand what Porter is trying to fix.

Build it smaller. Make it clearer. Keep it moving.

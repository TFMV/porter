<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="140" height="140">
  <h1>Porter</h1>
  <p>A streaming-first Arrow server for DuckDB — Flight SQL and WebSocket, simple and built for motion.</p>
</div>

---

## 🧭 Overview

Porter is a DuckDB-backed Arrow server with two transport protocols:

- **Flight SQL** — gRPC-based Arrow Flight SQL
- **WebSocket** — HTTP-based Arrow streaming

> SQL goes in. Arrow streams out. Everything else is detail.

Both transports share the same execution engine, ensuring identical query semantics.

---

## Summary Benchmark Results

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

See the [Benchmark Report](bench/bench_results.md) for details.

---

## ⚡ Key Characteristics

* Streaming-first execution model (Arrow RecordBatch streams)
* Dual transport support: Flight SQL + WebSocket
* Shared execution engine for semantic parity
* Native DuckDB execution via ADBC
* Full prepared statement lifecycle with parameter binding
* TTL-based handle management with background GC

---

## 🏗️ Architecture

```
           +-------------------+
           |   Flight Client   |  <-- ADBC / Flight SQL
           +-------------------+
                     |
               gRPC / Flight
                     |
           +-------------------+
           |   Porter Server   |
           |-------------------|
           | Shared Engine     |  <-- BuildStream()
           +-------------------+
                     |
           +-------------------+
           |     DuckDB        |
           |   (via ADBC)     |
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

You have three ways to run Porter:

* Docker (fastest path)
* `go install` (clean local toolchain)
* Build from source (full control)

---

### 🐳 Option 1 — Run with Docker

```bash
docker build -t porter .
docker run -p 32010:32010 -p 8080:8080 porter --ws
```

Run with a persistent database:

```bash
docker run -p 32010:32010 -p 8080:8080 -v $(pwd)/data:/data porter --db /data/porter.duckdb --ws
```

Defaults:

* Flight SQL: `0.0.0.0:32010`
* WebSocket: `0.0.0.0:8080` (when `--ws` enabled)
* Database: in-memory (`:memory:`)

---

## Prerequisites

Install dbc and required ADBC drivers:

```bash
curl -LsSf https://dbc.columnar.tech/install.sh | sh
dbc install duckdb
dbc install flightsql
```

---

### ⚙️ Option 2 — Install via `go install`

#### 1. Install Porter

```bash
go install github.com/TFMV/porter/cmd/porter@latest
```

This installs `porter` into your `$GOBIN`.

---

### 🛠 Option 3 — Build from Source

#### 1. Clone

```bash
git clone https://github.com/TFMV/porter.git
cd porter
```

#### 2. Run

```bash
go run ./cmd/porter serve
```

---

## 💻 CLI Usage

```bash
porter --help
```

### Quick Start

```bash
porter              # Start Flight SQL server on :32010
porter serve        # Same as above
```

### With WebSocket

```bash
porter --ws                        # Flight SQL + WebSocket
porter serve --ws                   # Same as above
porter serve --ws --ws-port 9090   # Custom WebSocket port
```

### Full Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--db` | DuckDB file path | `:memory:` |
| `--port` | Flight SQL port | `32010` |
| `--ws` | Enable WebSocket | `false` |
| `--ws-port` | WebSocket port | `8080` |

### Execute a query

```bash
porter query "SELECT 1 AS value"
```

### REPL

```bash
porter repl
```

### Load Parquet

```bash
porter load data.parquet
```

### Inspect schema

```bash
porter schema table_name
```

### Environment variables

* `PORTER_DB`
* `PORTER_PORT`
* `PORTER_WS`
* `PORTER_WS_PORT`

---

## 🌐 Wire Contract

### Flight SQL

| Operation            | Behavior                            |
| -------------------- | ----------------------------------- |
| SQL Query            | Raw SQL → FlightInfo → DoGet stream |
| Prepared Statements  | Handle-based execution with binding |
| Schema Introspection | Lightweight probe execution         |
| ExecuteUpdate        | DDL/DML via DoPutCommandStatementUpdate |

### WebSocket

Send JSON query request:

```json
{"query": "SELECT * FROM table"}
```

Receive:

1. Schema message: `{"type": "schema", "fields": ["col1", "col2"]}`
2. Binary IPC frames containing Arrow RecordBatches

---

## 🌊 Streaming Core

Both transports use the same execution primitive:

```go
BuildStream(ctx, sql, params) (*arrow.Schema, <-chan StreamChunk, error)
```

```
DuckDB → Arrow RecordReader → Channel → StreamChunk
```

Backpressure is enforced naturally via the channel boundary.

---

## 🛣️ Roadmap

- [x] Streaming Flight SQL execution
- [x] WebSocket transport
- [x] Shared execution engine
- [x] Prepared statements
- [x] TTL-based lifecycle
- [x] Background GC
- [ ] Session context
- [ ] Improved schema probing
- [ ] Benchmark suite

---

## 🤝 Contributing

If you've ever looked at a data system and thought:

> "Why is this so complicated?"

You're in the right place.

Build it smaller. Make it clearer. Keep it moving.

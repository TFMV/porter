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
* **Bulk Ingest** — Arrow RecordBatch → DuckDB with transactional semantics
* Shared execution engine for semantic parity
* Native DuckDB execution via ADBC
* Full prepared statement lifecycle with parameter binding
* TTL-based handle management with background GC
* Live status surface with pipeline flow, pressure, and backpressure visibility

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
* Status: `0.0.0.0:9091` (enabled by default)
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
porter serve --status-port 9191    # Custom status surface
porter serve --ducklake --ducklake-catalog-type duckdb --ducklake-catalog-dsn ./metadata.ducklake
porter serve --ducklake --ducklake-catalog-type sqlite --ducklake-catalog-dsn ./catalog.sqlite --ducklake-data-path ./ducklake-data
```

### Full Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--db` | DuckDB file path | `:memory:` |
| `--port` | Flight SQL port | `32010` |
| `--ws` | Enable WebSocket | `false` |
| `--ws-port` | WebSocket port | `8080` |
| `--status` | Enable live status surface | `true` |
| `--status-port` | Status server port | `9091` |
| `--ducklake` | Enable DuckLake during server startup | `false` |
| `--ducklake-catalog-type` | DuckLake metadata backend: `duckdb`, `sqlite`, `postgres`, `mysql` | `duckdb` |
| `--ducklake-catalog-dsn` | DuckLake metadata DSN or file path | `metadata.ducklake` |
| `--ducklake-data-path` | DuckLake Parquet/object storage path | empty |
| `--ducklake-name` | Attached DuckLake catalog name | `my_ducklake` |

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
* `PORTER_STATUS`
* `PORTER_STATUS_PORT`
* `PORTER_DUCKLAKE`
* `PORTER_DUCKLAKE_CATALOG_TYPE`
* `PORTER_DUCKLAKE_CATALOG_DSN`
* `PORTER_DUCKLAKE_DATA_PATH`
* `PORTER_DUCKLAKE_NAME`

### DuckLake Startup

When `--ducklake` is enabled, Porter initializes DuckLake during server startup and keeps the existing FlightSQL/Arrow execution path unchanged. DuckLake is treated as database configuration, not as a separate query mode.

Supported catalog backends:

* `duckdb`
* `sqlite`
* `postgres`
* `mysql`

Examples:

```bash
porter serve --ducklake \
  --ducklake-catalog-type duckdb \
  --ducklake-catalog-dsn ./metadata.ducklake

porter serve --ducklake \
  --ducklake-catalog-type sqlite \
  --ducklake-catalog-dsn ./catalog.sqlite \
  --ducklake-data-path ./ducklake-data

porter serve --ducklake \
  --ducklake-catalog-type postgres \
  --ducklake-catalog-dsn postgres://user:pass@host/db \
  --ducklake-data-path s3://bucket/prefix \
  --ducklake-name my_ducklake
```

Startup initialization:

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:<catalog>' AS my_ducklake (DATA_PATH '...');
USE my_ducklake;
```

Per-connection initialization:

```sql
LOAD ducklake;
LOAD <catalog-extension>;
USE my_ducklake;
```

DuckLake inspection and maintenance functions are available through the existing SQL path, for example:

```sql
FROM ducklake_snapshots('my_ducklake');
SELECT * FROM ducklake_table_info('my_ducklake');
SELECT * FROM my_table AT (VERSION => 2);
CALL ducklake_merge_adjacent_files('my_ducklake');
CALL ducklake_expire_snapshots('my_ducklake', dry_run => true);
CALL ducklake_cleanup_old_files('my_ducklake', dry_run => true, cleanup_all => true);
```

---

## Live Status Surface

Porter now exposes a dedicated status server with a living cross-section of the pipeline:

* `/status` — live instrument panel UI
* `/status/live` — current JSON snapshot
* `/status/stream` — SSE stream of snapshots
* `/status/history` — rolling snapshot history
* `/status/health` — deterministic health status

The flow view tracks:

* `ingress -> transport -> execution -> egress`
* rows/sec and MB/sec per stage
* queue depth and pressure buildup
* p50/p95/p99 latency divergence
* live structured activity feed
* WebSocket vs FlightSQL vs ingest path comparison

![Porter Status](assets/porter-status.png)

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

## 📥 Bulk Ingest

Porter supports high-throughput Arrow RecordBatch ingestion via Flight SQL's DoPut:

```go
// Engine interface
IngestStream(ctx, table, reader, opts) (int64, error)
```

**Features:**

| Feature | Description |
|---------|-------------|
| Transactional | One stream = one DB transaction |
| Schema validation | Incoming Arrow schema must match target table |
| Backpressure | Configurable `MaxUncommittedBytes` (default 64MB) |
| Table locking | Per-table mutex prevents concurrent writes to same table |
| Auto-commit | Automatically commits on successful ingest, rolls back on failure |

**IngestOptions:**

| Option | Description |
|--------|-------------|
| `Catalog` | Target catalog name |
| `DBSchema` | Target schema name |
| `Temporary` | Create as temporary table |
| `IngestMode` | Append, replace, or create |
| `MaxUncommittedBytes` | Memory limit before fail-fast (default 64MB) |

**Flow:**

```
Client → DoPut (Arrow RecordBatch stream) → Engine.IngestStream → SegmentWriter → Commit → DuckDB
```

The SegmentWriter accumulates RecordBatches in memory, then atomically publishes them on commit. If `MaxUncommittedBytes` is exceeded, ingestion fails fast with rollback.

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
- [x] Bulk Ingest (DoPut)
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

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

You have three ways to run Porter depending on how you like to work:

* Docker (fastest path)
* `go install` (clean local toolchain)
* Build from source (full control)

---

### 🐳 Option 1 — Run with Docker (fastest)

```bash
docker build -t porter .
docker run -p 32010:32010 porter
```

Run with a persistent database:

```bash
docker run -p 32010:32010 -v $(pwd)/data:/data porter --db /data/porter.duckdb
```

Defaults:

* Address: `0.0.0.0:32010`
* Database: in-memory (`:memory:`)

---

### ⚙️ Option 2 — Install via `go install`

#### 1. Install Porter

```bash
go install github.com/TFMV/porter/cmd/porter@latest
```

This installs `porter` into your `$GOBIN`.

#### 2. Install ADBC CLI (dbc)

```bash
curl -LsSf https://dbc.columnar.tech/install.sh | sh
```

#### 3. Install DuckDB ADBC driver

```bash
dbc install duckdb
```

Verify installation:

```bash
dbc list
```

You should see `duckdb` listed.

---

### 🛠 Option 3 — Build from Source

#### 1. Clone

```bash
git clone https://github.com/TFMV/porter.git
cd porter
```

#### 2. Install DuckDB ADBC driver

```bash
./install_duckdb.sh
```

#### 3. Run

```bash
go run ./cmd/porter serve
```

---

## 💻 CLI Usage

Porter exposes a composable CLI:

```bash
porter --help
```

### Run the server

```bash
porter serve --db :memory: --port 32010
```

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

---

## 🧠 Execution Model

Porter supports two execution paths:

### 1. One-shot SQL

* `GetFlightInfoStatement` → plan + handle
* `DoGetStatement` → stream results

### 2. Prepared Statements

* `CreatePreparedStatement`
* `DoPutPreparedStatementQuery`
* `DoGetPreparedStatement`
* `ClosePreparedStatement`

Parameter batches are real Arrow RecordBatches with explicit ownership.

---

## 🌊 Streaming Core

```
DuckDB → Arrow RecordReader → Channel → Flight StreamChunks
```

Backpressure is enforced naturally via the channel boundary.

---

## 🌐 Wire Contract

| Operation            | Behavior                            |
| -------------------- | ----------------------------------- |
| SQL Query            | Raw SQL → FlightInfo → DoGet stream |
| Prepared Statements  | Handle-based execution with binding |
| Schema Introspection | Lightweight probe execution         |

---

## 🛣️ Roadmap

* [x] Streaming Flight SQL execution
* [x] Prepared statements
* [x] TTL-based lifecycle
* [x] Background GC
* [ ] WebSocket transport
* [ ] Session context
* [ ] Improved schema probing
* [ ] Benchmark suite

---

## 🤝 Contributing

If you’ve ever looked at a data system and thought:

> “Why is this so complicated?”

You’re in the right place.

Build it smaller. Make it clearer. Keep it moving.

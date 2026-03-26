<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="150" height="150">
  
# 🦆 FlightSQL Server (Arrow + ADBC + DuckDB vibes)

An experimental, slightly opinionated, mostly serious, occasionally chaotic **Apache Arrow FlightSQL server** built on:

* 🏹 Apache Arrow Flight (`arrow-go/v18`)
* 🔌 ADBC (Arrow Database Connectivity)
* 🦆 DuckDB (in-memory, because disks are for people who trust persistence)
* 🧵 gRPC streaming everywhere

This server speaks **Flight natively**, thinks in **Arrow batches**, and behaves like a system that respects data more than architecture fashion.

---

# 🧠 What this actually is

Let’s be honest:

This is a **correct streaming SQL execution engine scaffold**.

Not a distributed database. Not a query engine platform.

More like:

> “DuckDB wearing a Flight helmet and taking itself very seriously in a streaming context”

---

# 🚪 Entry Points (Flight API)

## 1. Handshake

A polite but firm handshake.

* streaming auth
* token echo validation
* immediate rejection of empty payloads

Think:

> “Hello. Yes. You may pass. But behave.”

---

## 2. GetSchema

Runs the query just enough to determine shape.

Returns:

* Arrow IPC-encoded schema
* no opinions
* no embellishment

It answers one question only:

> “What am I about to stream?”

---

## 3. GetFlightInfo

The planning stage.

* executes query once
* extracts schema
* builds flight endpoints for execution

Each endpoint contains:

* SQL (resolved or prepared)
* execution metadata (`QueryID`, `execution context`)
* a ticket for retrieval

No fan-out illusion. No distributed theater.

Just a clean execution contract.

---

## 4. DoGet 🧨 (The main event)

This is the actual data pipeline.

Flow:

1. decode ticket
2. resolve SQL (raw or prepared)
3. open DuckDB connection via ADBC
4. execute query
5. stream Arrow RecordBatches

### Streaming guarantees

* batch-by-batch delivery
* schema-preserving writer
* context-aware cancellation
* no silent stream corruption (we try)

### Philosophy

> “If data exists, it will be streamed. If it doesn’t, we will still behave professionally.”

---

## 5. DoExchange 🌪️

This is the experimental streaming channel.

It exists for systems that want bidirectional data flow.

Behavior:

* receives Arrow payloads
* executes query contextually
* streams results back over the same channel

### Important truth

This is NOT:

* ❌ partitioned execution
* ❌ distributed compute
* ❌ parallel query fan-out

This IS:

* ✔ streaming stress test
* ✔ backpressure exploration
* ✔ “what happens if everything is async” simulator

---

## 6. DoAction 🛠️

Control plane for mildly dangerous behavior.

### Supported actions:

#### CreatePreparedStatement

* hashes SQL
* stores it in memory
* returns a prepared ID

Ephemeral by design.

#### ClosePreparedStatement

* deletes stored SQL
* restores system calm

---

# 🦆 Prepared Statements

Stored in:

```go
sync.Map
```

Which is Go’s way of saying:

> “Concurrent, unstructured, and emotionally confident.”

---

# 🧵 Connection Model

A deliberately simple ADBC model:

* in-memory DuckDB connections
* lazy creation
* reused when available
* no lifecycle ceremony

If something goes wrong:

> it was probably the query, not the pool

---

# 📦 Arrow Integration

## Schema

Serialized via:

```go
ipc.NewWriter(...WithSchema)
```

Meaning:

* schema becomes portable binary truth
* clients stay type-safe
* nobody argues about structure

---

## Records

Each batch:

* streamed as Arrow IPC
* schema-bound
* emitted incrementally

No JSON. No reinterpretation layers.

---

# ⚙️ Concurrency Model

* DoGet: disciplined single-stream execution
* DoExchange: experimental bidirectional streaming
* connection pool: mutex-protected optimism
* no distributed execution layer

---

# 💥 Known Limitations (a.k.a. “honest boundaries”)

### 🧠 No query planner control

DuckDB handles optimization internally.

### 🔁 No result merging layer

Streaming is linear and direct.

### 🧊 Prepared statements are in-memory only

Restart = reset.

### 🦆 DuckDB is ephemeral

Because persistence adds narrative complexity.

---

# 🧭 Design Philosophy

This system is built on a simple belief:

> “Streaming correctness beats architectural cosplay.”

It prioritizes:

* predictable execution
* clean Arrow semantics
* observable streaming behavior
* minimal hidden state

---

# 🚀 Future Ideas (a.k.a. “things that could get serious”)

If this evolves, it might become:

### Query routing layer

* predicate-aware execution paths
* selective dataset targeting

### Persistent catalog

* prepared statement registry
* query metadata tracking

### Distributed execution (real this time)

* actual partitioned datasets
* merge operators over Arrow streams
* coordination layer with intent

---

# 🦆 Closing Thought

This system is not pretending to be a distributed database.

It is something more honest:

> a streaming SQL engine that knows exactly what it is, and refuses to hallucinate architecture it doesn’t actually implement

And that restraint?

That’s where correctness starts.

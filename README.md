<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="150" height="150">
  
# 🦆 FlightSQL Server (Arrow + ADBC + DuckDB vibes)

Am expermintal, slightly opinionated, mostly serious, occasionally chaotic **Apache Arrow FlightSQL server** built on:

* 🏹 Apache Arrow Flight (`arrow-go/v18`)
* 🔌 ADBC (Arrow Database Connectivity)
* 🦆 DuckDB (in-memory, because disks are for people who trust persistence)
* 🧵 gRPC streaming everywhere

This server speaks **Flight natively**, thinks in **Arrow batches**, and occasionally pretends it is a distributed system.

---

# 🧠 What this actually is

Let’s be honest:

This is a **correct streaming SQL execution engine scaffold**.

Not a full distributed database. Not yet.
More like:

> “DuckDB wearing a Flight helmet and running through a fog machine labeled *sharding*”

---

# ⚠️ Sharding Reality Check (Read before you scale it to the moon)

Yes, there is a `shards` setting.

No, it does not partition data.

### 🧨 Current behavior:

Every shard:

* runs the **same SQL**
* hits the **same DuckDB instance type**
* returns the **same logical dataset**
* only differs by metadata (`Shard`, `QueryID`)

### Why this exists

Because real distributed execution is hard, and this version:

* used to attempt real sharding
* broke in interesting and painful ways
* was rolled back to something stable and observable

### So what is it good for?

* concurrency testing
* fan-out simulation
* load testing Flight streams
* pretending you have a distributed system at 2am

---

# 🧾 Core Idea

Everything revolves around a single struct:

```json
ExecTicket
```

It tells the server:

* what to run (`sql` or `prepared`)
* which shard you are pretending to be
* how many siblings exist in the hallucinated cluster

---

# 🚪 Entry Points (Flight API)

## 1. Handshake

A polite but firm handshake.

* streaming auth
* echo-style token validation
* immediately rejects empty souls (and payloads)

Think:

> “Hello. Yes. You may pass. But behave.”

---

## 2. GetSchema

Runs the query just enough to figure out:

> “what kind of data are we even talking about?”

Returns:

* Arrow IPC-encoded schema
* no drama
* no opinion

---

## 3. GetFlightInfo

This is where the illusion of scale begins.

* executes query once
* extracts schema
* generates `N` endpoints

Each endpoint contains:

* same SQL
* same result universe
* different shard label (for vibes)

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

This is where things get *artistically parallel*.

For each request:

* spawn `shards` goroutines
* each runs the **same query**
* all streams back into one Flight pipe

### Important truth

This is NOT:

* ❌ partitioning
* ❌ distributed execution
* ❌ a real shuffle engine

This IS:

* ✔ concurrency stress test
* ✔ connection pool chaos test
* ✔ “what if everything ran at once” simulator

---

## 6. DoAction 🛠️

Control plane for mildly dangerous behavior.

### Supported actions:

#### CreatePreparedStatement

* hashes SQL
* stores it in memory
* returns a prepared ID

It is fast. It is ephemeral. It is gone on restart.

#### ClosePreparedStatement

* deletes stored SQL
* restores emotional balance

---

# 🦆 Prepared Statements

Stored in:

```go
sync.Map
```

Which is Go’s way of saying:

> “We promise it’s concurrent. Don’t ask further questions.”

---

# 🧵 Connection Pool

A very honest pool:

* in-memory DuckDB
* lazy connection creation
* LIFO reuse
* no eviction policy
* no lifecycle drama

If it breaks:

> it was probably the query’s fault

---

# 📦 Arrow Integration

## Schema

Serialized via:

```go
ipc.NewWriter(...WithSchema)
```

Meaning:

* schema becomes portable binary truth
* clients stay happy
* nobody argues about types

---

## Records

Each row group:

* streamed as Arrow IPC
* schema-bound
* written one batch at a time

No JSON. No betrayal.

---

# ⚙️ Concurrency Model

* DoGet: single-stream disciplined execution
* DoExchange: goroutine chaos engine
* pool: mutex-protected optimism
* no distributed coordinator (yet)

---

# 💥 Known Limitations (a.k.a. “features in disguise”)

### 🧩 Sharding is fake

Every shard runs identical SQL.

### 🧠 No query planner

DuckDB is doing the thinking. We are just passing it notes.

### 🔁 No result merge

DoExchange just streams everything into one pipe like a confused firehose.

### 🧊 Prepared statements are in-memory only

Restart = amnesia.

### 🦆 DuckDB is in-memory only

Because persistence is a commitment.

---

# 🧭 Design Philosophy

This project sits in a very specific mental state:

> “What if Arrow Flight was easy to reason about, but still fast enough to scare people?”

It prioritizes:

* correctness over cleverness
* streaming stability over distributed fantasies
* observable behavior over theoretical scale

---

# 🚀 Future Ideas (a.k.a. “things that might hurt”)

If this grows up, it could become:

### Real sharding

* predicate pushdown
* partition-aware routing
* actual data locality

### Real distributed execution

* coordinator node
* per-shard result isolation
* merge operators

### Persistent catalog

* prepared statement registry
* query history
* maybe even telemetry (brace yourself)

---

# 🦆 Closing Thought

This system is not pretending to be perfect.

It is pretending to be:

> *a DuckDB that briefly believed it was a distributed system*

And honestly?

It’s doing a pretty good job of it.

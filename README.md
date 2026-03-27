<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="140" height="140">
  <h1>Porter</h1>
  <p>A streaming-first Arrow Flight SQL server for DuckDB — built for clarity, not ceremony.</p>
</div>

---

## 🧠 What It Is

Porter does one thing well:

> **Execute SQL against DuckDB and stream Arrow record batches over Flight.**

No orchestration layer. No distributed query planner. No pretending to be something bigger.

Just a clean, fast path from **SQL → Arrow**.

---

## ⚠️ What It Is *Not*

Porter is **not trying to be the entire Flight SQL universe**.

It focuses on the happy path and a practical compatibility layer:

* Supports native Porter payloads (raw SQL + JSON tickets)
* Supports core Flight SQL protobuf commands used by ADBC
* Skips kitchen-sink features and keeps behavior explicit

If you need every optional Flight SQL extension under the sun, use GizmoSQL.

If you want something fast, understandable, and hackable — use Porter.

---

## 🏗️ Architecture

A straight line. No detours.

```
    [ Client ]
        |
 (gRPC / Flight)
        |
+-------------------------------+
| Porter Server                 |
+-------------------------------+
| Flight RPCs                  |
| - GetFlightInfo (SQL → plan) |
| - DoGet (plan → data)        |
| - DoExchange (SQL → data)    |
|                              |
| Prepared Statements          |
| - in-memory                  |
|                              |
| DuckDB Engine (embedded)     |
+-------------------------------+
```

---

## ✈️ Wire Contract (Important)

Porter now speaks **two input paths** for query and prepared-statement flow:

| RPC             | Native Porter Path                                   | Flight SQL / ADBC Path                                                      |
| --------------- | ---------------------------------------------------- | --------------------------------------------------------------------------- |
| `GetFlightInfo` | `FlightDescriptor.Cmd = raw SQL bytes`               | `FlightDescriptor.Cmd = Any(CommandStatementQuery)`                         |
| `GetSchema`     | `FlightDescriptor.Cmd = raw SQL bytes`               | `FlightDescriptor.Cmd = Any(CommandStatementQuery)`                         |
| `DoGet`         | `Ticket = {"plan_id":"..."}` (JSON bytes)            | Prepared statement handle resolves to the same ticket bytes                 |
| `DoExchange`    | First message `FlightDescriptor.Cmd = raw SQL bytes` | (same behavior)                                                             |
| `DoAction`      | JSON bodies for create/close prepared statements      | `Any(ActionCreatePreparedStatementRequest/Result)` + `Any(ActionClose...)` |

In short: one engine, two ways in.

---

## 🚀 Quick Start

### 1. Start the Server

```bash
go run ./cmd/server
```

Default:

```
localhost:32010
```

---

### 2. Run the Native Client

```bash
go run ./cmd/client
```

You should see streamed Arrow batches logged to stdout.

---

### 3. (Optional) Try ADBC

ADBC now works for core query + prepared-statement flows.

```bash
go run ./example
```

If your server binary is older, you may still see protobuf-ish parser errors.
Restart with the latest build and those should disappear.

---

## 💡 Why This Design

Because the “standard” stack often looks like this:

```
SQL → protobuf → Flight SQL → driver manager → ADBC → DuckDB
```

Porter keeps the fast lane:

```
SQL (raw or protobuf command) → Flight → DuckDB → Arrow
```

Fewer mysteries. Fewer surprises. Faster iteration.

---

## 🧪 Philosophy

> SQL in. Arrow out. No drama.

---

## 🛣️ Roadmap (Honest Version)

* [ ] Parameter binding via `DoPut`
* [ ] Better session management
* [ ] Expand Flight SQL coverage beyond core command + prepare/close flows
* [ ] Benchmarks

---

## 🤝 Contributing

If you’ve ever fought a “standards-compliant” data stack and thought
“this should be simpler” — you’re in the right place.

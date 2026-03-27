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

Porter is **not a generic ADBC / Flight SQL endpoint**.

It intentionally avoids parts of the spec that introduce unnecessary indirection:

* No protobuf-wrapped SQL commands
* No driver-manager abstraction layer
* No “universal” compatibility guarantees

Instead, Porter speaks a **tight, explicit Flight contract**:

* SQL is sent as raw bytes
* Tickets are server-issued and opaque
* Streams are Arrow IPC, end to end

If you want full cross-driver compatibility, use a reference Flight SQL server.

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

Porter uses a **simplified Flight SQL protocol**:

| RPC             | Payload                                |
| --------------- | -------------------------------------- |
| `GetFlightInfo` | `FlightDescriptor.Cmd = raw SQL bytes` |
| `GetSchema`     | `FlightDescriptor.Cmd = raw SQL bytes` |
| `DoGet`         | `Ticket = {"plan_id": "..."}` (JSON)   |
| `DoExchange`    | First message contains raw SQL         |
| `DoAction`      | JSON for prepared statement lifecycle  |

No protobuf `CommandStatementQuery`.
No driver translation layer.
No surprises.

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

ADBC *can* work — but only if the server speaks full Flight SQL.

Porter does not.

If you try anyway, you’ll hit errors like:

```
Parser Error: syntax error at or near "Ctype.googleapis.com/..."
```

That’s the driver sending protobuf-encoded commands your server intentionally ignores.

---

## 💡 Why This Design

Because the “standard” stack looks like this:

```
SQL → protobuf → Flight SQL → driver manager → ADBC → DuckDB
```

Porter collapses it to:

```
SQL → Flight → DuckDB → Arrow
```

Fewer layers. Fewer surprises. Faster iteration.

---

## 🧪 Philosophy

> SQL in. Arrow out. No drama.

---

## 🛣️ Roadmap (Honest Version)

* [ ] Parameter binding via `DoPut`
* [ ] Better session management
* [ ] Optional strict Flight SQL compatibility layer
* [ ] Benchmarks

---

## 🤝 Contributing

If you’ve ever fought a “standards-compliant” data stack and thought
“this should be simpler” — you’re in the right place.

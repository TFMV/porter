<div align="center">
  <img src="assets/porter-logo.svg" alt="Porter Logo" width="140" height="140">
  <h1>Porter</h1>
  <p>A minimal, streaming-first Apache Arrow Flight SQL server built with Go, ADBC, and DuckDB.</p>
</div>

---

## 🧠 What It Is

Porter is a lightweight Flight SQL server designed for one job: **executing SQL against DuckDB and streaming the results as Arrow batches.**

It's not a distributed database or a complex query platform. It's a clean, correct, and streaming-native SQL execution layer.

## 🏗️ Architecture

The server follows a simple, linear flow: client requests come through gRPC, are handled by the Flight SQL layer, and executed against an in-memory DuckDB instance via an ADBC driver.

```
        [ Client ]
            |
     (gRPC / FlightSQL)
            |
+-------------------------------+
|     Porter Flight Server      |
+-------------------------------+
|  Flight Endpoints             |
|  - GetFlightInfo              |
|  - DoGet                      |
|  - DoPut / DoExchange         |
|                               |
|  Prepared Statement Cache     |
|  - in-memory (sync.Map)       |
|                               |
|  ADBC Connection Layer        |
|  - DuckDB connection reuse    |
+-------------------------------+
            |
         (ADBC)
            |
+-------------------------------+
|        DuckDB Engine          |
|      (in-memory SQL)          |
+-------------------------------+
```

### Core Components

*   **Connection Pool (`connPool`)**: Manages a lightweight pool of reusable DuckDB connections via the ADBC driver.
*   **Prepared Statement Cache (`sync.Map`)**: Stores prepared statement queries in a concurrent-safe map. These are ephemeral and reset on server restart.

## ✈️ Flight SQL Endpoints

Porter implements the core Flight SQL specification:

*   `Handshake`: A simple auth mechanism to start a session.
*   `GetSchema`: Returns the Arrow `Schema` for a SQL query without executing it.
*   `GetFlightInfo`: "Plans" a query by creating a ticket that can be used with `DoGet`.
*   `DoGet`: Executes a ticket's query and streams the `RecordBatch` results.
*   `DoAction`: Handles control-plane tasks like creating and closing prepared statements.
*   `DoPut`: Executes a prepared statement, receiving bound parameters as an Arrow `RecordBatch` stream.

## 🚀 Quick Start

1.  **Start the Server**:

    ```bash
    go run ./cmd/server
    ```

2.  **Run the Client**:

    In a separate terminal, execute a query through the client.

    ```bash
    go run ./cmd/client
    ```

## 💡 Philosophy

> SQL in. Arrow out. No drama.

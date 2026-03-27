#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
ROOT=$(pwd)
BUILD="$ROOT/porter-cli"
TMPDIR=$(mktemp -d)
SERVER_PID=0
PYTHON=python3
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  PYTHON=python
fi

cleanup() {
  if [[ $SERVER_PID -ne 0 ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "[1/7] Building porter CLI"
go build -o "$BUILD" .

DB1="$TMPDIR/source.duckdb"
DB2="$TMPDIR/loaded.duckdb"
PARQUET="$TMPDIR/data.parquet"
PORT=32100

printf "CREATE TABLE source(id INTEGER, name VARCHAR);\nINSERT INTO source VALUES (1, 'alpha'), (2, 'beta');\nCOPY source TO '$PARQUET' (FORMAT PARQUET);\n.exit\n" | "$BUILD" repl --db "$DB1"

if [[ ! -f "$PARQUET" ]]; then
  echo "ERROR: Parquet file was not created" >&2
  exit 1
fi

echo "[2/7] Testing load command"
"$BUILD" load "$PARQUET" --db "$DB2"

echo "[3/7] Testing schema command"
SCHEMA_OUTPUT=$("$BUILD" schema data --db "$DB2")
if ! grep -q "id" <<< "$SCHEMA_OUTPUT" || ! grep -q "name" <<< "$SCHEMA_OUTPUT"; then
  echo "ERROR: schema output missing expected columns" >&2
  echo "$SCHEMA_OUTPUT" >&2
  exit 1
fi

echo "[4/7] Testing query command"
QUERY_OUTPUT=$("$BUILD" query "SELECT id, name FROM data ORDER BY id" --db "$DB2")
if ! grep -q "1\talpha" <<< "$QUERY_OUTPUT" || ! grep -q "2\tbeta" <<< "$QUERY_OUTPUT"; then
  echo "ERROR: query output did not include expected rows" >&2
  echo "$QUERY_OUTPUT" >&2
  exit 1
fi

echo "[5/7] Testing repl command"
REPL_OUTPUT=$(printf "SELECT COUNT(*) AS count FROM data;\n.exit\n" | "$BUILD" repl --db "$DB2")
if ! grep -q "count" <<< "$REPL_OUTPUT" || ! grep -q "2" <<< "$REPL_OUTPUT"; then
  echo "ERROR: repl output did not include expected count" >&2
  echo "$REPL_OUTPUT" >&2
  exit 1
fi

echo "[6/7] Testing help output"
HELP_OUTPUT=$("$BUILD" --help)
if ! grep -q "serve" <<< "$HELP_OUTPUT" || ! grep -q "query" <<< "$HELP_OUTPUT"; then
  echo "ERROR: root help output is missing expected commands" >&2
  echo "$HELP_OUTPUT" >&2
  exit 1
fi

echo "[7/7] Testing serve command startup"
"$BUILD" serve --db "$DB2" --port "$PORT" > "$TMPDIR/serve.log" 2>&1 &
SERVER_PID=$!

$PYTHON - <<PY
import socket, time
for _ in range(50):
    try:
        s = socket.create_connection(("127.0.0.1", $PORT), timeout=1)
        s.close()
        break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit("port did not open")
PY

echo "serve accepted connections on port $PORT"
kill "$SERVER_PID"
wait "$SERVER_PID" || true
SERVER_PID=0

echo "All Porter CLI smoke tests passed."

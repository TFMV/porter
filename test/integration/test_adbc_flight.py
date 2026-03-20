import pytest
import subprocess
import time
import adbc_driver_flightsql
import adbc_driver_manager
import pyarrow
import random

@pytest.fixture(scope="session")
def duckdb_conn():
    port = str(random.randint(32000, 33000))
    process = subprocess.Popen(["../../build/bin/porter", "serve", "--address", f"0.0.0.0:{port}"])
    time.sleep(5)

    with adbc_driver_flightsql.connect(f"grpc://127.0.0.1:{port}") as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn

    conn.close()
    process.kill()
    process.wait()

def test_select_literal(duckdb_conn):
    with adbc_driver_manager.AdbcStatement(duckdb_conn) as stmt:
        stmt.set_sql_query("SELECT 42 AS Answer")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all().num_rows == 1

def test_create_select(duckdb_conn):
    with adbc_driver_manager.AdbcStatement(duckdb_conn) as stmt:
        stmt.set_sql_query("CREATE TABLE test_table(id INTEGER, name TEXT)")
        _ = stmt.execute_update()

    with adbc_driver_manager.AdbcStatement(duckdb_conn) as stmt:
        stmt.set_sql_query("INSERT INTO test_table VALUES (1, 'hello')")
        _ = stmt.execute_update()

    with adbc_driver_manager.AdbcStatement(duckdb_conn) as stmt:
        stmt.set_sql_query("SELECT * FROM test_table")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all().num_rows == 1

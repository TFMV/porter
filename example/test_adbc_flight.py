import pytest
import adbc_driver_manager.dbapi as adbc

URI = "grpc://localhost:32010"
DRIVER = "/Users/tfmv/Library/Application Support/ADBC/Drivers/flightsql_macos_arm64_v1.10.0/libadbc_driver_flightsql.dylib"


@pytest.fixture(scope="module")
def conn():
    c = adbc.connect(
        driver=DRIVER,
        db_kwargs={"uri": URI},
    )
    yield c
    c.close()


def test_connection(conn):
    assert conn is not None


def test_select_literal(conn):
    cur = conn.cursor()
    cur.execute("SELECT 42 AS answer")

    rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == 42

    cur.close()


def test_create_insert_select(conn):
    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS test_table(id INTEGER, name TEXT)")
    cur.execute("INSERT INTO test_table VALUES (1, 'hello')")
    cur.execute("SELECT * FROM test_table")

    rows = cur.fetchall()

    assert len(rows) >= 1
    assert rows[0][0] == 1
    assert rows[0][1] == "hello"

    cur.close()

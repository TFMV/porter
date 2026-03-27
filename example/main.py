#!/usr/bin/env python3

"""
Porter DBAPI Test Script

Covers:
- Basic query
- Prepared statements (parameter binding)
- Streaming fetch
- Schema introspection
- DDL + query

Requirements:
    pip install adbc-driver-manager adbc-driver-flightsql pyarrow
"""

import contextlib
import traceback

import adbc_driver_manager.dbapi as adbc


URI = "grpc://localhost:32010"

DRIVER = "/Users/tfmv/Library/Application Support/ADBC/Drivers/flightsql_macos_arm64_v1.10.0/libadbc_driver_flightsql.dylib"

def connect():
    return adbc.connect(
        driver=DRIVER,
        db_kwargs={"uri": URI},
    )


@contextlib.contextmanager
def cursor(conn):
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def run_test(name, fn, conn):
    print(f"\n=== {name} ===")
    try:
        fn(conn)
    except Exception:
        print(f"FAILED: {name}")
        traceback.print_exc()


def basic_query(conn):
    with cursor(conn) as cur:
        cur.execute("SELECT 42 AS answer, 'porter' AS name")
        rows = cur.fetchall()

        for row in rows:
            print(row)


def prepared_statement(conn):
    print("\n=== Prepared Statement ===")
    with cursor(conn) as cur:
        try:
            cur.execute(
                "SELECT ? AS id, ? AS name",
                [1, "porter"],
            )
            rows = cur.fetchall()
            for row in rows:
                print(row)
        except Exception:
            print("Prepared statement failed: SQL=SELECT ? AS id, ? AS name, params=[1, 'porter']")
            raise


def streaming_query(conn):
    print("\n=== Streaming Query ===")
    with cursor(conn) as cur:
        cur.execute("SELECT * FROM range(10000)")

        total = 0
        while True:
            batch = cur.fetchmany(1024)
            if not batch:
                break
            total += len(batch)
            print(f"Read batch of {len(batch)} rows")

        print(f"Total rows read: {total}")


def schema_introspection(conn):
    print("\n=== Schema Introspection ===")
    with cursor(conn) as cur:
        cur.execute("SELECT 1 AS id, 'test' AS name")
        for col in cur.description:
            print(col)


def ddl_and_query(conn):
    print("\n=== DDL + Query ===")
    with cursor(conn) as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS t AS SELECT * FROM range(10)")
        cur.execute("SELECT * FROM t")

        rows = cur.fetchall()
        for row in rows:
            print(row)


def main():
    print("Connecting to Porter at", URI)

    conn = connect()
    try:
        run_test("Basic query", basic_query, conn)
        run_test("Prepared statement", prepared_statement, conn)
        run_test("Streaming query", streaming_query, conn)
        run_test("Schema introspection", schema_introspection, conn)
        run_test("DDL + query", ddl_and_query, conn)
    finally:
        conn.close()
        print("\nConnection closed")


if __name__ == "__main__":
    main()

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func main() {
	ctx := context.Background()

	var drv drivermgr.Driver

	// ─────────────────────────────────────────────
	// 1. Open ADBC database (Flight SQL)
	// ─────────────────────────────────────────────
	db, err := drv.NewDatabase(map[string]string{
		"driver": "flightsql",
		"uri":    "grpc+tcp://127.0.0.1:32010",
		// add auth here if needed:
		// "username": "user",
		// "password": "pass",
	})
	if err != nil {
		log.Fatal("NewDatabase:", err)
	}
	defer db.Close()

	// ─────────────────────────────────────────────
	// 2. Open connection
	// ─────────────────────────────────────────────
	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal("Open:", err)
	}
	defer conn.Close()

	// ─────────────────────────────────────────────
	// 3. Simple query execution
	// ─────────────────────────────────────────────
	fmt.Println("=== Simple Query ===")

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	sql := `
		SELECT 1 AS id, 'porter' AS name
		UNION ALL
		SELECT 2, 'flight'
	`

	if err := stmt.SetSqlQuery(sql); err != nil {
		log.Fatal(err)
	}

	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	printStream(stream)

	// ─────────────────────────────────────────────
	// 4. Get schema (Flight GetSchema path)
	// ─────────────────────────────────────────────
	fmt.Println("\n=== Schema Only ===")

	streamForSchema, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal("ExecuteQuery for schema:", err)
	}
	defer streamForSchema.Release()
	schema := streamForSchema.Schema()
	fmt.Println(schema)

	// ─────────────────────────────────────────────
	// 5. Prepared statement lifecycle
	// ─────────────────────────────────────────────
	fmt.Println("\n=== Prepared Statement ===")

	prep, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer prep.Close()

	// NOTE: Porter may or may not support parameters yet.
	// Keep it simple + portable.
	prepSQL := `
		SELECT *
		FROM (
			SELECT 1 AS id, 'alpha' AS name
			UNION ALL
			SELECT 2, 'beta'
			UNION ALL
			SELECT 3, 'gamma'
		) t
		WHERE id > 1
	`

	if err := prep.SetSqlQuery(prepSQL); err != nil {
		log.Fatal(err)
	}

	// Prepare (Flight DoAction CreatePreparedStatement)
	if err := prep.Prepare(ctx); err != nil {
		log.Fatal("Prepare:", err)
	}

	// Execute prepared (Flight ExecutePreparedStatement)
	stream2, _, err := prep.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal("ExecutePrepared:", err)
	}
	printStream(stream2)

	// ─────────────────────────────────────────────
	// 6. Re-execute prepared (plan reuse)
	// ─────────────────────────────────────────────
	fmt.Println("\n=== Re-execute Prepared ===")

	stream3, _, err := prep.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	printStream(stream3)

	// ─────────────────────────────────────────────
	// 7. ExecuteUpdate (DDL / DML path)
	// ─────────────────────────────────────────────
	fmt.Println("\n=== ExecuteUpdate ===")

	upd, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer upd.Close()

	if err := upd.SetSqlQuery(`CREATE TABLE IF NOT EXISTS demo (id INT)`); err != nil {
		log.Fatal(err)
	}

	_, err = upd.ExecuteUpdate(ctx)
	if err != nil {
		log.Fatal("ExecuteUpdate:", err)
	}
	fmt.Println("table created")

	// insert
	if err := upd.SetSqlQuery(`INSERT INTO demo VALUES (1), (2), (3)`); err != nil {
		log.Fatal(err)
	}

	rows, err := upd.ExecuteUpdate(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("rows inserted:", rows)

	// verify
	verify, _ := conn.NewStatement()
	defer verify.Close()

	verify.SetSqlQuery(`SELECT * FROM demo`)
	stream4, _, err := verify.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	printStream(stream4)

	// ─────────────────────────────────────────────
	// 8. Cleanup happens via defer
	// ─────────────────────────────────────────────
	fmt.Println("\n=== Done ===")
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func printStream(stream array.RecordReader) {
	// 1. Ensure the stream is released to avoid memory leaks
	defer stream.Release()

	// 2. Iterate through batches of records
	for stream.Next() {
		// In Arrow-Go, the method is .Record(), not .RecordBatch()
		rec := stream.RecordBatch()

		// 3. Process the record (batch of rows)
		fmt.Printf("Read batch with %d rows and %d columns\n", rec.NumRows(), rec.NumCols())
		fmt.Println(rec)
	}

	// 4. Check for errors after the loop
	if err := stream.Err(); err != nil {
		log.Fatal("stream error:", err)
	}
}

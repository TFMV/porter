package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

func main() {
	ctx := context.Background()

	var drv drivermgr.Driver

	// ─────────────────────────────────────────────
	// 1. Open ADBC database
	// ─────────────────────────────────────────────
	db, err := drv.NewDatabase(map[string]string{
		"driver": "flightsql",
		"uri":    "grpc+tcp://localhost:32010",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// ─────────────────────────────────────────────
	// 2. Open connection
	// ─────────────────────────────────────────────
	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// ─────────────────────────────────────────────
	// 3. Create statement
	// ─────────────────────────────────────────────
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(`
		SELECT 1 AS id, 'porter' AS name
		UNION ALL
		SELECT 2, 'flight'
	`); err != nil {
		log.Fatal(err)
	}

	// ─────────────────────────────────────────────
	// 4. Execute
	// ─────────────────────────────────────────────
	stream, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Release()

	// ─────────────────────────────────────────────
	// 5. Consume Arrow record batches
	// ─────────────────────────────────────────────
	for stream.Next() {
		batch := stream.RecordBatch()
		fmt.Println(batch)
	}

	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}
}

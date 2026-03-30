package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	flightSQLClient, err := fsql.NewClient("127.0.0.1:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("FlightSQL client:", err)
	}
	defer flightSQLClient.Close()

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
	verify, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer verify.Close()

	if err := verify.SetSqlQuery(`SELECT * FROM demo`); err != nil {
		log.Fatal(err)
	}
	stream4, _, err := verify.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}
	printStream(stream4)

	// ─────────────────────────────────────────────
	// 8. IngestStream test (Arrow bulk ingest path)
	// ─────────────────────────────────────────────
	fmt.Println("\n=== ExecuteIngest (Arrow Bulk Ingest) ===")

	ingestSQL := `DROP TABLE IF EXISTS ingest_demo`

	upd2, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer upd2.Close()

	if err := upd2.SetSqlQuery(ingestSQL); err != nil {
		log.Fatal(err)
	}

	if _, err := upd2.ExecuteUpdate(ctx); err != nil {
		log.Fatal("reset ingest table:", err)
	}

	// ─────────────────────────────────────────────
	// Build Arrow schema + batches
	// ─────────────────────────────────────────────
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	ingestSchema := arrow.NewSchema(fields, nil)

	makeBatch := func(ids []int64, names []string) arrow.RecordBatch {
		b := array.NewRecordBuilder(memory.NewGoAllocator(), ingestSchema)
		defer b.Release()

		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(names, nil)

		return b.NewRecordBatch()
	}

	rec1 := makeBatch([]int64{1, 2}, []string{"a", "b"})
	rec2 := makeBatch([]int64{3, 4}, []string{"c", "d"})
	rec3 := makeBatch([]int64{5, 6}, []string{"e", "f"})

	defer rec1.Release()
	defer rec2.Release()
	defer rec3.Release()

	// ─────────────────────────────────────────────
	// Correct Arrow RecordReader
	// ─────────────────────────────────────────────
	reader, err := array.NewRecordReader(ingestSchema, []arrow.RecordBatch{
		rec1, rec2, rec3,
	})
	if err != nil {
		log.Fatal("record reader:", err)
	}
	defer reader.Release()

	// ─────────────────────────────────────────────
	// Execute ingest
	// ─────────────────────────────────────────────
	rowsIngested, err := flightSQLClient.ExecuteIngest(ctx, reader, &fsql.ExecuteIngestOpts{
		TableDefinitionOptions: &fsql.TableDefinitionOptions{
			IfNotExist: fsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   fsql.TableDefinitionOptionsTableExistsOptionReplace,
		},
		Table: "ingest_demo",
	})
	if err != nil {
		log.Fatal("ExecuteIngest failed:", err)
	}

	fmt.Println("rows ingested:", rowsIngested)

	// ─────────────────────────────────────────────
	// verify
	// ─────────────────────────────────────────────
	verify2, err := conn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer verify2.Close()

	if err := verify2.SetSqlQuery(`SELECT COUNT(*) FROM ingest_demo`); err != nil {
		log.Fatal(err)
	}

	stream5, _, err := verify2.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal(err)
	}

	printStream(stream5)

	// ─────────────────────────────────────────────
	// 9. DoExchange test
	// ─────────────────────────────────────────────
	fmt.Println("\n=== DoExchange ===")

	flightClient, err := flight.NewClientWithMiddleware("127.0.0.1:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("NewFlightClient:", err)
	}
	defer flightClient.Close()

	exchangeStream, err := flightClient.DoExchange(ctx)
	if err != nil {
		log.Fatal("DoExchange:", err)
	}

	if err := exchangeStream.Send(&flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte(`SELECT 1 AS id, 'exchange' AS name`),
		},
	}); err != nil {
		log.Fatal("DoExchange send:", err)
	}

	if err := exchangeStream.CloseSend(); err != nil {
		log.Fatal("DoExchange close send:", err)
	}

	rd, err := flight.NewRecordReader(exchangeStream)
	if err != nil {
		log.Fatal("DoExchange reader:", err)
	}
	printStream(rd)

	// ─────────────────────────────────────────────
	// 10. Cleanup happens via defer
	// ─────────────────────────────────────────────
	fmt.Println("=== Done ===")
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

type testRecordReader struct {
	schema *arrow.Schema
	recs   []arrow.RecordBatch
	i      int
}

func (t *testRecordReader) Retain() {}

func (t *testRecordReader) Schema() *arrow.Schema {
	return t.schema
}

func (t *testRecordReader) Next() bool {
	if t.i >= len(t.recs) {
		return false
	}
	t.i++
	return true
}

func (t *testRecordReader) RecordBatch() arrow.RecordBatch {
	return t.recs[t.i-1]
}

func (t *testRecordReader) Err() error {
	return nil
}

func (t *testRecordReader) Release() {}

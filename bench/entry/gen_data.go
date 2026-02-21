package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	rows := flag.Int("rows", 1000, "number of rows")
	cols := flag.Int("cols", 5, "number of integer columns")
	format := flag.String("format", "parquet", "output format: parquet or csv")
	out := flag.String("out", "bench.parquet", "output file path")
	flag.Parse()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var fields []string
	for i := 1; i <= *cols; i++ {
		fields = append(fields, fmt.Sprintf("c%d INTEGER", i))
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE bench(%s)", strings.Join(fields, ","))); err != nil {
		log.Fatalf("create table: %v", err)
	}

	insertCols := strings.Repeat("random(),", *cols)
	insertCols = strings.TrimSuffix(insertCols, ",")
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO bench SELECT %s FROM range(0,%d)", insertCols, *rows)); err != nil {
		log.Fatalf("insert rows: %v", err)
	}

	fmt.Printf("writing %s with %d rows...\n", *out, *rows)
	if _, err := db.Exec(fmt.Sprintf("COPY bench TO '%s' (FORMAT '%s')", *out, *format)); err != nil {
		log.Fatalf("export: %v", err)
	}
}

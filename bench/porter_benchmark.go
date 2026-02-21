package bench

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func setupDB(b *testing.B) *sql.DB {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t AS SELECT i as id, random() AS v FROM range(0,100000) tbl(i)"); err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkCount(b *testing.B) {
	db := setupDB(b)
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := db.QueryRow("SELECT count(*) FROM t")
		var c int
		if err := row.Scan(&c); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFilter(b *testing.B) {
	db := setupDB(b)
	defer db.Close()
	stmt, err := db.Prepare("SELECT v FROM t WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := stmt.QueryRow(i % 100000)
		var v float64
		if err := row.Scan(&v); err != nil {
			b.Fatal(err)
		}
	}
}

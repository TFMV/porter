package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	internaladbc "github.com/TFMV/porter/internal/adbc"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:32010", "Porter FlightSQL address")
	flag.Parse()

	ctx := context.Background()

	manager, err := internaladbc.NewManager()
	if err != nil {
		log.Fatal("create ADBC manager:", err)
	}
	resolved, err := manager.EnsureRequiredDrivers()
	if err != nil {
		log.Fatal("resolve ADBC drivers:", err)
	}

	previousDriverPath := os.Getenv("ADBC_DRIVER_PATH")
	if err := os.Setenv("ADBC_DRIVER_PATH", resolved["flightsql"].LibPath); err != nil {
		log.Fatal("set ADBC_DRIVER_PATH:", err)
	}
	defer func() {
		if previousDriverPath == "" {
			_ = os.Unsetenv("ADBC_DRIVER_PATH")
			return
		}
		_ = os.Setenv("ADBC_DRIVER_PATH", previousDriverPath)
	}()

	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver": "flightsql",
		"uri":    "grpc+tcp://" + *addr,
	})
	if err != nil {
		log.Fatal("open FlightSQL database:", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal("open FlightSQL connection:", err)
	}
	defer conn.Close()

	mustExecuteUpdate(ctx, conn, "CREATE TABLE IF NOT EXISTS ducklake_demo (id INTEGER)")
	mustExecuteUpdate(ctx, conn, "INSERT INTO ducklake_demo VALUES (1), (2)")

	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal("new statement:", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT id FROM ducklake_demo ORDER BY id"); err != nil {
		log.Fatal("set query:", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal("execute query:", err)
	}
	defer reader.Release()

	fmt.Println("ducklake_demo")
	for reader.Next() {
		record := reader.Record()
		if record == nil || record.NumCols() == 0 {
			continue
		}
		ids := record.Column(0).(*array.Int32)
		for i := 0; i < ids.Len(); i++ {
			fmt.Printf("id=%d\n", ids.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		log.Fatal("read Arrow stream:", err)
	}
}

func mustExecuteUpdate(ctx context.Context, conn adbc.Connection, sql string) {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal("new update statement:", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		log.Fatal("set update SQL:", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		log.Fatal("execute update:", err)
	}
}

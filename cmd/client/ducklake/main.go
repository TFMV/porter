package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	internaladbc "github.com/TFMV/porter/internal/adbc"
	internalducklake "github.com/TFMV/porter/internal/ducklake"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:32010", "Porter FlightSQL address")
	catalogType := flag.String("ducklake-catalog-type", "duckdb", "DuckLake catalog type")
	catalogDSN := flag.String("ducklake-catalog-dsn", "metadata.ducklake", "DuckLake catalog DSN or file path")
	dataPath := flag.String("ducklake-data-path", "", "DuckLake data path")
	name := flag.String("ducklake-name", "my_ducklake", "DuckLake catalog name")
	snapshotVersion := flag.Int64("snapshot-version", -1, "Optional DuckLake snapshot version to query")
	inspectMetadata := flag.Bool("inspect-metadata", true, "Inspect DuckLake metadata functions")
	runMaintenance := flag.Bool("run-maintenance", false, "Run DuckLake maintenance commands")
	flag.Parse()

	cfg := internalducklake.Config{
		Enabled:     true,
		CatalogType: *catalogType,
		CatalogDSN:  *catalogDSN,
		DataPath:    *dataPath,
		Name:        *name,
	}.Normalize()

	ctx := context.Background()
	conn := openFlightSQLConnection(ctx, *addr)
	defer conn.Close()

	fmt.Printf("DuckLake target: type=%s dsn=%q data_path=%q name=%q\n", cfg.CatalogType, cfg.CatalogDSN, cfg.DataPath, cfg.Name)

	if *inspectMetadata {
		printQuery(ctx, conn, fmt.Sprintf("SELECT catalog_type, data_path FROM ducklake_settings('%s')", cfg.Name))
	}

	mustExecuteUpdate(ctx, conn, "CREATE TABLE IF NOT EXISTS ducklake_demo (id BIGINT)")
	mustExecuteUpdate(ctx, conn, "INSERT INTO ducklake_demo VALUES (1), (2)")
	firstSnapshot := latestSnapshot(ctx, conn, cfg.Name)
	mustExecuteUpdate(ctx, conn, "INSERT INTO ducklake_demo VALUES (3)")

	fmt.Println("\nCurrent rows")
	printQuery(ctx, conn, "SELECT id FROM ducklake_demo ORDER BY id")

	version := *snapshotVersion
	if version < 0 {
		version = firstSnapshot
	}
	fmt.Printf("\nSnapshot rows at version %d\n", version)
	printQuery(ctx, conn, fmt.Sprintf("SELECT id FROM ducklake_demo AT (VERSION => %d) ORDER BY id", version))

	if *inspectMetadata {
		fmt.Println("\nDuckLake snapshots")
		printQuery(ctx, conn, fmt.Sprintf("FROM ducklake_snapshots('%s')", cfg.Name))

		fmt.Println("\nDuckLake table info")
		printQuery(ctx, conn, fmt.Sprintf("SELECT * FROM ducklake_table_info('%s')", cfg.Name))
	}

	if *runMaintenance {
		mustExecuteUpdate(ctx, conn, fmt.Sprintf("CALL ducklake_merge_adjacent_files('%s')", cfg.Name))
		mustExecuteUpdate(ctx, conn, fmt.Sprintf("CALL ducklake_expire_snapshots('%s', dry_run => true, older_than => now() + INTERVAL '1 day')", cfg.Name))
		mustExecuteUpdate(ctx, conn, fmt.Sprintf("CALL ducklake_cleanup_old_files('%s', dry_run => true, cleanup_all => true)", cfg.Name))
		fmt.Println("\nMaintenance commands completed")
	}
}

func openFlightSQLConnection(ctx context.Context, addr string) adbc.Connection {
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
		"uri":    "grpc+tcp://" + addr,
	})
	if err != nil {
		log.Fatal("open FlightSQL database:", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		_ = db.Close()
		log.Fatal("open FlightSQL connection:", err)
	}
	return &clientConnection{Connection: conn, db: db}
}

type clientConnection struct {
	adbc.Connection
	db adbc.Database
}

func (c *clientConnection) Close() error {
	closeErr := c.Connection.Close()
	dbErr := c.db.Close()
	if closeErr != nil {
		return closeErr
	}
	return dbErr
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

func latestSnapshot(ctx context.Context, conn adbc.Connection, catalog string) int64 {
	values := queryInts(ctx, conn, fmt.Sprintf("SELECT snapshot_id FROM ducklake_snapshots('%s') ORDER BY snapshot_id DESC LIMIT 1", catalog))
	if len(values) == 0 {
		log.Fatal("no DuckLake snapshots found")
	}
	return values[0]
}

func printQuery(ctx context.Context, conn adbc.Connection, sql string) {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal("new query statement:", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		log.Fatal("set query SQL:", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal("execute query:", err)
	}
	defer reader.Release()

	fmt.Println(sql)
	for reader.Next() {
		record := reader.Record()
		if record == nil {
			continue
		}
		printRecord(record)
	}
	if err := reader.Err(); err != nil {
		log.Fatal("read Arrow stream:", err)
	}
}

func printRecord(record arrow.Record) {
	for row := 0; row < int(record.NumRows()); row++ {
		values := make([]any, 0, record.NumCols())
		for col := 0; col < int(record.NumCols()); col++ {
			values = append(values, valueAt(record.Column(col), row))
		}
		fmt.Println(values...)
	}
}

func queryInts(ctx context.Context, conn adbc.Connection, sql string) []int64 {
	stmt, err := conn.NewStatement()
	if err != nil {
		log.Fatal("new int query statement:", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		log.Fatal("set int query SQL:", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatal("execute int query:", err)
	}
	defer reader.Release()

	var out []int64
	for reader.Next() {
		record := reader.Record()
		if record == nil || record.NumCols() == 0 {
			continue
		}
		out = appendIntValues(record.Column(0), out)
	}
	if err := reader.Err(); err != nil {
		log.Fatal("read int query:", err)
	}
	return out
}

func appendIntValues(column arrow.Array, dst []int64) []int64 {
	switch typed := column.(type) {
	case *array.Int64:
		for i := 0; i < typed.Len(); i++ {
			dst = append(dst, typed.Value(i))
		}
	case *array.Int32:
		for i := 0; i < typed.Len(); i++ {
			dst = append(dst, int64(typed.Value(i)))
		}
	default:
		log.Fatalf("unexpected integer Arrow column type %T", column)
	}
	return dst
}

func valueAt(column arrow.Array, row int) any {
	switch typed := column.(type) {
	case *array.Int64:
		return typed.Value(row)
	case *array.Int32:
		return typed.Value(row)
	case *array.String:
		return typed.Value(row)
	default:
		return fmt.Sprintf("<%T>", column)
	}
}

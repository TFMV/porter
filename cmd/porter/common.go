package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/spf13/viper"
)

type porterConfig struct {
	DBPath string
	Port   int
}

func loadConfig() porterConfig {
	return porterConfig{
		DBPath: viper.GetString("db"),
		Port:   viper.GetInt("port"),
	}
}

func normalizeDBPath(path string) string {
	if path == "" {
		return ":memory:"
	}
	return path
}

func openDuckDBConnection(ctx context.Context, dbPath string) (adbc.Database, adbc.Connection, error) {
	dbPath = normalizeDBPath(dbPath)

	var drv drivermgr.Driver

	// IMPORTANT: use URI-style DSN for DuckDB persistence
	dsn := "duckdb://:memory:"
	if dbPath != ":memory:" {
		dsn = fmt.Sprintf("duckdb://%s", dbPath)
	}

	adb, err := drv.NewDatabase(map[string]string{
		"driver": "duckdb",
		"uri":    dsn,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb %q: %w", dbPath, err)
	}

	conn, err := adb.Open(ctx)
	if err != nil {
		adb.Close()
		return nil, nil, fmt.Errorf("open connection: %w", err)
	}

	return adb, conn, nil
}

func withDuckDBConnection(ctx context.Context, dbPath string, fn func(adbc.Connection) error) error {
	db, conn, err := openDuckDBConnection(ctx, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	defer conn.Close()
	return fn(conn)
}

func quoteIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

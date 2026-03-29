package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	internaladbc "github.com/TFMV/porter/internal/adbc"
	apacheadbc "github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/spf13/viper"
)

type porterConfig struct {
	DBPath   string
	Port     int
	WsEnable bool
	WsPort   int
}

func loadConfig() porterConfig {
	return porterConfig{
		DBPath:   viper.GetString("db"),
		Port:     viper.GetInt("port"),
		WsEnable: viper.GetBool("ws"),
		WsPort:   viper.GetInt("ws-port"),
	}
}

func normalizeDBPath(path string) string {
	if path == "" {
		return ":memory:"
	}
	return path
}

func ensureADBCDefaultDriver() error {
	manager, err := internaladbc.NewManager()
	if err != nil {
		return fmt.Errorf("initialize adbc manager: %w", err)
	}
	resolved, err := manager.EnsureDefaultDriver()
	if err != nil {
		return fmt.Errorf("ensure default adbc driver: %w", err)
	}
	if err := os.Setenv("ADBC_DRIVER_PATH", resolved.LibPath); err != nil {
		return fmt.Errorf("set ADBC_DRIVER_PATH: %w", err)
	}
	return nil
}

func openDuckDBConnection(ctx context.Context, dbPath string) (apacheadbc.Database, apacheadbc.Connection, error) {
	dbPath = normalizeDBPath(dbPath)

	if err := ensureADBCDefaultDriver(); err != nil {
		return nil, nil, err
	}

	var drv drivermgr.Driver

	params := map[string]string{
		"driver": "duckdb",
		"path":   dbPath,
	}
	if dbPath != ":memory:" {
		params["access_mode"] = "read_write"
	}

	adb, err := drv.NewDatabase(params)
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

func withDuckDBConnection(ctx context.Context, dbPath string, fn func(apacheadbc.Connection) error) error {
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

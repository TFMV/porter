package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	internaladbc "github.com/TFMV/porter/internal/adbc"
	internalducklake "github.com/TFMV/porter/internal/ducklake"
	apacheadbc "github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/spf13/viper"
)

type porterConfig struct {
	DBPath       string
	Port         int
	WsEnable     bool
	WsPort       int
	StatusEnable bool
	StatusPort   int
	DuckLake     internalducklake.Config
}

func loadConfig() porterConfig {
	return porterConfig{
		DBPath:       viper.GetString("db"),
		Port:         viper.GetInt("port"),
		WsEnable:     viper.GetBool("ws"),
		WsPort:       viper.GetInt("ws-port"),
		StatusEnable: viper.GetBool("status"),
		StatusPort:   viper.GetInt("status-port"),
		DuckLake: internalducklake.Config{
			Enabled:     viper.GetBool("ducklake"),
			CatalogType: viper.GetString("ducklake-catalog-type"),
			CatalogDSN:  viper.GetString("ducklake-catalog-dsn"),
			DataPath:    viper.GetString("ducklake-data-path"),
			Name:        viper.GetString("ducklake-name"),
		}.Normalize(),
	}
}

func normalizeDBPath(path string) string {
	if path == "" {
		return ":memory:"
	}
	return path
}

func ensureRequiredADBCDrivers() error {
	manager, err := internaladbc.NewManager()
	if err != nil {
		return fmt.Errorf("initialize adbc manager: %w", err)
	}
	resolved, err := manager.EnsureRequiredDrivers()
	if err != nil {
		return err
	}
	if err := os.Setenv("ADBC_DRIVER_PATH", resolved["duckdb"].LibPath); err != nil {
		return fmt.Errorf("set ADBC_DRIVER_PATH: %w", err)
	}
	return nil
}

func openDuckDBConnection(ctx context.Context, dbPath string) (apacheadbc.Database, apacheadbc.Connection, error) {
	dbPath = normalizeDBPath(dbPath)

	if err := ensureRequiredADBCDrivers(); err != nil {
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

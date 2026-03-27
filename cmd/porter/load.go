package main

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"
)

var loadCmd = &cobra.Command{
	Use:   "load [file.parquet]",
	Short: "Load a dataset into Porter",
	Long:  "Load a Parquet file into the configured DuckDB database.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := loadConfig()
		return runLoad(cfg, args[0])
	},
}

func runLoad(cfg porterConfig, path string) error {
	if !strings.EqualFold(filepath.Ext(path), ".parquet") {
		return fmt.Errorf("unsupported format %q: only .parquet is supported", filepath.Ext(path))
	}

	table := sanitizeTableName(filepath.Base(path))
	if table == "" {
		table = "data"
	}

	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM parquet_scan('%s')",
		quoteIdentifier(table),
		quoteSQLString(path),
	)

	ctx := context.Background()
	return withDuckDBConnection(ctx, cfg.DBPath, func(conn adbc.Connection) error {
		stmt, err := conn.NewStatement()
		if err != nil {
			return err
		}
		defer stmt.Close()

		if err := stmt.SetSqlQuery(sql); err != nil {
			return err
		}

		_, err = stmt.ExecuteUpdate(ctx)
		if err != nil {
			return err
		}

		count, err := countTableRows(ctx, conn, table)
		if err != nil {
			return err
		}

		fmt.Printf("loaded %s into table %s (%d rows)\n", path, quoteIdentifier(table), count)
		return nil
	})
}

func sanitizeTableName(name string) string {
	name = strings.TrimSuffix(name, filepath.Ext(name))
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	re := regexp.MustCompile(`[^a-z0-9_]+`)
	return re.ReplaceAllString(name, "_")
}

func countTableRows(ctx context.Context, conn adbc.Connection, table string) (int64, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	sql := fmt.Sprintf("SELECT COUNT(*) AS count FROM %s", quoteIdentifier(table))
	if err := stmt.SetSqlQuery(sql); err != nil {
		return 0, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, err
	}
	defer reader.Release()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("unable to read row count")
	}

	rec := reader.RecordBatch()
	defer rec.Release()
	if rec.NumCols() != 1 || rec.NumRows() != 1 {
		return 0, fmt.Errorf("unexpected count result")
	}

	col := rec.Column(0)
	if col.IsNull(0) {
		return 0, nil
	}

	switch c := col.(type) {
	case *array.Int8:
		return int64(c.Value(0)), nil
	case *array.Int16:
		return int64(c.Value(0)), nil
	case *array.Int32:
		return int64(c.Value(0)), nil
	case *array.Int64:
		return c.Value(0), nil
	default:
		return 0, fmt.Errorf("unexpected count type %T", c)
	}
}

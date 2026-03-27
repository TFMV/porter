package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query [sql]",
	Short: "Execute a single SQL statement",
	Long:  "Execute SQL against the configured Porter database and print Arrow results in a readable CLI format.",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := loadConfig()
		sql := strings.Join(args, " ")
		return runQuery(cfg, sql)
	},
}

func runQuery(cfg porterConfig, sql string) error {
	return withDuckDBConnection(context.Background(), cfg.DBPath, func(conn adbc.Connection) error {
		stmt, err := conn.NewStatement()
		if err != nil {
			return err
		}
		defer stmt.Close()

		if err := stmt.SetSqlQuery(sql); err != nil {
			return err
		}

		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			return err
		}
		return printQueryResults(reader)
	})
}

func printQueryResults(reader array.RecordReader) error {
	defer reader.Release()

	if err := maybePrintHeader(reader.Schema()); err != nil {
		return err
	}

	rowCount := 0
	for reader.Next() {
		rec := reader.RecordBatch()
		cols := rec.Columns()
		colsCount := int(rec.NumCols())
		rowsCount := int(rec.NumRows())

		for row := 0; row < rowsCount; row++ {
			values := make([]string, colsCount)
			for col := 0; col < colsCount; col++ {
				values[col] = formatScalar(cols[col], row)
			}
			fmt.Println(strings.Join(values, "\t"))
			rowCount++
		}
		rec.Release()
	}

	if err := reader.Err(); err != nil {
		return err
	}

	if rowCount == 0 {
		fmt.Println("(no rows)")
	}
	return nil
}

func maybePrintHeader(schema *arrow.Schema) error {
	if schema == nil || len(schema.Fields()) == 0 {
		return nil
	}

	names := make([]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		names[i] = field.Name
	}
	fmt.Println(strings.Join(names, "\t"))
	fmt.Println(strings.Repeat("-", len(strings.Join(names, "\t"))))
	return nil
}

func formatScalar(arr arrow.Array, index int) string {
	if arr.IsNull(index) {
		return "NULL"
	}

	switch col := arr.(type) {
	case *array.Boolean:
		return fmt.Sprintf("%t", col.Value(index))
	case *array.Int8:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Int16:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Int32:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Int64:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Uint8:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Uint16:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Uint32:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Uint64:
		return fmt.Sprintf("%d", col.Value(index))
	case *array.Float32:
		return fmt.Sprintf("%g", col.Value(index))
	case *array.Float64:
		return fmt.Sprintf("%g", col.Value(index))
	case *array.String:
		return col.Value(index)
	case *array.Binary:
		return string(col.Value(index))
	default:
		return fmt.Sprintf("%v", col)
	}
}

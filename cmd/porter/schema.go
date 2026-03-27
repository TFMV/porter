package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema [table_name]",
	Short: "Print the schema for a table",
	Long:  "Display the column schema for a table in the configured Porter database.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := loadConfig()
		return runSchema(cfg, args[0])
	},
}

func runSchema(cfg porterConfig, table string) error {
	ctx := context.Background()

	return withDuckDBConnection(ctx, cfg.DBPath, func(conn adbc.Connection) error {
		stmt, err := conn.NewStatement()
		if err != nil {
			return err
		}
		defer stmt.Close()

		sql := fmt.Sprintf("SELECT * FROM %s LIMIT 0", quoteIdentifier(table))
		if err := stmt.SetSqlQuery(sql); err != nil {
			return err
		}

		reader, _, err := stmt.ExecuteQuery(ctx)
		if err != nil {
			return err
		}
		defer reader.Release()

		return printSchema(reader.Schema())
	})
}

func printSchema(schema *arrow.Schema) error {
	if schema == nil || len(schema.Fields()) == 0 {
		fmt.Println("(no schema available)")
		return nil
	}

	fmt.Printf("%-20s %-20s %-8s\n", "COLUMN", "TYPE", "NULLABLE")
	fmt.Printf("%-20s %-20s %-8s\n", strings.Repeat("-", 20), strings.Repeat("-", 20), strings.Repeat("-", 8))
	for _, field := range schema.Fields() {
		fmt.Printf("%-20s %-20s %-8t\n", field.Name, field.Type.String(), field.Nullable)
	}
	return nil
}

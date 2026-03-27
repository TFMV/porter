package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Start an interactive Porter SQL session",
	Long:  "Run an interactive SQL shell that keeps session state and accepts multi-line input.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runRepl(loadConfig())
	},
}

func runRepl(cfg porterConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	adb, conn, err := openDuckDBConnection(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer adb.Close()
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Porter REPL (db=%s). Type .exit to quit.\n", cfg.DBPath)

	var buffer strings.Builder
	for {
		fmt.Print("porter> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == os.ErrClosed {
				return nil
			}
			return err
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == ".exit" {
			return nil
		}

		buffer.WriteString(line)
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			sql := strings.TrimSpace(buffer.String())
			buffer.Reset()
			if sql == ".exit;" {
				return nil
			}

			if err := executeReplStatement(ctx, conn, sql); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			}
		}
	}
}

func executeReplStatement(ctx context.Context, conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err == nil {
		return printQueryResults(reader)
	}

	rows, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("ok, %d rows affected\n", rows)
	return nil
}

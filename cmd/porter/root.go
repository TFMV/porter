package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "porter",
	Short: "Porter is a lightweight FlightSQL developer tool",
	Long: `Porter is a developer-facing CLI for DuckDB and FlightSQL.
It supports running a standalone server, executing ad hoc SQL, loading Parquet data,
and introspecting schema from the command line.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe(loadConfig())
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("db", ":memory:", "DuckDB database path or :memory:")
	rootCmd.PersistentFlags().Int("port", 32010, "FlightSQL server port")
	rootCmd.PersistentFlags().Bool("ws", false, "Enable WebSocket endpoint")
	rootCmd.PersistentFlags().Int("ws-port", 8080, "WebSocket server port")
	rootCmd.PersistentFlags().Bool("status", true, "Enable live status endpoint")
	rootCmd.PersistentFlags().Int("status-port", 9091, "Status server port")
	rootCmd.PersistentFlags().Bool("ducklake", false, "Enable DuckLake at server startup")
	rootCmd.PersistentFlags().String("ducklake-catalog-type", "duckdb", "DuckLake catalog type: duckdb, sqlite, postgres, mysql")
	rootCmd.PersistentFlags().String("ducklake-catalog-dsn", "metadata.ducklake", "DuckLake catalog DSN or file path")
	rootCmd.PersistentFlags().String("ducklake-data-path", "", "DuckLake data path for Parquet/object storage")
	rootCmd.PersistentFlags().String("ducklake-name", "my_ducklake", "Attached DuckLake catalog name")

	if err := viper.BindPFlag("db", rootCmd.PersistentFlags().Lookup("db")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ws", rootCmd.PersistentFlags().Lookup("ws")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ws-port", rootCmd.PersistentFlags().Lookup("ws-port")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("status", rootCmd.PersistentFlags().Lookup("status")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("status-port", rootCmd.PersistentFlags().Lookup("status-port")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ducklake", rootCmd.PersistentFlags().Lookup("ducklake")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ducklake-catalog-type", rootCmd.PersistentFlags().Lookup("ducklake-catalog-type")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ducklake-catalog-dsn", rootCmd.PersistentFlags().Lookup("ducklake-catalog-dsn")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ducklake-data-path", rootCmd.PersistentFlags().Lookup("ducklake-data-path")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("ducklake-name", rootCmd.PersistentFlags().Lookup("ducklake-name")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	viper.SetDefault("db", ":memory:")
	viper.SetDefault("port", 32010)
	viper.SetDefault("ws", false)
	viper.SetDefault("ws-port", 8080)
	viper.SetDefault("status", true)
	viper.SetDefault("status-port", 9091)
	viper.SetDefault("ducklake", false)
	viper.SetDefault("ducklake-catalog-type", "duckdb")
	viper.SetDefault("ducklake-catalog-dsn", "metadata.ducklake")
	viper.SetDefault("ducklake-data-path", "")
	viper.SetDefault("ducklake-name", "my_ducklake")

	viper.SetEnvPrefix("PORTER")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	if err := viper.BindEnv("db"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("port"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ws"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ws-port"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("status"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("status-port"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ducklake"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ducklake-catalog-type"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ducklake-catalog-dsn"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ducklake-data-path"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindEnv("ducklake-name"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(replCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(schemaCmd)
}

func initConfig() {
	// No-op: flags and env vars are already bound.
}

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

	if err := viper.BindPFlag("db", rootCmd.PersistentFlags().Lookup("db")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	viper.SetDefault("db", ":memory:")
	viper.SetDefault("port", 32010)

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

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(replCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(schemaCmd)
}

func initConfig() {
	// No-op: flags and env vars are already bound.
}

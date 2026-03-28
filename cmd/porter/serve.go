package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	flightsql "github.com/TFMV/porter/execution/adapter/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	dbPath string
	port   int
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start a standalone Porter FlightSQL server",
	Long:  "Start a FlightSQL server backed by DuckDB and keep it running until interrupted.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := porterConfig{
			DBPath: dbPath,
			Port:   port,
		}
		return runServe(cfg)
	},
}

func init() {
	serveCmd.Flags().StringVar(&dbPath, "db", "", "DuckDB file path (use :memory: for in-memory)")
	serveCmd.Flags().IntVar(&port, "port", 32010, "Port to run FlightSQL server on")
}

func runServe(cfg porterConfig) error {
	cfg.DBPath = normalizeDBPath(cfg.DBPath)
	addr := fmt.Sprintf(":%d", cfg.Port)

	fmt.Printf("Starting Porter FlightSQL server on %s with DuckDB database at %q\n", addr, cfg.DBPath)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv, err := flightsql.NewServer(flightsql.Config{
		DBPath: cfg.DBPath,
	})

	if err != nil {
		return fmt.Errorf("initialize server: %w", err)
	}
	defer srv.Close()

	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, srv.AsFlightServer())

	log.Printf("starting Porter FlightSQL on %s (db=%s)", addr, cfg.DBPath)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcSrv.Serve(lis)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		grpcSrv.GracefulStop()
		return nil
	}
}

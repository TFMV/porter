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

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start a standalone Porter FlightSQL server",
	Long:  "Start a FlightSQL server backed by DuckDB and keep it running until interrupted.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe(loadConfig())
	},
}

func runServe(cfg porterConfig) error {
	cfg.DBPath = normalizeDBPath(cfg.DBPath)
	addr := fmt.Sprintf(":%d", cfg.Port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	srv, err := flightsql.NewServer(flightsql.Config{DBPath: cfg.DBPath, Port: cfg.Port})
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

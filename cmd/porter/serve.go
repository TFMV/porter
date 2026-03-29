package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	flightsql "github.com/TFMV/porter/execution/adapter/flightsql"
	ws "github.com/TFMV/porter/execution/adapter/ws"
	"github.com/TFMV/porter/execution/engine"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	dbPath   string
	port     int
	wsEnable bool
	wsPort   int
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start a Porter server (FlightSQL with optional WebSocket)",
	Long:  "Start a Porter server backed by DuckDB and keep it running until interrupted.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := porterConfig{
			DBPath:   dbPath,
			Port:     port,
			WsEnable: wsEnable,
			WsPort:   wsPort,
		}
		return runServe(cfg)
	},
}

func init() {
	serveCmd.Flags().StringVar(&dbPath, "db", "", "DuckDB file path (use :memory: for in-memory)")
	serveCmd.Flags().IntVar(&port, "port", 32010, "Port to run FlightSQL server on")
	serveCmd.Flags().BoolVar(&wsEnable, "ws", false, "Enable WebSocket endpoint")
	serveCmd.Flags().IntVar(&wsPort, "ws-port", 8080, "Port to run WebSocket server on")
}

func runServe(cfg porterConfig) error {
	cfg.DBPath = normalizeDBPath(cfg.DBPath)

	eng, err := engine.New(engine.Config{
		DBPath: cfg.DBPath,
	})
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}
	defer eng.Close()

	flightAddr := fmt.Sprintf(":%d", cfg.Port)
	fmt.Printf("Starting Porter FlightSQL server on %s with DuckDB database at %q\n", flightAddr, cfg.DBPath)

	lis, err := net.Listen("tcp", flightAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", flightAddr, err)
	}

	srv, err := flightsql.NewServer(flightsql.Config{
		Engine: eng,
	})
	if err != nil {
		return fmt.Errorf("initialize FlightSQL server: %w", err)
	}
	defer srv.Close()

	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, srv.AsFlightServer())

	log.Printf("starting Porter FlightSQL on %s (db=%s)", flightAddr, cfg.DBPath)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 2)

	// Start FlightSQL server
	go func() {
		errCh <- grpcSrv.Serve(lis)
	}()

	// Start WebSocket server if enabled
	if cfg.WsEnable {
		wsAddr := fmt.Sprintf(":%d", cfg.WsPort)
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		wsServer := ws.NewServer(eng, logger)
		go func() {
			log.Printf("starting Porter WebSocket on %s", wsAddr)
			errCh <- http.ListenAndServe(wsAddr, wsServer)
		}()
	}

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		grpcSrv.GracefulStop()
		return nil
	}
}

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
	"github.com/TFMV/porter/internal/adbc"
	"github.com/TFMV/porter/telemetry"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	dbPath       string
	port         int
	wsEnable     bool
	wsPort       int
	statusEnable bool
	statusPort   int
	duckLake     bool
	duckLakePath string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start a Porter server (FlightSQL with optional WebSocket)",
	Long:  "Start a Porter server backed by DuckDB and keep it running until interrupted.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := porterConfig{
			DBPath:       dbPath,
			Port:         port,
			WsEnable:     wsEnable,
			WsPort:       wsPort,
			StatusEnable: statusEnable,
			StatusPort:   statusPort,
			DuckLake:     duckLake,
			DuckLakePath: duckLakePath,
		}
		return runServe(cfg)
	},
}

func init() {
	serveCmd.Flags().StringVar(&dbPath, "db", "", "DuckDB file path (use :memory: for in-memory)")
	serveCmd.Flags().IntVar(&port, "port", 32010, "Port to run FlightSQL server on")
	serveCmd.Flags().BoolVar(&wsEnable, "ws", false, "Enable WebSocket endpoint")
	serveCmd.Flags().IntVar(&wsPort, "ws-port", 8080, "Port to run WebSocket server on")
	serveCmd.Flags().BoolVar(&statusEnable, "status", true, "Enable status endpoint")
	serveCmd.Flags().IntVar(&statusPort, "status-port", 9091, "Port to run status server on")
	serveCmd.Flags().BoolVar(&duckLake, "ducklake", false, "Enable DuckLake at server startup")
	serveCmd.Flags().StringVar(&duckLakePath, "ducklake-path", "metadata.ducklake", "DuckLake metadata path")
}

func duckLakeInitStatements(path string) []string {
	return []string{
		"INSTALL ducklake;",
		"LOAD ducklake;",
		fmt.Sprintf("ATTACH 'ducklake:%s' AS my_ducklake;", quoteSQLString(path)),
		"USE my_ducklake;",
	}
}

func buildEngineConfig(cfg porterConfig, adbcManager *adbc.Manager, publisher telemetry.Publisher) engine.Config {
	engineCfg := engine.Config{
		DBPath:      cfg.DBPath,
		ADBCManager: adbcManager,
		Telemetry:   publisher,
	}
	if cfg.DuckLake {
		statements := duckLakeInitStatements(cfg.DuckLakePath)
		engineCfg.StartupSQL = statements
		engineCfg.ConnectionInitSQL = []string{
			"LOAD ducklake;",
			"USE my_ducklake;",
		}
	}
	return engineCfg
}

func runServe(cfg porterConfig) error {
	cfg.DBPath = normalizeDBPath(cfg.DBPath)
	statusAggregator := telemetry.NewAggregator(telemetry.Config{})
	defer statusAggregator.Close()

	adbcManager, err := adbc.NewManager()
	if err != nil {
		return fmt.Errorf("initialize adbc manager: %w", err)
	}

	eng, err := engine.New(buildEngineConfig(cfg, adbcManager, statusAggregator))
	if err != nil {
		if cfg.DuckLake {
			return fmt.Errorf("initialize DuckLake: %w", err)
		}
		return fmt.Errorf("create engine: %w", err)
	}
	defer eng.Close()

	flightAddr := fmt.Sprintf(":%d", cfg.Port)
	fmt.Printf("Starting Porter FlightSQL server on %s with DuckDB database at %q\n", flightAddr, cfg.DBPath)
	if cfg.DuckLake {
		fmt.Printf("DuckLake enabled with metadata path %q\n", cfg.DuckLakePath)
	}

	lis, err := net.Listen("tcp", flightAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", flightAddr, err)
	}

	srv, err := flightsql.NewServer(flightsql.Config{
		Engine:    eng,
		Telemetry: statusAggregator,
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

	errCh := make(chan error, 4)

	// Start FlightSQL server
	go func() {
		errCh <- grpcSrv.Serve(lis)
	}()

	// Start WebSocket server if enabled
	if cfg.WsEnable {
		wsAddr := fmt.Sprintf(":%d", cfg.WsPort)
		logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
		wsServer := ws.NewServer(eng, logger, statusAggregator)
		go func() {
			log.Printf("starting Porter WebSocket on %s", wsAddr)
			errCh <- http.ListenAndServe(wsAddr, wsServer)
		}()
	}

	var statusHTTP *http.Server
	if cfg.StatusEnable {
		statusAddr := fmt.Sprintf(":%d", cfg.StatusPort)
		statusHTTP = &http.Server{
			Addr:    statusAddr,
			Handler: telemetry.NewServer(statusAggregator),
		}
		go func() {
			log.Printf("starting Porter status server on %s", statusAddr)
			errCh <- statusHTTP.ListenAndServe()
		}()
	}

	select {
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return fmt.Errorf("server error: %w", err)
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		grpcSrv.GracefulStop()
		if statusHTTP != nil {
			_ = statusHTTP.Close()
		}
		return nil
	}
}

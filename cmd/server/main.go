package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	flightsql "github.com/TFMV/porter"
	"github.com/apache/arrow-go/v18/arrow/flight"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultAddr     = ":32010"
	shutdownTimeout = 15 * time.Second
)

func main() {
	dbPath := os.Getenv("DUCKDB_PATH")
	if dbPath == "" {
		dbPath = ":memory:"
	}

	addr := os.Getenv("FLIGHT_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	srv, err := flightsql.NewServer(dbPath)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, srv)
	reflection.Register(grpcSrv) // optional: enables grpc_cli introspection

	// ── Graceful shutdown ──────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Flight SQL server listening on %s  (DuckDB: %s)", addr, dbPath)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down...")

	// Give in-flight RPCs time to complete.
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	select {
	case <-stopped:
		log.Println("clean shutdown")
	case <-ctx.Done():
		log.Println("shutdown timeout; forcing stop")
		grpcSrv.Stop()
	}

	fmt.Println("bye")
}

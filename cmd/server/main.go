package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	flightsql "github.com/TFMV/porter/execution/adapter/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultAddr     = ":32010"
	shutdownTimeout = 15 * time.Second
)

func main() {
	dbPath := getenv("DUCKDB_PATH", ":memory:")
	addr := getenv("FLIGHT_ADDR", defaultAddr)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	reflection.Register(grpcSrv)

	s, err := flightsql.NewServer(dbPath)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}
	flight.RegisterFlightServiceServer(grpcSrv, s.AsFlightServer())

	// ── serve ───────────────────────────────────────────────
	go func() {
		log.Printf("listening on %s", addr)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		grpcSrv.Stop()
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

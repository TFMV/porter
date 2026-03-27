package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	flightsql "github.com/TFMV/porter/execution/adapter/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
)

func main() {
	dbPath := flag.String("db", "", "database path (empty = in-memory)")
	port := flag.Int("port", 32010, "FlightSQL server port")

	flag.Parse()

	cfg := flightsql.Config{
		DBPath: *dbPath,
		Port:   *port,
	}

	srv, err := flightsql.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	grpcSrv := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcSrv, srv.AsFlightServer())

	log.Printf("starting Porter FlightSQL on %s (db=%s)", addr, *dbPath)
	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

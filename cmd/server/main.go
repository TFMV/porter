package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	flightsql "github.com/TFMV/porter"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//
// ─────────────────────────────────────────────────────────────────────────────
// ENTRYPOINT
// ─────────────────────────────────────────────────────────────────────────────
//

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := loadConfig()

	log.Info("starting flight sql server",
		"addr", cfg.addr,
		"shards", cfg.shards,
		"max_recv_mb", cfg.maxRecvMB,
		"max_send_mb", cfg.maxSendMB,
	)

	lis, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}

	srv := flightsql.NewServer(cfg.shards)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.maxRecvBytes()),
		grpc.MaxSendMsgSize(cfg.maxSendBytes()),
		grpc.Creds(insecure.NewCredentials()),
	)

	// IMPORTANT: register against gen/flight server interface
	flight.RegisterFlightServiceServer(grpcServer, srv)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)

	go func() {
		log.Info("server listening", "addr", cfg.addr)

		if err := grpcServer.Serve(lis); err != nil {
			serveErr <- err
		}
	}()

	// ─────────────────────────────────────────────────────────────
	// FAILURE PATH (SERVER DIED EARLY)
	// ─────────────────────────────────────────────────────────────
	select {
	case err := <-serveErr:
		log.Error("grpc server failed", "err", err)
		os.Exit(1)

	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	// ─────────────────────────────────────────────────────────────
	// GRACEFUL SHUTDOWN
	// ─────────────────────────────────────────────────────────────
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		// stop accepting new RPCs
		grpcServer.GracefulStop()

		// allow server-level cleanup hooks (if implemented later)
		if s, ok := any(srv).(interface {
			Shutdown(context.Context) error
		}); ok {
			_ = s.Shutdown(shutdownCtx)
		}
	}()

	select {
	case <-done:
		log.Info("shutdown complete")

	case <-shutdownCtx.Done():
		log.Warn("forced shutdown (timeout exceeded)")
		grpcServer.Stop()
	}
}

//
// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────
//

type config struct {
	addr      string
	shards    int
	maxRecvMB int
	maxSendMB int
}

func loadConfig() config {
	return config{
		addr:      getEnv("FLIGHT_ADDR", ":50051"),
		shards:    getEnvInt("FLIGHT_SHARDS", 4),
		maxRecvMB: getEnvInt("GRPC_MAX_RECV_MB", 64),
		maxSendMB: getEnvInt("GRPC_MAX_SEND_MB", 64),
	}
}

func (c config) maxRecvBytes() int {
	return c.maxRecvMB << 20
}

func (c config) maxSendBytes() int {
	return c.maxSendMB << 20
}

//
// ─────────────────────────────────────────────────────────────────────────────
// ENV HELPERS
// ─────────────────────────────────────────────────────────────────────────────
//

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return i
}

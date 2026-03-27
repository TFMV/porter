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

	porter "github.com/TFMV/porter"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()

	log.Info("starting flight sql server",
		"addr", cfg.Addr,
		"max_recv_mb", cfg.MaxRecvMB,
		"max_send_mb", cfg.MaxSendMB,
	)

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}

	srv := porter.NewServer(":memory:")

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.maxRecvBytes()),
		grpc.MaxSendMsgSize(cfg.maxSendBytes()),
		grpc.Creds(insecure.NewCredentials()),
	)

	flight.RegisterFlightServiceServer(grpcServer, srv)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)

	go func() {
		log.Info("grpc serving", "addr", cfg.Addr)
		errCh <- grpcServer.Serve(lis)
	}()

	select {
	case err := <-errCh:
		log.Error("grpc server error", "err", err)
		os.Exit(1)

	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shutdown(grpcServer, srv, shutdownCtx, log)
}

func shutdown(grpcServer *grpc.Server, srv any, ctx context.Context, log *slog.Logger) {
	done := make(chan struct{})

	go func() {
		defer close(done)

		grpcServer.GracefulStop()

		if s, ok := srv.(interface {
			Shutdown(context.Context) error
		}); ok {
			if err := s.Shutdown(ctx); err != nil {
				log.Warn("server shutdown error", "err", err)
			}
		}
	}()

	select {
	case <-done:
		log.Info("shutdown complete")

	case <-ctx.Done():
		log.Warn("forced shutdown (timeout)")
		grpcServer.Stop()
	}
}

//
// ─────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────
//

type config struct {
	Addr      string
	MaxRecvMB int
	MaxSendMB int
}

func loadConfig() config {
	return config{
		Addr:      env("FLIGHT_ADDR", ":50051"),
		MaxRecvMB: envInt("GRPC_MAX_RECV_MB", 64),
		MaxSendMB: envInt("GRPC_MAX_SEND_MB", 64),
	}
}

func (c config) maxRecvBytes() int { return c.MaxRecvMB << 20 }
func (c config) maxSendBytes() int { return c.MaxSendMB << 20 }

//
// ─────────────────────────────────────────────────────────────
// ENV HELPERS
// ─────────────────────────────────────────────────────────────
//

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
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

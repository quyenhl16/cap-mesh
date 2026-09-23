package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcserver"
	"github.com/quyenhl16/cap-mesh/internal/adapter/memory"
	metricadapter "github.com/quyenhl16/cap-mesh/internal/adapter/metrics"
	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	appstream "github.com/quyenhl16/cap-mesh/internal/application/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	listenAddress := flag.String("listen", ":18443", "gRPC listen address")
	metricsAddress := flag.String("metrics-listen", ":19090", "Prometheus metrics listen address")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	adminToken := flag.String("admin-token", os.Getenv("CAPMESH_ADMIN_TOKEN"), "bearer token allowed to create, view, and stop sessions")
	viewerToken := flag.String("viewer-token", os.Getenv("CAPMESH_VIEWER_TOKEN"), "bearer token allowed to view sessions and packets")
	agentToken := flag.String("agent-token", os.Getenv("CAPMESH_AGENT_TOKEN"), "bearer token allowed to connect agents")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file")
	tlsKey := flag.String("tls-key", "", "TLS private key file")
	subscriberQueue := flag.Int("subscriber-queue-size", 10000, "per-subscriber packet queue size")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		logger.Error("listen failed", "error", err)
		os.Exit(1)
	}

	registry := prometheus.NewRegistry()
	serverMetrics := metricadapter.NewServer(registry)
	packetService := appstream.NewService(serverMetrics)
	agents := grpcserver.NewAgentRegistry()
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, packetService, *subscriberQueue)
	agents.OnDisconnect(func(node string) {
		logger.Warn("marking sessions after agent disconnect", "node", node)
		sessions.AgentDisconnected(context.Background(), node)
	})

	auth := grpcserver.AuthConfig{SharedToken: *token, AdminToken: *adminToken, ViewerToken: *viewerToken, AgentToken: *agentToken}
	if *token == "" && *adminToken == "" && *viewerToken == "" && *agentToken == "" {
		logger.Warn("authentication is disabled; use only for local development")
	}
	options := []grpc.ServerOption{grpc.UnaryInterceptor(grpcserver.UnaryAuth(auth)), grpc.StreamInterceptor(grpcserver.StreamAuth(auth))}
	if (*tlsCert == "") != (*tlsKey == "") {
		logger.Error("both --tls-cert and --tls-key are required")
		os.Exit(2)
	}
	if *tlsCert != "" {
		certificate, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			logger.Error("load TLS certificate failed", "error", err)
			os.Exit(1)
		}
		options = append(options, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})))
	} else {
		logger.Warn("gRPC is running without TLS; use only for local development")
	}
	grpcServer := grpc.NewServer(options...)
	capmeshv1.RegisterAgentServiceServer(grpcServer, grpcserver.NewAgentServer(agents, packetService, logger))
	capmeshv1.RegisterCaptureServiceServer(grpcServer, grpcserver.NewCaptureServer(sessions, packetService, logger))

	metricsServer := &http.Server{Addr: *metricsAddress, Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("metrics server listening", "address", *metricsAddress)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
			cancel()
		}
	}()
	go func() {
		logger.Info("gRPC server listening", "address", *listenAddress)
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("gRPC server failed", "error", err)
			cancel()
		}
	}()
	<-ctx.Done()
	logger.Info("server shutting down")
	graceful := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(graceful)
	}()
	select {
	case <-graceful:
	case <-time.After(5 * time.Second):
		logger.Warn("forcing gRPC shutdown after grace period")
		grpcServer.Stop()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = metricsServer.Shutdown(shutdownCtx)
}

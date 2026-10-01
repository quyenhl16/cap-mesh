package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
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
	"github.com/quyenhl16/cap-mesh/internal/adapter/envconfig"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcserver"
	"github.com/quyenhl16/cap-mesh/internal/adapter/kubernetes"
	"github.com/quyenhl16/cap-mesh/internal/adapter/memory"
	metricadapter "github.com/quyenhl16/cap-mesh/internal/adapter/metrics"
	"github.com/quyenhl16/cap-mesh/internal/adapter/recording"
	appcontinuous "github.com/quyenhl16/cap-mesh/internal/application/continuous"
	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	appstream "github.com/quyenhl16/cap-mesh/internal/application/stream"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
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
	recordDirectory := flag.String("record-dir", "", "directory for server-side PCAPNG recordings; empty disables recording")
	recordSegmentSize := flag.String("record-segment-size", "100MiB", "maximum size of each PCAPNG segment; 0 disables rotation")
	recordMaxSessionSize := flag.String("record-max-session-size", "10GiB", "maximum total recording size per session; 0 means unlimited")
	recordQueueSize := flag.Int("record-queue-size", 65536, "packet queue size for each session recorder")
	workloadReconcileInterval := flag.Duration("workload-reconcile-interval", 5*time.Second, "interval for reconciling workload pods and Calico endpoints")
	if err := envconfig.Apply(flag.CommandLine, map[string]string{
		"CAPMESH_SERVER_LISTEN":               "listen",
		"CAPMESH_SERVER_METRICS_LISTEN":       "metrics-listen",
		"CAPMESH_TLS_CERT":                    "tls-cert",
		"CAPMESH_TLS_KEY":                     "tls-key",
		"CAPMESH_SUBSCRIBER_QUEUE_SIZE":       "subscriber-queue-size",
		"CAPMESH_RECORD_DIR":                  "record-dir",
		"CAPMESH_RECORD_SEGMENT_SIZE":         "record-segment-size",
		"CAPMESH_RECORD_MAX_SESSION_SIZE":     "record-max-session-size",
		"CAPMESH_RECORD_QUEUE_SIZE":           "record-queue-size",
		"CAPMESH_WORKLOAD_RECONCILE_INTERVAL": "workload-reconcile-interval",
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	var segmentSize, maxSessionSize int64
	var err error
	if *recordDirectory != "" {
		segmentSize, err = recording.ParseSize(*recordSegmentSize)
		if err != nil {
			logger.Error("invalid --record-segment-size", "error", err)
			os.Exit(2)
		}
		maxSessionSize, err = recording.ParseSize(*recordMaxSessionSize)
		if err != nil {
			logger.Error("invalid --record-max-session-size", "error", err)
			os.Exit(2)
		}
		if *recordQueueSize < 1 {
			logger.Error("--record-queue-size must be positive")
			os.Exit(2)
		}
	}
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
	var captureRecorder ports.CaptureRecorder
	var recorderManager *recording.Manager
	if *recordDirectory != "" {
		recordingMetrics := recording.NewMetrics(registry)
		recorderManager, err = recording.NewManager(recording.Config{Directory: *recordDirectory, SegmentSize: segmentSize, MaxSessionSize: maxSessionSize, QueueSize: *recordQueueSize, FlushInterval: time.Second, SyncInterval: 10 * time.Second}, packetService, logger, recordingMetrics)
		if err != nil {
			logger.Error("initialize capture recording failed", "error", err)
			os.Exit(1)
		}
		captureRecorder = recorderManager
		logger.Info("server-side capture recording enabled", "directory", *recordDirectory, "segment_size", segmentSize, "max_session_size", maxSessionSize, "queue_size", *recordQueueSize)
	}
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, packetService, captureRecorder, *subscriberQueue)
	if *workloadReconcileInterval <= 0 {
		logger.Error("--workload-reconcile-interval must be positive")
		os.Exit(2)
	}
	sessions.SetWorkloadResolver(kubernetes.NewWorkloadResolver(), *workloadReconcileInterval)
	continuousCapture := appcontinuous.NewService(sessions, recorderManager, recorderManager != nil)
	if recorderManager != nil {
		recorderManager.OnComplete(continuousCapture.RecordingCompleted)
	}
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
	agentServer := grpcserver.NewAgentServer(agents, packetService, logger)
	agentServer.OnCaptureStatus(sessions.CaptureStatus)
	capmeshv1.RegisterAgentServiceServer(grpcServer, agentServer)
	captureServer := grpcserver.NewCaptureServer(sessions, packetService, logger)
	captureServer.SetContinuousCapture(continuousCapture)
	captureServer.SetAgentRegistry(agents)
	capmeshv1.RegisterCaptureServiceServer(grpcServer, captureServer)

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
	if recorderManager != nil {
		recordingCtx, recordingCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer recordingCancel()
		if err := recorderManager.Shutdown(recordingCtx); err != nil {
			logger.Error("capture recording shutdown failed", "error", err)
		}
	}
}

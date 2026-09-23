package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/adapter/capture"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcclient"
	"github.com/quyenhl16/capmesh/internal/adapter/kubernetes"
	metricadapter "github.com/quyenhl16/capmesh/internal/adapter/metrics"
	appagent "github.com/quyenhl16/capmesh/internal/application/agent"
)

func main() {
	server := flag.String("server", "127.0.0.1:8443", "capmesh-server address")
	node := flag.String("node", defaultNodeName(), "Kubernetes node name")
	interfaceA := flag.String("interface-a", "", "physical interface mapped to A")
	interfaceB := flag.String("interface-b", "", "physical interface mapped to B")
	interfaceC := flag.String("interface-c", "", "physical interface mapped to C")
	dumpcap := flag.String("dumpcap", "dumpcap", "dumpcap executable path")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	insecureTransport := flag.Bool("insecure", false, "disable TLS (local development only)")
	caFile := flag.String("tls-ca", "", "server CA certificate")
	serverName := flag.String("tls-server-name", "", "TLS server name override")
	metricsAddress := flag.String("metrics-listen", ":9091", "Prometheus metrics listen address")
	interfacesFromKubernetes := flag.Bool("interfaces-from-kubernetes", false, "read interface mappings from Node annotations")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	interfaces := cleanInterfaces(map[string]string{"A": *interfaceA, "B": *interfaceB, "C": *interfaceC})
	if *interfacesFromKubernetes {
		annotated, err := kubernetes.InterfaceAnnotations(ctx, *node)
		if err != nil {
			logger.Error("load node interface annotations failed", "error", err)
			os.Exit(1)
		}
		for logical, physical := range annotated {
			if interfaces[logical] == "" {
				interfaces[logical] = physical
			}
		}
	}
	if len(interfaces) == 0 {
		logger.Error("at least one interface mapping is required")
		os.Exit(2)
	}

	reporter := grpcclient.NewReporter(256)
	registry := prometheus.NewRegistry()
	agentMetrics := metricadapter.NewAgent(registry, reporter)
	engine := metricadapter.NewCaptureEngine(capture.Dumpcap{Binary: *dumpcap, Logger: logger}, agentMetrics)
	service := appagent.NewService(*node, interfaces, engine, agentMetrics, 64, 10*time.Millisecond)
	metricsServer := &http.Server{Addr: *metricsAddress, Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
			cancel()
		}
	}()

	config := grpcclient.DialConfig{Address: *server, Insecure: *insecureTransport, CAFile: *caFile, ServerName: *serverName, Token: *token}
	for ctx.Err() == nil {
		connection, err := grpcclient.Dial(config)
		if err == nil {
			transport := grpcclient.NewAgentTransport(connection, reporter, *token)
			err = transport.Connect(ctx, *node, interfaces, func(command *capmeshv1.AgentCommand) error {
				if start := command.GetStart(); start != nil {
					return service.Start(ctx, appagent.StartRequest{SessionID: start.GetSessionId(), LogicalInterface: start.GetLogicalInterface(), Filter: start.GetFilter(), Snaplen: start.GetSnaplen()})
				}
				if stop := command.GetStop(); stop != nil {
					return service.Stop(ctx, stop.GetSessionId())
				}
				return nil
			})
			_ = connection.Close()
		}
		service.StopAll()
		if ctx.Err() != nil {
			break
		}
		logger.Warn("agent connection lost; retrying", "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = metricsServer.Shutdown(shutdownCtx)
}

func defaultNodeName() string {
	if value := os.Getenv("NODE_NAME"); value != "" {
		return value
	}
	value, _ := os.Hostname()
	return value
}

func cleanInterfaces(input map[string]string) map[string]string {
	out := make(map[string]string)
	for logical, physical := range input {
		if value := strings.TrimSpace(physical); value != "" {
			out[logical] = value
		}
	}
	return out
}

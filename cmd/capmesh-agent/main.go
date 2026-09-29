package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/capture"
	"github.com/quyenhl16/cap-mesh/internal/adapter/envconfig"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcclient"
	"github.com/quyenhl16/cap-mesh/internal/adapter/kubernetes"
	logadapter "github.com/quyenhl16/cap-mesh/internal/adapter/logging"
	metricadapter "github.com/quyenhl16/cap-mesh/internal/adapter/metrics"
	appagent "github.com/quyenhl16/cap-mesh/internal/application/agent"
	"github.com/quyenhl16/cap-mesh/internal/core/interfacealias"
)

type interfaceMappings map[string]string

func (m *interfaceMappings) String() string {
	if m == nil || len(*m) == 0 {
		return ""
	}
	values := make([]string, 0, len(*m))
	for logical, physical := range *m {
		values = append(values, logical+"="+physical)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}

func (m *interfaceMappings) Set(value string) error {
	logical, physical, ok := strings.Cut(value, "=")
	if !ok {
		return fmt.Errorf("interface mapping %q must use alias=physical format", value)
	}
	logical, err := interfacealias.Normalize(logical)
	if err != nil {
		return err
	}
	physical = strings.TrimSpace(physical)
	if physical == "" {
		return fmt.Errorf("physical interface for alias %q is empty", logical)
	}
	if *m == nil {
		*m = make(interfaceMappings)
	}
	if _, exists := (*m)[logical]; !exists && len(*m) >= interfacealias.MaxMappings {
		return fmt.Errorf("too many interface mappings: maximum is %d", interfacealias.MaxMappings)
	}
	(*m)[logical] = physical
	return nil
}

func main() {
	customInterfaces := make(interfaceMappings)
	server := flag.String("server", "127.0.0.1:18443", "capmesh-server address")
	node := flag.String("node", defaultNodeName(), "Kubernetes node name")
	flag.Var(&customInterfaces, "interface", "interface mapping alias=physical; repeat for up to 10 interfaces")
	interfaceA := flag.String("interface-a", "", "physical interface mapped to A")
	interfaceB := flag.String("interface-b", "", "physical interface mapped to B")
	interfaceC := flag.String("interface-c", "", "physical interface mapped to C")
	dumpcap := flag.String("dumpcap", "dumpcap", "dumpcap executable path")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	insecureTransport := flag.Bool("insecure", false, "disable TLS (local development only)")
	caFile := flag.String("tls-ca", "", "server CA certificate")
	serverName := flag.String("tls-server-name", "", "TLS server name override")
	metricsAddress := flag.String("metrics-listen", ":9091", "Prometheus metrics listen address")
	captureLogInterval := flag.Duration("capture-log-interval", 10*time.Second, "interval for per-interface packet count logs; 0 disables periodic logs")
	interfacesFromKubernetes := flag.Bool("interfaces-from-kubernetes", false, "read interface mappings from Node annotations")
	if err := envconfig.Apply(flag.CommandLine, map[string]string{
		"CAPMESH_SERVER":                     "server",
		"CAPMESH_INTERFACE_A":                "interface-a",
		"CAPMESH_INTERFACE_B":                "interface-b",
		"CAPMESH_INTERFACE_C":                "interface-c",
		"CAPMESH_DUMPCAP":                    "dumpcap",
		"CAPMESH_INSECURE":                   "insecure",
		"CAPMESH_TLS_CA":                     "tls-ca",
		"CAPMESH_TLS_SERVER_NAME":            "tls-server-name",
		"CAPMESH_AGENT_METRICS_LISTEN":       "metrics-listen",
		"CAPMESH_CAPTURE_LOG_INTERVAL":       "capture-log-interval",
		"CAPMESH_INTERFACES_FROM_KUBERNETES": "interfaces-from-kubernetes",
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := envconfig.ApplyList(flag.CommandLine, "CAPMESH_INTERFACES", "interface"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	interfaces, err := cleanInterfaces(map[string]string{"A": *interfaceA, "B": *interfaceB, "C": *interfaceC})
	if err != nil {
		logger.Error("invalid interface mapping", "error", err)
		os.Exit(2)
	}
	for logical, physical := range customInterfaces {
		interfaces[logical] = physical
	}
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
	if len(interfaces) > interfacealias.MaxMappings {
		logger.Error("too many interface mappings", "count", len(interfaces), "maximum", interfacealias.MaxMappings)
		os.Exit(2)
	}
	if len(interfaces) == 0 {
		logger.Error("at least one interface mapping is required")
		os.Exit(2)
	}
	if *captureLogInterval < 0 {
		logger.Error("--capture-log-interval must not be negative")
		os.Exit(2)
	}

	reporter := grpcclient.NewReporter(256)
	registry := prometheus.NewRegistry()
	agentMetrics := metricadapter.NewAgent(registry, reporter)
	engine := metricadapter.NewCaptureEngine(capture.Dumpcap{Binary: *dumpcap, Logger: logger}, agentMetrics)
	progressLogger := logadapter.NewCaptureProgress(logger)
	service := appagent.NewService(*node, interfaces, engine, agentMetrics, progressLogger, 64, 10*time.Millisecond, *captureLogInterval)
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

func cleanInterfaces(input map[string]string) (map[string]string, error) {
	out := make(map[string]string)
	for logical, physical := range input {
		if value := strings.TrimSpace(physical); value != "" {
			canonical, err := interfacealias.Normalize(logical)
			if err != nil {
				return nil, err
			}
			out[canonical] = value
		}
	}
	if len(out) > interfacealias.MaxMappings {
		return nil, fmt.Errorf("too many interface mappings: got %d, maximum is %d", len(out), interfacealias.MaxMappings)
	}
	return out, nil
}

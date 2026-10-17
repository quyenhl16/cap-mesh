package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/benchmark"
)

var (
	version   = "dev"
	gitCommit = "unknown"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printUsage(stdout)
		return nil
	}
	switch args[0] {
	case "receiver":
		return runReceiver(ctx, args[1:], stderr)
	case "run":
		return runBenchmark(ctx, args[1:], stdout, stderr)
	case "compare":
		return runCompare(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "capmesh-benchmark %s (%s)\n", version, gitCommit)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runReceiver(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("receiver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", env("CAPMESH_BENCHMARK_LISTEN", ":9000"), "UDP listen address")
	readBuffer := flags.Int("read-buffer", envInt("CAPMESH_BENCHMARK_READ_BUFFER", 4*1024*1024), "UDP socket read buffer")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected receiver arguments: %s", strings.Join(flags.Args(), " "))
	}
	fmt.Fprintf(stderr, "benchmark receiver listening on %s\n", *listen)
	return benchmark.Receive(ctx, *listen, *readBuffer)
}

func runBenchmark(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	scenarioPath := flags.String("scenario", "", "YAML scenario file")
	server := flags.String("server", env("CAPMESH_SERVER", "capmesh-server:18443"), "CapMesh server address")
	receiver := flags.String("receiver", env("CAPMESH_BENCHMARK_RECEIVER", "capmesh-benchmark-receiver:9000"), "UDP receiver address")
	targetNamespace := flags.String("target-namespace", env("CAPMESH_BENCHMARK_NAMESPACE", "capmesh"), "receiver workload namespace")
	targetKind := flags.String("target-kind", env("CAPMESH_BENCHMARK_TARGET_KIND", "deployment"), "receiver workload kind")
	targetName := flags.String("target-name", env("CAPMESH_BENCHMARK_TARGET_NAME", "capmesh-benchmark-receiver"), "receiver workload name")
	packetSize := flags.Int("packet-size", 512, "Ethernet packet size in bytes, excluding FCS")
	packetRate := flags.Int("packet-rate", 10000, "requested packets per second")
	duration := flags.Duration("duration", time.Minute, "measurement duration")
	warmup := flags.Duration("warmup", 10*time.Second, "warmup duration")
	settle := flags.Duration("settle", 2*time.Second, "delay after subscribers become ready")
	drain := flags.Duration("drain", 2*time.Second, "delay for in-flight packets")
	subscribers := flags.Int("subscribers", 1, "number of packet stream subscribers")
	snaplen := flags.Uint("snaplen", 65535, "capture snapshot length")
	reorderWindow := flags.Duration("reorder-window", 300*time.Millisecond, "server reorder window")
	maxLossPercent := flags.Float64("max-loss-percent", 0.1, "maximum allowed packet loss percentage")
	maxP99Latency := flags.Duration("max-p99-latency", 0, "maximum allowed p99 latency; zero disables the check")
	output := flags.String("output", "", "JSON result path")
	token := flags.String("token", env("CAPMESH_TOKEN", ""), "bearer token")
	tokenFile := flags.String("token-file", env("CAPMESH_TOKEN_FILE", ""), "file containing bearer token")
	insecure := flags.Bool("insecure", envBool("CAPMESH_INSECURE", false), "disable TLS")
	caFile := flags.String("tls-ca", env("CAPMESH_TLS_CA", ""), "server CA certificate")
	tlsServerName := flags.String("tls-server-name", env("CAPMESH_TLS_SERVER_NAME", ""), "TLS server name")
	versionLabel := flags.String("version-label", version, "version recorded in the result")
	commitLabel := flags.String("git-commit", gitCommit, "Git commit recorded in the result")
	runnerNode := flags.String("runner-node", env("CAPMESH_BENCHMARK_NODE_NAME", ""), "Kubernetes node running the benchmark")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %s", strings.Join(flags.Args(), " "))
	}
	visited := make(map[string]bool)
	flags.Visit(func(item *flag.Flag) { visited[item.Name] = true })
	scenarioName := "command-line"
	if *scenarioPath != "" {
		scenario, err := benchmark.LoadScenario(*scenarioPath)
		if err != nil {
			return err
		}
		scenarioName = scenario.Name
		applyScenario(scenario, visited, receiver, targetNamespace, targetKind, targetName, packetSize, packetRate, duration, warmup, settle, drain, subscribers, snaplen, reorderWindow, maxLossPercent, maxP99Latency)
	}
	if *token == "" && *tokenFile != "" {
		data, err := os.ReadFile(*tokenFile)
		if err != nil {
			return fmt.Errorf("read token file: %w", err)
		}
		*token = strings.TrimSpace(string(data))
	}
	result, err := benchmark.Run(ctx, benchmark.RunConfig{
		Server: *server, Receiver: *receiver, TargetNamespace: *targetNamespace,
		TargetKind: strings.ToLower(*targetKind), TargetName: *targetName,
		PacketSize: *packetSize, PacketRate: *packetRate, Duration: *duration,
		Warmup: *warmup, Settle: *settle, Drain: *drain, Subscribers: *subscribers,
		Snaplen: uint32(*snaplen), ReorderWindow: *reorderWindow,
		MaxLossPercent: *maxLossPercent, MaxP99Latency: *maxP99Latency,
		Insecure: *insecure, CAFile: *caFile, TLSServerName: *tlsServerName, Token: *token,
		Version: *versionLabel, GitCommit: *commitLabel, RunnerNode: *runnerNode, Scenario: scenarioName,
	})
	if err != nil {
		return err
	}
	benchmark.PrintResult(stdout, result)
	if *output != "" {
		if err := benchmark.WriteResult(*output, result); err != nil {
			return err
		}
		fmt.Fprintf(stderr, "result written to %s\n", *output)
	}
	if result.Verdict != "PASS" {
		return errors.New(result.FailureReason)
	}
	return nil
}

func applyScenario(s benchmark.Scenario, visited map[string]bool, receiver, namespace, kind, name *string, packetSize, packetRate *int, duration, warmup, settle, drain *time.Duration, subscribers *int, snaplen *uint, reorder *time.Duration, maxLoss *float64, maxP99 *time.Duration) {
	if !visited["receiver"] && s.Receiver != "" {
		*receiver = s.Receiver
	}
	if !visited["target-namespace"] && s.TargetNamespace != "" {
		*namespace = s.TargetNamespace
	}
	if !visited["target-kind"] && s.TargetKind != "" {
		*kind = s.TargetKind
	}
	if !visited["target-name"] && s.TargetName != "" {
		*name = s.TargetName
	}
	if !visited["packet-size"] && s.PacketSize != nil {
		*packetSize = *s.PacketSize
	}
	if !visited["packet-rate"] && s.PacketRate != nil {
		*packetRate = *s.PacketRate
	}
	if !visited["duration"] && s.Duration != nil {
		*duration = time.Duration(*s.Duration)
	}
	if !visited["warmup"] && s.Warmup != nil {
		*warmup = time.Duration(*s.Warmup)
	}
	if !visited["settle"] && s.Settle != nil {
		*settle = time.Duration(*s.Settle)
	}
	if !visited["drain"] && s.Drain != nil {
		*drain = time.Duration(*s.Drain)
	}
	if !visited["subscribers"] && s.Subscribers != nil {
		*subscribers = *s.Subscribers
	}
	if !visited["snaplen"] && s.Snaplen != nil {
		*snaplen = uint(*s.Snaplen)
	}
	if !visited["reorder-window"] && s.ReorderWindow != nil {
		*reorder = time.Duration(*s.ReorderWindow)
	}
	if !visited["max-loss-percent"] && s.MaxLossPercent != nil {
		*maxLoss = *s.MaxLossPercent
	}
	if !visited["max-p99-latency"] && s.MaxP99Latency != nil {
		*maxP99 = time.Duration(*s.MaxP99Latency)
	}
}

func runCompare(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	basePath := flags.String("base", "", "baseline JSON result")
	candidatePath := flags.String("candidate", "", "candidate JSON result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *basePath == "" || *candidatePath == "" {
		return errors.New("--base and --candidate are required")
	}
	base, err := benchmark.ReadResult(*basePath)
	if err != nil {
		return err
	}
	candidate, err := benchmark.ReadResult(*candidatePath)
	if err != nil {
		return err
	}
	if err := benchmark.ValidateComparable(base, candidate); err != nil {
		return err
	}
	benchmark.PrintComparison(stdout, base, candidate)
	return nil
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, `Usage:
  capmesh-benchmark receiver [flags]
  capmesh-benchmark run [flags]
  capmesh-benchmark compare --base FILE --candidate FILE
  capmesh-benchmark version`)
}

func env(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

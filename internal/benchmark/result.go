package benchmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

const ResultSchemaVersion = 1

type Environment struct {
	Hostname     string   `json:"hostname"`
	RunnerNode   string   `json:"runnerNode,omitempty"`
	CaptureNodes []string `json:"captureNodes,omitempty"`
	GOOS         string   `json:"goos"`
	GOARCH       string   `json:"goarch"`
	GoVersion    string   `json:"goVersion"`
	CPUs         int      `json:"cpus"`
	GOMAXPROCS   int      `json:"gomaxprocs"`
}

type ResultConfig struct {
	Scenario        string        `json:"scenario"`
	Server          string        `json:"server"`
	Receiver        string        `json:"receiver"`
	TargetNamespace string        `json:"targetNamespace"`
	TargetKind      string        `json:"targetKind"`
	TargetName      string        `json:"targetName"`
	PacketSize      int           `json:"packetSizeBytes"`
	PacketRate      int           `json:"requestedPacketsPerSecond"`
	Duration        time.Duration `json:"duration"`
	Warmup          time.Duration `json:"warmup"`
	Settle          time.Duration `json:"settle"`
	Drain           time.Duration `json:"drain"`
	Subscribers     int           `json:"subscribers"`
	Snaplen         uint32        `json:"snaplen"`
	ReorderWindow   time.Duration `json:"reorderWindow"`
	MaxLossPercent  float64       `json:"maxLossPercent"`
	MaxP99Latency   time.Duration `json:"maxP99Latency"`
}

type ResultMetrics struct {
	PacketsSent            uint64        `json:"packetsSent"`
	PacketsCaptured        uint64        `json:"packetsCaptured"`
	PacketsLost            uint64        `json:"packetsLost"`
	LossPercent            float64       `json:"lossPercent"`
	SubscriberPackets      []uint64      `json:"subscriberPackets"`
	WorstSubscriberLoss    float64       `json:"worstSubscriberLossPercent"`
	Duplicates             uint64        `json:"duplicates"`
	OutOfOrder             uint64        `json:"outOfOrder"`
	ActualPacketsPerSecond float64       `json:"actualPacketsPerSecond"`
	CapturedPacketsPerSec  float64       `json:"capturedPacketsPerSecond"`
	DataRateBitsPerSecond  float64       `json:"dataRateBitsPerSecond"`
	LatencySamples         uint64        `json:"latencySamples"`
	LatencyP50             time.Duration `json:"latencyP50"`
	LatencyP95             time.Duration `json:"latencyP95"`
	LatencyP99             time.Duration `json:"latencyP99"`
	LatencyMax             time.Duration `json:"latencyMax"`
	GeneratorSaturated     bool          `json:"generatorSaturated"`
}

type Result struct {
	SchemaVersion int           `json:"schemaVersion"`
	RunID         string        `json:"runId"`
	Version       string        `json:"version"`
	GitCommit     string        `json:"gitCommit"`
	StartedAt     time.Time     `json:"startedAt"`
	FinishedAt    time.Time     `json:"finishedAt"`
	Environment   Environment   `json:"environment"`
	Config        ResultConfig  `json:"config"`
	Metrics       ResultMetrics `json:"metrics"`
	Verdict       string        `json:"verdict"`
	FailureReason string        `json:"failureReason,omitempty"`
}

func CurrentEnvironment() Environment {
	hostname, _ := os.Hostname()
	return Environment{
		Hostname:   hostname,
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		GoVersion:  runtime.Version(),
		CPUs:       runtime.NumCPU(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
}

func WriteResult(path string, result Result) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write result: %w", err)
	}
	return nil
}

func ReadResult(path string) (Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{}, fmt.Errorf("read result: %w", err)
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		return Result{}, fmt.Errorf("decode result: %w", err)
	}
	if result.SchemaVersion != ResultSchemaVersion {
		return Result{}, fmt.Errorf("unsupported result schema version %d", result.SchemaVersion)
	}
	return result, nil
}

func PrintResult(output io.Writer, result Result) {
	fmt.Fprintln(output, "CapMesh Benchmark Result")
	fmt.Fprintln(output, "------------------------------------------------------------")
	printRow(output, "Version", result.Version)
	printRow(output, "Git commit", result.GitCommit)
	printRow(output, "Run ID", result.RunID)
	printRow(output, "Scenario", result.Config.Scenario)
	if result.Environment.RunnerNode != "" {
		printRow(output, "Runner node", result.Environment.RunnerNode)
	}
	if len(result.Environment.CaptureNodes) > 0 {
		printRow(output, "Capture nodes", fmt.Sprint(result.Environment.CaptureNodes))
	}
	printRow(output, "Duration", result.Config.Duration.String())
	printRow(output, "Packet size", fmt.Sprintf("%d bytes", result.Config.PacketSize))
	printRow(output, "Requested rate", formatRate(float64(result.Config.PacketRate)))
	printRow(output, "Actual send rate", formatRate(result.Metrics.ActualPacketsPerSecond))
	fmt.Fprintln(output)
	printRow(output, "Packets sent", formatUint(result.Metrics.PacketsSent))
	printRow(output, "Packets captured", formatUint(result.Metrics.PacketsCaptured))
	printRow(output, "Packets lost", fmt.Sprintf("%s (%.4f%%)", formatUint(result.Metrics.PacketsLost), result.Metrics.LossPercent))
	printRow(output, "Worst subscriber loss", fmt.Sprintf("%.4f%%", result.Metrics.WorstSubscriberLoss))
	printRow(output, "Duplicates", formatUint(result.Metrics.Duplicates))
	printRow(output, "Out of order", formatUint(result.Metrics.OutOfOrder))
	fmt.Fprintln(output)
	printRow(output, "Captured throughput", formatRate(result.Metrics.CapturedPacketsPerSec))
	printRow(output, "Captured data rate", formatBitRate(result.Metrics.DataRateBitsPerSecond))
	printRow(output, "Latency p50", result.Metrics.LatencyP50.String())
	printRow(output, "Latency p95", result.Metrics.LatencyP95.String())
	printRow(output, "Latency p99", result.Metrics.LatencyP99.String())
	printRow(output, "Latency max", result.Metrics.LatencyMax.String())
	if result.Metrics.GeneratorSaturated {
		printRow(output, "Generator", "SATURATED")
	}
	fmt.Fprintln(output)
	printRow(output, "Result", result.Verdict)
	if result.FailureReason != "" {
		printRow(output, "Reason", result.FailureReason)
	}
	fmt.Fprintln(output, "------------------------------------------------------------")
}

func PrintComparison(output io.Writer, base, candidate Result) {
	fmt.Fprintf(output, "CapMesh Benchmark Comparison: %s -> %s\n", label(base), label(candidate))
	fmt.Fprintln(output, "--------------------------------------------------------------------------------")
	fmt.Fprintf(output, "%-24s %16s %16s %16s\n", "Metric", "Baseline", "Candidate", "Improvement")
	fmt.Fprintln(output, "--------------------------------------------------------------------------------")
	comparisonRow(output, "Captured throughput", base.Metrics.CapturedPacketsPerSec, candidate.Metrics.CapturedPacketsPerSec, formatRate, true)
	comparisonRow(output, "Packet loss", base.Metrics.LossPercent, candidate.Metrics.LossPercent, func(value float64) string { return fmt.Sprintf("%.4f%%", value) }, false)
	comparisonRow(output, "Worst subscriber loss", base.Metrics.WorstSubscriberLoss, candidate.Metrics.WorstSubscriberLoss, func(value float64) string { return fmt.Sprintf("%.4f%%", value) }, false)
	comparisonRow(output, "Latency p50", float64(base.Metrics.LatencyP50), float64(candidate.Metrics.LatencyP50), formatDurationFloat, false)
	comparisonRow(output, "Latency p95", float64(base.Metrics.LatencyP95), float64(candidate.Metrics.LatencyP95), formatDurationFloat, false)
	comparisonRow(output, "Latency p99", float64(base.Metrics.LatencyP99), float64(candidate.Metrics.LatencyP99), formatDurationFloat, false)
	fmt.Fprintln(output, "--------------------------------------------------------------------------------")
}

func ValidateComparable(base, candidate Result) error {
	a, b := base.Config, candidate.Config
	if a.PacketSize != b.PacketSize || a.PacketRate != b.PacketRate || a.Duration != b.Duration ||
		a.Warmup != b.Warmup || a.Settle != b.Settle || a.Drain != b.Drain ||
		a.Subscribers != b.Subscribers || a.Snaplen != b.Snaplen || a.ReorderWindow != b.ReorderWindow ||
		a.TargetNamespace != b.TargetNamespace || a.TargetKind != b.TargetKind || a.TargetName != b.TargetName {
		return errors.New("results use different benchmark settings")
	}
	return nil
}

func comparisonRow(output io.Writer, name string, base, candidate float64, format func(float64) string, higherIsBetter bool) {
	difference := 0.0
	if base != 0 {
		difference = (candidate - base) / base * 100
	}
	if !higherIsBetter {
		difference = -difference
	}
	fmt.Fprintf(output, "%-24s %16s %16s %+15.2f%%\n", name, format(base), format(candidate), difference)
}

func label(result Result) string {
	if result.Version != "" && result.Version != "dev" {
		return result.Version
	}
	return result.GitCommit

}

func printRow(output io.Writer, name, value string) {
	fmt.Fprintf(output, "%-24s %s\n", name, value)
}

func formatRate(value float64) string {
	return fmt.Sprintf("%.0f pps", value)
}

func formatBitRate(value float64) string {
	units := []string{"bit/s", "Kbit/s", "Mbit/s", "Gbit/s"}
	unit := 0
	for value >= 1000 && unit < len(units)-1 {
		value /= 1000
		unit++
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

func formatDurationFloat(value float64) string {
	return time.Duration(value).String()
}

func formatUint(value uint64) string {
	return fmt.Sprintf("%d", value)
}

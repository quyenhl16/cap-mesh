package benchmark

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTerminationMessageResultFitsKubernetesLimit(t *testing.T) {
	result := Result{
		SchemaVersion: ResultSchemaVersion,
		RunID:         "12345678",
		Version:       "0.1.0",
		GitCommit:     "0123456789abcdef",
		Environment: Environment{
			Hostname: "capmesh-benchmark-runner-example", RunnerNode: "worker-example",
			CaptureNodes: []string{"worker-capture"}, GOOS: "linux", GOARCH: "amd64",
			GoVersion: "go1.24.0", CPUs: 8, GOMAXPROCS: 8,
		},
		Config: ResultConfig{
			Scenario: "baseline", Server: "capmesh-server:18443", Receiver: "capmesh-benchmark-receiver:9000",
			TargetNamespace: "capmesh", TargetKind: "deployment", TargetName: "capmesh-benchmark-receiver",
			PacketSize: 512, PacketRate: 10000, Duration: time.Minute, Warmup: 10 * time.Second,
			Subscribers: 10, Snaplen: 65535, ReorderWindow: 300 * time.Millisecond,
		},
		Metrics: ResultMetrics{SubscriberPackets: make([]uint64, 50)},
		Verdict: "PASS",
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 4096 {
		t.Fatalf("result is %d bytes; Kubernetes termination message limit is 4096", len(data))
	}
}

func TestValidateComparableRejectsDifferentScenarios(t *testing.T) {
	base := Result{Config: ResultConfig{PacketSize: 512, PacketRate: 10000, Duration: time.Minute}}
	candidate := base
	candidate.Config.PacketRate = 20000
	if err := ValidateComparable(base, candidate); err == nil {
		t.Fatal("different packet rates were accepted")
	}
}

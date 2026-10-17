package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type fakeEngine struct {
	batches chan domain.CapturedBatch
	errors  chan error
}

type captureCall struct {
	interfaceName string
	filter        string
}

type multiEngine struct {
	mu    sync.Mutex
	calls []captureCall
}

func (e *multiEngine) Capture(_ context.Context, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	e.mu.Lock()
	e.calls = append(e.calls, captureCall{interfaceName: request.InterfaceName, filter: request.Filter})
	e.mu.Unlock()
	return make(chan domain.CapturedBatch), make(chan error), nil
}

func (f *fakeEngine) Capture(context.Context, ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	return f.batches, f.errors, nil
}

type fakeReporter struct {
	mu      sync.Mutex
	batches []domain.PacketBatch
	status  []string
}

type fakeProgressReporter struct {
	mu       sync.Mutex
	progress []ports.CaptureProgress
}

func (f *fakeProgressReporter) ReportCaptureProgress(progress ports.CaptureProgress) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress = append(f.progress, progress)
}

func (f *fakeReporter) SendBatch(_ context.Context, batch domain.PacketBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, batch)
	return nil
}

func (f *fakeReporter) SendStatus(_ context.Context, _, _ string, state, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = append(f.status, state)
	return nil
}

func TestForwardsCapturedBatch(t *testing.T) {
	engine := &fakeEngine{batches: make(chan domain.CapturedBatch, 1), errors: make(chan error)}
	reporter := &fakeReporter{}
	service := NewService("worker-1", map[string]string{"A": "eth0"}, engine, reporter, nil, 2, time.Hour, 0)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.batches <- domain.CapturedBatch{Packets: []domain.Packet{{SequenceNumber: 1}, {SequenceNumber: 2}}, StartedAt: time.Now(), FlushReason: "packets"}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reporter.mu.Lock()
		count := len(reporter.batches)
		reporter.mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.batches) != 1 || len(reporter.batches[0].Packets) != 2 {
		t.Fatalf("unexpected batches: %#v", reporter.batches)
	}
	service.StopAll()
}

func TestStartNormalizesCustomInterfaceAlias(t *testing.T) {
	engine := &fakeEngine{batches: make(chan domain.CapturedBatch), errors: make(chan error)}
	service := NewService("worker-1", map[string]string{"data-east": "ens192"}, engine, &fakeReporter{}, nil, 64, time.Hour, 0)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "DATA-East", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	service.StopAll()
}

func TestStartsMultipleSourcesInOneSession(t *testing.T) {
	engine := &multiEngine{}
	service := NewService("worker-1", map[string]string{"uplink": "eth0", "data": "eth1"}, engine, &fakeReporter{}, nil, 64, time.Hour, 0)
	for _, source := range []domain.CaptureSource{
		{ID: "source-uplink", TargetType: "interface", LogicalInterface: "uplink"},
		{ID: "source-pod", TargetType: "workload", InterfaceName: "cali123", PodIP: "10.0.0.10"},
	} {
		if err := service.Start(context.Background(), StartRequest{SessionID: "s1", Source: source, Snaplen: 256}); err != nil {
			t.Fatal(err)
		}
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if len(engine.calls) != 2 || engine.calls[0].interfaceName != "eth0" || engine.calls[1].interfaceName != "cali123" {
		t.Fatalf("unexpected capture calls: %#v", engine.calls)
	}
	if len(service.captures["s1"]) != 2 {
		t.Fatalf("active sources = %d, want 2", len(service.captures["s1"]))
	}
	service.StopAll()
}

func TestForwardsTimeoutBatch(t *testing.T) {
	engine := &fakeEngine{batches: make(chan domain.CapturedBatch, 1), errors: make(chan error)}
	reporter := &fakeReporter{}
	service := NewService("worker-1", map[string]string{"A": "eth0"}, engine, reporter, nil, 64, 5*time.Millisecond, 0)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.batches <- domain.CapturedBatch{Packets: []domain.Packet{{SequenceNumber: 1}}, StartedAt: time.Now().Add(-5 * time.Millisecond), FlushReason: "timeout"}
	time.Sleep(30 * time.Millisecond)
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.batches) != 1 || len(reporter.batches[0].Packets) != 1 {
		t.Fatalf("unexpected batches: %#v", reporter.batches)
	}
	service.StopAll()
}

func TestReportsPacketCountPerInterface(t *testing.T) {
	engine := &fakeEngine{batches: make(chan domain.CapturedBatch, 1), errors: make(chan error)}
	reporter := &fakeReporter{}
	progress := &fakeProgressReporter{}
	service := NewService("worker-1", map[string]string{"A": "bond1"}, engine, reporter, progress, 64, time.Hour, 5*time.Millisecond)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.batches <- domain.CapturedBatch{Packets: []domain.Packet{{CapturedLength: 40, SequenceNumber: 1}, {CapturedLength: 60, SequenceNumber: 2}}, CapturedBytes: 100, StartedAt: time.Now(), FlushReason: "capture_end"}
	close(engine.batches)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		progress.mu.Lock()
		count := len(progress.progress)
		progress.mu.Unlock()
		if count > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	progress.mu.Lock()
	defer progress.mu.Unlock()
	if len(progress.progress) == 0 {
		t.Fatal("expected final capture progress")
	}
	last := progress.progress[len(progress.progress)-1]
	if !last.Final || last.NodeName != "worker-1" || last.InterfaceName != "bond1" || last.PacketsTotal != 2 || last.BytesTotal != 100 {
		t.Fatalf("unexpected progress: %#v", last)
	}
}

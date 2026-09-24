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
	packets chan domain.Packet
	errors  chan error
}

func (f *fakeEngine) Capture(context.Context, string, string, uint32) (<-chan domain.Packet, <-chan error, error) {
	return f.packets, f.errors, nil
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

func (f *fakeReporter) SendStatus(_ context.Context, _ string, state, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = append(f.status, state)
	return nil
}

func TestBatchFlushesAtMaximumPacketCount(t *testing.T) {
	engine := &fakeEngine{packets: make(chan domain.Packet, 3), errors: make(chan error)}
	reporter := &fakeReporter{}
	service := NewService("worker-1", map[string]string{"A": "eth0"}, engine, reporter, nil, 2, time.Hour, 0)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.packets <- domain.Packet{SequenceNumber: 1}
	engine.packets <- domain.Packet{SequenceNumber: 2}
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

func TestBatchFlushesAfterDelay(t *testing.T) {
	engine := &fakeEngine{packets: make(chan domain.Packet, 1), errors: make(chan error)}
	reporter := &fakeReporter{}
	service := NewService("worker-1", map[string]string{"A": "eth0"}, engine, reporter, nil, 64, 5*time.Millisecond, 0)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.packets <- domain.Packet{SequenceNumber: 1}
	time.Sleep(30 * time.Millisecond)
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.batches) != 1 || len(reporter.batches[0].Packets) != 1 {
		t.Fatalf("unexpected batches: %#v", reporter.batches)
	}
	service.StopAll()
}

func TestReportsPacketCountPerInterface(t *testing.T) {
	engine := &fakeEngine{packets: make(chan domain.Packet, 2), errors: make(chan error)}
	reporter := &fakeReporter{}
	progress := &fakeProgressReporter{}
	service := NewService("worker-1", map[string]string{"A": "bond1"}, engine, reporter, progress, 64, time.Hour, 5*time.Millisecond)
	if err := service.Start(context.Background(), StartRequest{SessionID: "s1", LogicalInterface: "A", Snaplen: 256}); err != nil {
		t.Fatal(err)
	}
	engine.packets <- domain.Packet{CapturedLength: 40, SequenceNumber: 1}
	engine.packets <- domain.Packet{CapturedLength: 60, SequenceNumber: 2}
	close(engine.packets)
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

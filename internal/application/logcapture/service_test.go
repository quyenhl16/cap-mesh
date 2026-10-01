package logcapture

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type fakeSource struct{}

func (fakeSource) ResolvePods(_ context.Context, target domain.WorkloadLogTarget) ([]domain.WorkloadPod, error) {
	return []domain.WorkloadPod{{Target: target, Name: "app-0", UID: "uid-1", Containers: []string{"main"}, RestartCount: map[string]int32{"main": 0}}}, nil
}

func (fakeSource) StreamLogs(ctx context.Context, _ domain.PodLogRequest) (io.ReadCloser, error) {
	return &contextReader{ctx: ctx}, nil
}

type contextReader struct{ ctx context.Context }

func (r *contextReader) Read([]byte) (int, error) { <-r.ctx.Done(); return 0, r.ctx.Err() }
func (*contextReader) Close() error               { return nil }

type fakeFactory struct{ recorder *fakeRecorder }

func (f *fakeFactory) Start(domain.LogCaptureRun) (ports.WorkloadLogRecorder, error) {
	f.recorder = &fakeRecorder{}
	return f.recorder, nil
}

type fakeRecorder struct {
	mu     sync.Mutex
	closed bool
}

func (*fakeRecorder) Write(domain.LogRecord) error { return nil }
func (r *fakeRecorder) Close(string, string) error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}
func (*fakeRecorder) Usage() domain.LogRecordingUsage { return domain.LogRecordingUsage{} }

func TestServiceIsGlobalSingletonAndStopsIndependentJob(t *testing.T) {
	factory := &fakeFactory{}
	service := NewService(fakeSource{}, factory, Config{ReconcileInterval: time.Hour, QueueSize: 4, SegmentSize: 10, MaxRetainedSize: 100}, slog.Default(), nil)
	input := StartInput{Targets: []domain.WorkloadLogTarget{{Namespace: "ns", Kind: "statefulset", Name: "app", MaxPods: 10}}}
	started, err := service.Start(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if started.Status != StateRunning || started.ActiveStreams != 1 {
		t.Fatalf("unexpected state: %#v", started)
	}
	if _, err := service.Start(context.Background(), input); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("second start error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopped, err := service.Stop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Status != StateStopped || stopped.ActiveStreams != 0 {
		t.Fatalf("unexpected stopped state: %#v", stopped)
	}
	factory.recorder.mu.Lock()
	closed := factory.recorder.closed
	factory.recorder.mu.Unlock()
	if !closed {
		t.Fatal("recorder was not closed")
	}
}

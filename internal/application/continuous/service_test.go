package continuous

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/adapter/memory"
	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type fakeAgents struct {
	mu       sync.Mutex
	commands []ports.AgentCommand
}

func (a *fakeAgents) ConnectedNodes() []string { return []string{"worker-1"} }
func (a *fakeAgents) Send(_ context.Context, _ string, command ports.AgentCommand) error {
	a.mu.Lock()
	a.commands = append(a.commands, command)
	a.mu.Unlock()
	return nil
}

type fakePackets struct{}

func (fakePackets) OpenSession(string, time.Duration, int) error { return nil }
func (fakePackets) Publish(domain.PacketBatch)                   {}
func (fakePackets) Subscribe(context.Context, string) (<-chan domain.PacketBatch, error) {
	return nil, nil
}
func (fakePackets) SubscribeLossAware(context.Context, string, int) (ports.PacketSubscription, error) {
	return ports.PacketSubscription{}, nil
}
func (fakePackets) CloseSession(string) {}

type fakeRecorder struct {
	mu       sync.Mutex
	sessions []domain.Session
}

func (r *fakeRecorder) Start(session domain.Session) error {
	r.mu.Lock()
	r.sessions = append(r.sessions, session)
	r.mu.Unlock()
	return nil
}

func (r *fakeRecorder) Usage(string) (domain.RecordingUsage, bool) {
	return domain.RecordingUsage{RetainedSize: 1024, SegmentCount: 2}, true
}

func TestSingletonStartStopAndRestart(t *testing.T) {
	agents := &fakeAgents{}
	recorder := &fakeRecorder{}
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, fakePackets{}, recorder, 128)
	service := NewService(sessions, recorder, true)
	input := StartInput{Targets: []domain.CaptureTarget{{ID: "interface-1", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "A"}}}, Snaplen: 256}

	first, err := service.Start(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != StateRunning || first.SessionID == "" || first.RetainedSize != 1024 {
		t.Fatalf("unexpected first capture: %#v", first)
	}
	if _, err := service.Start(context.Background(), input); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("second start error = %v, want already exists", err)
	}
	stopped, err := service.Stop(context.Background())
	if err != nil || stopped.Status != StateStopped || stopped.SessionID != first.SessionID {
		t.Fatalf("stop result=%#v error=%v", stopped, err)
	}
	second, err := service.Start(context.Background(), input)
	if err != nil || second.Status != StateRunning || second.SessionID == first.SessionID {
		t.Fatalf("restart result=%#v error=%v", second, err)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.sessions) != 2 || recorder.sessions[0].Mode != domain.SessionModeContinuous || !recorder.sessions[0].ExpiresAt.IsZero() {
		t.Fatalf("unexpected recorded sessions: %#v", recorder.sessions)
	}
}

func TestUnexpectedRecorderCompletionRequiresStopBeforeRestart(t *testing.T) {
	agents := &fakeAgents{}
	recorder := &fakeRecorder{}
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, fakePackets{}, recorder, 128)
	service := NewService(sessions, recorder, true)
	input := StartInput{Targets: []domain.CaptureTarget{{ID: "interface-1", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "A"}}}, Snaplen: 256}

	capture, err := service.Start(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	service.RecordingCompleted(capture.SessionID, "COMPLETED", "")
	status := service.Get(context.Background())
	if status.Status != StateFailed {
		t.Fatalf("status = %s, want %s", status.Status, StateFailed)
	}
	if _, err := service.Start(context.Background(), input); !errors.Is(err, ports.ErrAlreadyExists) {
		t.Fatalf("restart before stop error = %v, want already exists", err)
	}
	if _, err := service.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Start(context.Background(), input); err != nil {
		t.Fatalf("restart after stop: %v", err)
	}
}

func TestOnlyOneConcurrentStartSucceeds(t *testing.T) {
	agents := &fakeAgents{}
	recorder := &fakeRecorder{}
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, fakePackets{}, recorder, 128)
	service := NewService(sessions, recorder, true)
	input := StartInput{Targets: []domain.CaptureTarget{{ID: "interface-1", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "A"}}}, Snaplen: 256}
	var wait sync.WaitGroup
	var successes int
	var resultMu sync.Mutex
	for range 20 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := service.Start(context.Background(), input); err == nil {
				resultMu.Lock()
				successes++
				resultMu.Unlock()
			}
		}()
	}
	wait.Wait()
	if successes != 1 {
		t.Fatalf("successful starts = %d, want 1", successes)
	}
}

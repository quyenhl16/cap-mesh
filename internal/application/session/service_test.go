package session

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/adapter/memory"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type fakeAgents struct {
	nodes    []string
	commands []ports.AgentCommand
	send     func() error
}

func (f *fakeAgents) ConnectedNodes() []string { return f.nodes }
func (f *fakeAgents) Send(_ context.Context, _ string, command ports.AgentCommand) error {
	if f.send != nil {
		if err := f.send(); err != nil {
			return err
		}
	}
	f.commands = append(f.commands, command)
	return nil
}

type fakeRecorder struct {
	started bool
	err     error
}

func (f *fakeRecorder) Start(domain.Session) error {
	f.started = true
	return f.err
}

type fakePackets struct {
	opened bool
	closed bool
}

type fakeWorkloadResolver struct {
	sources []domain.CaptureSource
	err     error
}

func (r *fakeWorkloadResolver) Resolve(context.Context, domain.WorkloadTarget, string) ([]domain.CaptureSource, error) {
	return append([]domain.CaptureSource(nil), r.sources...), r.err
}

func (f *fakePackets) OpenSession(string, time.Duration, int) error { f.opened = true; return nil }
func (f *fakePackets) Publish(domain.PacketBatch)                   {}
func (f *fakePackets) Subscribe(context.Context, string) (<-chan domain.PacketBatch, error) {
	return nil, nil
}
func (f *fakePackets) SubscribeLossAware(context.Context, string, int) (ports.PacketSubscription, error) {
	return ports.PacketSubscription{}, nil
}
func (f *fakePackets) CloseSession(string) { f.closed = true }

func TestCreateAndStopSession(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1", "worker-2"}}
	packets := &fakePackets{}
	service := NewService(memory.NewSessionRepository(), agents, packets, nil, 100)
	session, err := service.Create(context.Background(), CreateInput{LogicalInterface: "A", Snaplen: 256, TTL: time.Minute, ReorderWindow: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != domain.SessionRunning || len(agents.commands) != 2 || !packets.opened {
		t.Fatalf("unexpected create result: %#v, commands=%d", session, len(agents.commands))
	}
	session, err = service.Stop(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != domain.SessionStopped || !packets.closed {
		t.Fatalf("unexpected stop result: %#v", session)
	}
}

func TestCreateStartsRecorderBeforeAgents(t *testing.T) {
	recorder := &fakeRecorder{}
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	agents.send = func() error {
		if !recorder.started {
			return errors.New("recorder was not started")
		}
		return nil
	}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, recorder, 100)
	if _, err := service.Create(context.Background(), CreateInput{LogicalInterface: "management", Snaplen: 256, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateFailsBeforeStartingAgentsWhenRecorderFails(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	packets := &fakePackets{}
	recorder := &fakeRecorder{err: errors.New("disk unavailable")}
	service := NewService(memory.NewSessionRepository(), agents, packets, recorder, 100)
	if _, err := service.Create(context.Background(), CreateInput{LogicalInterface: "management", Snaplen: 256, TTL: time.Minute}); err == nil {
		t.Fatal("expected recorder failure")
	}
	if len(agents.commands) != 0 {
		t.Fatalf("agent received commands despite recorder failure: %#v", agents.commands)
	}
	if !packets.closed {
		t.Fatal("packet pipeline was not closed after recorder failure")
	}
}

func TestCreateNormalizesCustomInterfaceAlias(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, nil, 100)
	captureSession, err := service.Create(context.Background(), CreateInput{LogicalInterface: " Data-East ", Snaplen: 256, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if captureSession.LogicalInterface != "data-east" {
		t.Fatalf("logical interface = %q, want data-east", captureSession.LogicalInterface)
	}
	if len(agents.commands) != 1 || agents.commands[0].LogicalInterface != "data-east" {
		t.Fatalf("unexpected commands: %#v", agents.commands)
	}
}

func TestCreateStartsMultipleInterfaceTargetsInOneSession(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, nil, 100)
	targets := []domain.CaptureTarget{
		{ID: "uplink", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "UPLINK"}},
		{ID: "data", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "DATA"}},
	}
	captureSession, err := service.Create(context.Background(), CreateInput{Targets: targets, Snaplen: 256, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if captureSession.Status != domain.SessionRunning || len(agents.commands) != 2 {
		t.Fatalf("unexpected session or commands: %#v %#v", captureSession, agents.commands)
	}
	if agents.commands[0].SessionID != agents.commands[1].SessionID || agents.commands[0].SourceID == agents.commands[1].SourceID {
		t.Fatalf("expected distinct sources in one session: %#v", agents.commands)
	}
}

func TestCreateWorkloadTargetBuildsDirectionFilter(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	resolver := &fakeWorkloadResolver{sources: []domain.CaptureSource{{ID: "pod:t:uid", TargetID: "t", TargetType: "workload", NodeName: "worker-1", InterfaceName: "cali123", PodIP: "10.0.0.10"}}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, nil, 100)
	service.SetWorkloadResolver(resolver, time.Hour)
	targets := []domain.CaptureTarget{{ID: "t", Workload: &domain.WorkloadTarget{Namespace: "payment", Kind: "deployment", Name: "api", Direction: "egress", MaxPods: 10}}}
	if _, err := service.Create(context.Background(), CreateInput{Targets: targets, Filter: "tcp port 443", Snaplen: 256, TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if len(agents.commands) != 1 || agents.commands[0].Filter != "(src host 10.0.0.10) and (tcp port 443)" {
		t.Fatalf("unexpected workload command: %#v", agents.commands)
	}
}

func TestReconcileReplacesRestartedWorkloadPod(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	resolver := &fakeWorkloadResolver{sources: []domain.CaptureSource{{ID: "pod:t:old", TargetID: "t", TargetType: "workload", NodeName: "worker-1", InterfaceName: "cali-old", PodUID: "old", PodIP: "10.0.0.10"}}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, nil, 100)
	service.SetWorkloadResolver(resolver, time.Hour)
	targets := []domain.CaptureTarget{{ID: "t", Workload: &domain.WorkloadTarget{Namespace: "payment", Kind: "statefulset", Name: "worker", Direction: "egress", Follow: true, MaxPods: 10}}}
	captureSession, err := service.Create(context.Background(), CreateInput{Targets: targets, Snaplen: 256, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	resolver.sources = []domain.CaptureSource{{ID: "pod:t:new", TargetID: "t", TargetType: "workload", NodeName: "worker-1", InterfaceName: "cali-new", PodUID: "new", PodIP: "10.0.0.11"}}
	service.reconcile(context.Background(), captureSession.ID)
	if len(agents.commands) != 3 || agents.commands[1].Kind != "stop" || agents.commands[1].SourceID != "pod:t:old" || agents.commands[2].Kind != "start" || agents.commands[2].SourceID != "pod:t:new" {
		t.Fatalf("unexpected reconcile commands: %#v", agents.commands)
	}
	_, _ = service.Stop(context.Background(), captureSession.ID)
}

func TestFailedSourceIsRemovedSoReconcileCanRetry(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	source := domain.CaptureSource{ID: "pod:t:uid", TargetID: "t", TargetType: "workload", NodeName: "worker-1", PodIP: "10.0.0.10"}
	resolver := &fakeWorkloadResolver{sources: []domain.CaptureSource{source}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, nil, 100)
	service.SetWorkloadResolver(resolver, time.Hour)
	targets := []domain.CaptureTarget{{ID: "t", Workload: &domain.WorkloadTarget{Namespace: "payment", Kind: "deployment", Name: "api", Direction: "egress", Follow: true, MaxPods: 10}}}
	captureSession, err := service.Create(context.Background(), CreateInput{Targets: targets, Snaplen: 256, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	service.CaptureStatus(context.Background(), "worker-1", captureSession.ID, source.ID, "FAILED", "route unavailable")
	service.reconcile(context.Background(), captureSession.ID)
	if len(agents.commands) != 2 || agents.commands[1].Kind != "start" || agents.commands[1].SourceID != source.ID {
		t.Fatalf("expected failed source to be retried: %#v", agents.commands)
	}
	_, _ = service.Stop(context.Background(), captureSession.ID)
}

func TestSourceDistributionLimitPerNode(t *testing.T) {
	sources := make([]domain.CaptureSource, maxSourcesPerNode+1)
	for index := range sources {
		sources[index] = domain.CaptureSource{ID: fmt.Sprintf("source-%d", index), NodeName: "worker-1"}
	}
	if err := validateSourceDistribution(sources[:maxSourcesPerNode]); err != nil {
		t.Fatalf("limit should be accepted: %v", err)
	}
	if err := validateSourceDistribution(sources); err == nil {
		t.Fatal("expected per-node source limit error")
	}
}

func TestCreateRejectsInvalidInterfaceAlias(t *testing.T) {
	service := NewService(memory.NewSessionRepository(), &fakeAgents{nodes: []string{"worker-1"}}, &fakePackets{}, nil, 100)
	_, err := service.Create(context.Background(), CreateInput{LogicalInterface: "data_plane", Snaplen: 256, TTL: time.Minute})
	if err == nil {
		t.Fatal("expected invalid interface alias to fail")
	}
}

func TestCreateRejectsDisconnectedNode(t *testing.T) {
	service := NewService(memory.NewSessionRepository(), &fakeAgents{nodes: []string{"worker-1"}}, &fakePackets{}, nil, 100)
	_, err := service.Create(context.Background(), CreateInput{Nodes: []string{"worker-2"}, LogicalInterface: "A", Snaplen: 256, TTL: time.Minute, ReorderWindow: time.Millisecond})
	if err == nil {
		t.Fatal("expected unavailable node to fail")
	}
}

func TestSoleAgentDisconnectFailsSession(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	packets := &fakePackets{}
	repository := memory.NewSessionRepository()
	service := NewService(repository, agents, packets, nil, 100)
	captureSession, err := service.Create(context.Background(), CreateInput{LogicalInterface: "A", Snaplen: 256, TTL: time.Minute, ReorderWindow: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	agents.nodes = nil
	service.AgentDisconnected(context.Background(), "worker-1")
	captureSession, err = service.Get(context.Background(), captureSession.ID)
	if err != nil {
		t.Fatal(err)
	}
	if captureSession.Status != domain.SessionFailed || !packets.closed {
		t.Fatalf("unexpected disconnected session: %#v", captureSession)
	}
}

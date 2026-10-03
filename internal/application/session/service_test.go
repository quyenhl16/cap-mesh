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

type fakeRecoveryRecorder struct {
	started []domain.Session
	states  map[string]string
}

type fakeCapacityRecorder struct {
	started  bool
	checkErr error
}

func (f *fakeCapacityRecorder) Start(domain.Session) error                     { f.started = true; return nil }
func (f *fakeCapacityRecorder) RecoverableSessions() ([]domain.Session, error) { return nil, nil }
func (f *fakeCapacityRecorder) SetSessionDesiredState(string, string) error    { return nil }
func (f *fakeCapacityRecorder) ListSessionRecordings(context.Context) (domain.SessionRecordingCatalog, error) {
	return domain.SessionRecordingCatalog{}, nil
}
func (f *fakeCapacityRecorder) CleanSessionRecordings(context.Context, bool) (domain.RecordingCleanupResult, error) {
	return domain.RecordingCleanupResult{}, nil
}
func (f *fakeCapacityRecorder) CheckNormalRecordingCapacity(context.Context) error {
	return f.checkErr
}

func (f *fakeRecoveryRecorder) Start(session domain.Session) error {
	f.started = append(f.started, session)
	if f.states == nil {
		f.states = make(map[string]string)
	}
	f.states[session.ID] = "RUNNING"
	return nil
}

func (f *fakeRecoveryRecorder) RecoverableSessions() ([]domain.Session, error) { return nil, nil }
func (f *fakeRecoveryRecorder) SetSessionDesiredState(id, desired string) error {
	if f.states == nil {
		f.states = make(map[string]string)
	}
	f.states[id] = desired
	return nil
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

func TestCreateNormalSessionChecksRecordingCapacityButContinuousDoesNot(t *testing.T) {
	capacityErr := &ports.RecordingCapacityExceededError{Capacity: domain.RecordingCapacity{UsedSize: 100, MaxSize: 100}}
	recorder := &fakeCapacityRecorder{checkErr: capacityErr}
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	service := NewService(memory.NewSessionRepository(), agents, &fakePackets{}, recorder, 100)
	input := CreateInput{LogicalInterface: "management", Snaplen: 256, TTL: time.Minute}
	if _, err := service.Create(context.Background(), input); !errors.Is(err, capacityErr) {
		t.Fatalf("normal session error = %v", err)
	}
	if recorder.started || len(agents.commands) != 0 {
		t.Fatalf("capacity rejection had side effects: started=%v commands=%#v", recorder.started, agents.commands)
	}
	input.Continuous = true
	if _, err := service.Create(context.Background(), input); err != nil {
		t.Fatalf("continuous session was blocked by normal quota: %v", err)
	}
	if !recorder.started {
		t.Fatal("continuous recorder was not started")
	}
}

func TestRestoreWaitsForAgentThenReplaysCaptureCommand(t *testing.T) {
	agents := &fakeAgents{}
	packets := &fakePackets{}
	recorder := &fakeRecoveryRecorder{}
	service := NewService(memory.NewSessionRepository(), agents, packets, recorder, 100)
	persisted := domain.Session{
		ID: "restored-session", Mode: domain.SessionModeContinuous, Status: domain.SessionRunning,
		Snaplen: 512, CreatedAt: time.Now().UTC(), ReorderWindow: 10 * time.Millisecond,
		Targets: []domain.CaptureTarget{{ID: "management", Interface: &domain.InterfaceTarget{Nodes: []string{"worker-1"}, LogicalInterface: "management"}}},
	}
	restored, err := service.Restore(context.Background(), persisted)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status != domain.SessionRunning || len(agents.commands) != 0 {
		t.Fatalf("unexpected restore result: %#v commands=%#v", restored, agents.commands)
	}
	agents.nodes = []string{"worker-1"}
	service.AgentConnected(context.Background(), "worker-1")
	if len(agents.commands) != 1 || agents.commands[0].Kind != "start" || agents.commands[0].SessionID != persisted.ID {
		t.Fatalf("capture command was not replayed: %#v", agents.commands)
	}
	if _, err := service.Stop(context.Background(), persisted.ID); err != nil {
		t.Fatal(err)
	}
	if recorder.states[persisted.ID] != "STOPPED" {
		t.Fatalf("desired state = %q, want STOPPED", recorder.states[persisted.ID])
	}
}

func TestListFiltersSessionsAndSortsNewestFirst(t *testing.T) {
	repository := memory.NewSessionRepository()
	created := time.Now().UTC()
	for _, captureSession := range []domain.Session{
		{ID: "normal-old", Mode: domain.SessionModeNormal, Status: domain.SessionRunning, CreatedAt: created},
		{ID: "normal-new", Mode: domain.SessionModeNormal, Status: domain.SessionRunning, CreatedAt: created.Add(time.Second)},
		{ID: "normal-stopped", Mode: domain.SessionModeNormal, Status: domain.SessionStopped, CreatedAt: created.Add(2 * time.Second)},
		{ID: "continuous", Mode: domain.SessionModeContinuous, Status: domain.SessionRunning, CreatedAt: created.Add(3 * time.Second)},
	} {
		if err := repository.Create(context.Background(), captureSession); err != nil {
			t.Fatal(err)
		}
	}
	service := NewService(repository, &fakeAgents{}, &fakePackets{}, nil, 100)
	sessions, err := service.List(context.Background(), ListInput{Status: "running", Mode: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].ID != "normal-new" || sessions[1].ID != "normal-old" {
		t.Fatalf("unexpected sessions: %#v", sessions)
	}
	if _, err := service.List(context.Background(), ListInput{Status: "unknown"}); err == nil {
		t.Fatal("expected invalid status to fail")
	}
	if _, err := service.List(context.Background(), ListInput{Mode: "unknown"}); err == nil {
		t.Fatal("expected invalid mode to fail")
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

package session

import (
	"context"
	"testing"
	"time"

	"github.com/quyenhl16/capmesh/internal/adapter/memory"
	"github.com/quyenhl16/capmesh/internal/core/domain"
	"github.com/quyenhl16/capmesh/internal/core/ports"
)

type fakeAgents struct {
	nodes    []string
	commands []ports.AgentCommand
}

func (f *fakeAgents) ConnectedNodes() []string { return f.nodes }
func (f *fakeAgents) Send(_ context.Context, _ string, command ports.AgentCommand) error {
	f.commands = append(f.commands, command)
	return nil
}

type fakePackets struct {
	opened bool
	closed bool
}

func (f *fakePackets) OpenSession(string, time.Duration, int) error { f.opened = true; return nil }
func (f *fakePackets) Publish(domain.PacketBatch)                   {}
func (f *fakePackets) Subscribe(context.Context, string) (<-chan domain.PacketBatch, error) {
	return nil, nil
}
func (f *fakePackets) CloseSession(string) { f.closed = true }

func TestCreateAndStopSession(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1", "worker-2"}}
	packets := &fakePackets{}
	service := NewService(memory.NewSessionRepository(), agents, packets, 100)
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

func TestCreateRejectsDisconnectedNode(t *testing.T) {
	service := NewService(memory.NewSessionRepository(), &fakeAgents{nodes: []string{"worker-1"}}, &fakePackets{}, 100)
	_, err := service.Create(context.Background(), CreateInput{Nodes: []string{"worker-2"}, LogicalInterface: "A", Snaplen: 256, TTL: time.Minute, ReorderWindow: time.Millisecond})
	if err == nil {
		t.Fatal("expected unavailable node to fail")
	}
}

func TestSoleAgentDisconnectFailsSession(t *testing.T) {
	agents := &fakeAgents{nodes: []string{"worker-1"}}
	packets := &fakePackets{}
	repository := memory.NewSessionRepository()
	service := NewService(repository, agents, packets, 100)
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

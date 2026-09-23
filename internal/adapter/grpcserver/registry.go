package grpcserver

import (
	"context"
	"sort"
	"sync"

	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type agentConnection struct {
	commands chan ports.AgentCommand
}

type AgentRegistry struct {
	mu           sync.RWMutex
	agents       map[string]*agentConnection
	onDisconnect func(string)
}

func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{agents: make(map[string]*agentConnection)}
}

func (r *AgentRegistry) register(node string) *agentConnection {
	connection := &agentConnection{commands: make(chan ports.AgentCommand, 32)}
	r.mu.Lock()
	r.agents[node] = connection
	r.mu.Unlock()
	return connection
}

func (r *AgentRegistry) unregister(node string, connection *agentConnection) {
	r.mu.Lock()
	removed := false
	if r.agents[node] == connection {
		delete(r.agents, node)
		removed = true
	}
	handler := r.onDisconnect
	r.mu.Unlock()
	if removed && handler != nil {
		handler(node)
	}
}

func (r *AgentRegistry) OnDisconnect(handler func(string)) {
	r.mu.Lock()
	r.onDisconnect = handler
	r.mu.Unlock()
}

func (r *AgentRegistry) ConnectedNodes() []string {
	r.mu.RLock()
	nodes := make([]string, 0, len(r.agents))
	for node := range r.agents {
		nodes = append(nodes, node)
	}
	r.mu.RUnlock()
	sort.Strings(nodes)
	return nodes
}

func (r *AgentRegistry) Send(ctx context.Context, node string, command ports.AgentCommand) error {
	r.mu.RLock()
	connection := r.agents[node]
	r.mu.RUnlock()
	if connection == nil {
		return ports.ErrUnavailable
	}
	select {
	case connection.commands <- command:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ports.ErrUnavailable
	}
}

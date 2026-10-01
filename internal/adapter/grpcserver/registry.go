package grpcserver

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type agentConnection struct {
	commands    chan ports.AgentCommand
	interfaces  map[string]string
	connectedAt time.Time
	lastSeenAt  time.Time
}

type ConnectedAgent struct {
	NodeName    string
	Interfaces  map[string]string
	ConnectedAt time.Time
	LastSeenAt  time.Time
}

type AgentRegistry struct {
	mu           sync.RWMutex
	agents       map[string]*agentConnection
	onDisconnect func(string)
}

func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{agents: make(map[string]*agentConnection)}
}

func (r *AgentRegistry) register(node string, interfaces map[string]string) *agentConnection {
	now := time.Now().UTC()
	connection := &agentConnection{commands: make(chan ports.AgentCommand, 512), interfaces: cloneInterfaces(interfaces), connectedAt: now, lastSeenAt: now}
	r.mu.Lock()
	r.agents[node] = connection
	r.mu.Unlock()
	return connection
}

func (r *AgentRegistry) touch(node string, connection *agentConnection) {
	r.mu.Lock()
	if r.agents[node] == connection {
		connection.lastSeenAt = time.Now().UTC()
	}
	r.mu.Unlock()
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

func (r *AgentRegistry) ConnectedAgents() []ConnectedAgent {
	r.mu.RLock()
	agents := make([]ConnectedAgent, 0, len(r.agents))
	for node, connection := range r.agents {
		agents = append(agents, ConnectedAgent{
			NodeName:    node,
			Interfaces:  cloneInterfaces(connection.interfaces),
			ConnectedAt: connection.connectedAt,
			LastSeenAt:  connection.lastSeenAt,
		})
	}
	r.mu.RUnlock()
	sort.Slice(agents, func(i, j int) bool { return agents[i].NodeName < agents[j].NodeName })
	return agents
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

func cloneInterfaces(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for alias, name := range input {
		output[alias] = name
	}
	return output
}

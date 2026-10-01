package grpcserver

import (
	"testing"
	"time"
)

func TestConnectedAgentsReturnsSortedSnapshotsAndTracksLastSeen(t *testing.T) {
	registry := NewAgentRegistry()
	interfaces := map[string]string{"management": "eth0"}
	second := registry.register("worker-2", interfaces)
	first := registry.register("worker-1", map[string]string{"data": "eth1"})
	interfaces["management"] = "changed"

	agents := registry.ConnectedAgents()
	if len(agents) != 2 || agents[0].NodeName != "worker-1" || agents[1].NodeName != "worker-2" {
		t.Fatalf("unexpected agents: %#v", agents)
	}
	if agents[1].Interfaces["management"] != "eth0" {
		t.Fatalf("registered interfaces were not copied: %#v", agents[1].Interfaces)
	}
	previous := agents[0].LastSeenAt
	time.Sleep(time.Millisecond)
	registry.touch("worker-1", first)
	agents[0].Interfaces["data"] = "changed"
	updated := registry.ConnectedAgents()
	if !updated[0].LastSeenAt.After(previous) {
		t.Fatalf("last seen was not updated: before=%s after=%s", previous, updated[0].LastSeenAt)
	}
	if updated[0].Interfaces["data"] != "eth1" {
		t.Fatalf("returned interfaces were not copied: %#v", updated[0].Interfaces)
	}

	registry.unregister("worker-1", first)
	registry.unregister("worker-2", second)
	if remaining := registry.ConnectedAgents(); len(remaining) != 0 {
		t.Fatalf("agents remain after disconnect: %#v", remaining)
	}
}

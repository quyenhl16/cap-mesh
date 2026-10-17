package observability

import (
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRuntimeCollectorsRegisterAndGather(t *testing.T) {
	registry := prometheus.NewRegistry()
	RegisterRuntimeCollectors(registry)
	metrics, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seenGo, seenProcess := false, false
	for _, metric := range metrics {
		if metric.GetName() == "go_goroutines" {
			seenGo = true
		}
		if metric.GetName() == "process_cpu_seconds_total" {
			seenProcess = true
		}
	}
	if !seenGo || !seenProcess {
		t.Fatalf("runtime collectors missing: go=%v process=%v", seenGo, seenProcess)
	}
}

func TestPprofUsesDedicatedMux(t *testing.T) {
	server := NewPprofServer(":0")
	request := httptest.NewRequest("GET", "/debug/pprof/", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("status = %d", response.Code)
	}
}

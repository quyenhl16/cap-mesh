package benchmark

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadScenarioParsesDurations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	data := []byte("name: test\nduration: 2m\nwarmup: 5s\npacketSize: 512\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	scenario, err := LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	if scenario.Name != "test" || scenario.Duration == nil || time.Duration(*scenario.Duration) != 2*time.Minute || scenario.Warmup == nil || time.Duration(*scenario.Warmup) != 5*time.Second {
		t.Fatalf("unexpected scenario: %+v", scenario)
	}
}

func TestLoadScenarioRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	if err := os.WriteFile(path, []byte("name: test\nunknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil {
		t.Fatal("unknown scenario field was accepted")
	}
}

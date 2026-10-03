package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/quyenhl16/cap-mesh/internal/adapter/clientconfig"
)

func TestConfirmCleanupDefaultsToNo(t *testing.T) {
	var output bytes.Buffer
	if confirmCleanup(strings.NewReader("\n"), &output) {
		t.Fatal("empty confirmation was accepted")
	}
	if !strings.Contains(output.String(), "[y/N]") {
		t.Fatalf("prompt = %q", output.String())
	}
	if !confirmCleanup(strings.NewReader("yes\n"), &output) {
		t.Fatal("explicit yes was rejected")
	}
}

func TestFormatBytes(t *testing.T) {
	for value, want := range map[uint64]string{0: "0 B", 1024: "1.0 KiB", 5 << 20: "5.0 MiB", 2 << 30: "2.0 GiB"} {
		if got := formatBytes(value); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", value, got, want)
		}
	}
}

func TestTranslateCommandArgs(t *testing.T) {
	tests := []struct {
		input []string
		want  string
	}{
		{[]string{"get", "sessions", "--context", "prod"}, "--list-sessions --context prod"},
		{[]string{"describe", "session", "session-1"}, "--get-session session-1"},
		{[]string{"create", "session", "--interface", "A"}, "--create-only --interface A"},
		{[]string{"clean", "recordings", "--dry-run"}, "--clean-recordings --dry-run"},
		{[]string{"start", "log", "--workload", "default/deployment/api"}, "--log-start --workload default/deployment/api"},
		{[]string{"--context", "prod", "get", "sessions"}, "--list-sessions --context prod"},
	}
	for _, test := range tests {
		got, modern, err := translateCommandArgs(test.input)
		if err != nil {
			t.Fatalf("translateCommandArgs(%v): %v", test.input, err)
		}
		if !modern || strings.Join(got, " ") != test.want {
			t.Errorf("translateCommandArgs(%v) = %q, modern=%t; want %q", test.input, strings.Join(got, " "), modern, test.want)
		}
	}
}

func TestConfigCommandsAcceptNameBeforeFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	var output bytes.Buffer
	if err := runConfigCommand([]string{"set-context", "production", "--config", path, "--server", "capture.example.com:18443", "--tls-ca", "ca.crt", "--ttl", "10m"}, &output, &output); err != nil {
		t.Fatal(err)
	}
	config, err := clientconfig.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	selected := config.Contexts["production"]
	if config.CurrentContext != "production" || selected.Server != "capture.example.com:18443" || selected.TLSCA != "ca.crt" || selected.Defaults.TTL != "10m" {
		t.Fatalf("unexpected saved config: %+v", config)
	}
	if err := runConfigCommand([]string{"view", "--config", path, "--effective"}, &output, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "tlsCA: ca.crt") || !strings.Contains(output.String(), "ttl: 10m0s") {
		t.Fatalf("unexpected effective config output: %s", output.String())
	}
}

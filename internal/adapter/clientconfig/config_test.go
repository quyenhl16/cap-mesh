package clientconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadResolvePrecedence(t *testing.T) {
	root := t.TempDir()
	tokenPath := filepath.Join(root, "token")
	if err := os.WriteFile(tokenPath, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	follow := false
	config := New()
	config.CurrentContext = "prod"
	config.Contexts["prod"] = Context{
		Server: "configured:18443", TLSCA: "ca.crt", TokenFile: tokenPath,
		Defaults: Defaults{Snaplen: 2048, TTL: "10m", ReorderWindow: "1s", Direction: "both", Follow: &follow, MaxPods: 50},
	}
	path := filepath.Join(root, "config.yaml")
	if err := Save(path, config); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"CAPMESH_SERVER": "environment:18443", "CAPMESH_TOKEN": "env-token", "CAPMESH_TTL": "20m"}
	effective, err := loaded.Resolve("", func(name string) (string, bool) { value, ok := environment[name]; return value, ok })
	if err != nil {
		t.Fatal(err)
	}
	if effective.Server != "environment:18443" || effective.Token != "env-token" || effective.TTL != 20*time.Minute {
		t.Fatalf("unexpected environment precedence: %+v", effective)
	}
	if effective.Snaplen != 2048 || effective.Direction != "both" || effective.Follow || effective.MaxPods != 50 {
		t.Fatalf("context defaults not resolved: %+v", effective)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: capmesh.io/v1\nkind: ClientConfig\nunknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown field was accepted")
	}
}

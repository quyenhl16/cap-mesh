package benchmark

import (
	"os"
	"path/filepath"
	"testing"

	"go.yaml.in/yaml/v2"
)

func TestBenchmarkAssetsAreValidYAML(t *testing.T) {
	patterns := []string{
		filepath.Join("..", "..", "benchmarks", "scenarios", "*.yaml"),
		filepath.Join("..", "..", "benchmarks", "k8s", "*.yaml"),
	}
	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) == 0 {
			t.Fatalf("no assets matched %s", pattern)
		}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var document map[any]any
			if err := yaml.Unmarshal(data, &document); err != nil {
				t.Errorf("%s: %v", path, err)
			}
		}
	}
}

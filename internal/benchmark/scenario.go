package benchmark

import (
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v2"
)

type Scenario struct {
	Name            string    `yaml:"name"`
	Receiver        string    `yaml:"receiver"`
	TargetNamespace string    `yaml:"targetNamespace"`
	TargetKind      string    `yaml:"targetKind"`
	TargetName      string    `yaml:"targetName"`
	PacketSize      *int      `yaml:"packetSize"`
	PacketRate      *int      `yaml:"packetRate"`
	Duration        *Duration `yaml:"duration"`
	Warmup          *Duration `yaml:"warmup"`
	Settle          *Duration `yaml:"settle"`
	Drain           *Duration `yaml:"drain"`
	Subscribers     *int      `yaml:"subscribers"`
	Snaplen         *uint32   `yaml:"snaplen"`
	ReorderWindow   *Duration `yaml:"reorderWindow"`
	MaxLossPercent  *float64  `yaml:"maxLossPercent"`
	MaxP99Latency   *Duration `yaml:"maxP99Latency"`
}

type Duration time.Duration

func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func LoadScenario(path string) (Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("read scenario: %w", err)
	}
	var scenario Scenario
	if err := yaml.UnmarshalStrict(data, &scenario); err != nil {
		return Scenario{}, fmt.Errorf("decode scenario: %w", err)
	}
	return scenario, nil
}

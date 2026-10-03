package clientconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v2"
)

const (
	APIVersion = "capmesh.io/v1"
	Kind       = "ClientConfig"
)

type Config struct {
	APIVersion     string             `yaml:"apiVersion"`
	Kind           string             `yaml:"kind"`
	CurrentContext string             `yaml:"currentContext,omitempty"`
	Contexts       map[string]Context `yaml:"contexts,omitempty"`
}

type Context struct {
	Server        string   `yaml:"server,omitempty"`
	Insecure      bool     `yaml:"insecure,omitempty"`
	TLSCA         string   `yaml:"tlsCA,omitempty"`
	TLSServerName string   `yaml:"tlsServerName,omitempty"`
	TokenFile     string   `yaml:"tokenFile,omitempty"`
	Defaults      Defaults `yaml:"defaults,omitempty"`
}

type Defaults struct {
	Snaplen       uint   `yaml:"snaplen,omitempty"`
	TTL           string `yaml:"ttl,omitempty"`
	ReorderWindow string `yaml:"reorderWindow,omitempty"`
	Direction     string `yaml:"direction,omitempty"`
	Follow        *bool  `yaml:"follow,omitempty"`
	MaxPods       uint   `yaml:"maxPods,omitempty"`
}

type Effective struct {
	ContextName   string        `yaml:"context"`
	Server        string        `yaml:"server"`
	Insecure      bool          `yaml:"insecure"`
	TLSCA         string        `yaml:"tlsCA,omitempty"`
	TLSServerName string        `yaml:"tlsServerName,omitempty"`
	TokenFile     string        `yaml:"tokenFile,omitempty"`
	Token         string        `yaml:"token,omitempty"`
	Snaplen       uint          `yaml:"snaplen"`
	TTL           time.Duration `yaml:"ttl"`
	ReorderWindow time.Duration `yaml:"reorderWindow"`
	Direction     string        `yaml:"direction"`
	Follow        bool          `yaml:"follow"`
	MaxPods       uint          `yaml:"maxPods"`
}

func DefaultPath() (string, error) {
	directory, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config directory: %w", err)
	}
	return filepath.Join(directory, "capmesh", "config.yaml"), nil
}

func New() Config {
	return Config{APIVersion: APIVersion, Kind: Kind, Contexts: make(map[string]Context)}
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return New(), nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read client config %s: %w", path, err)
	}
	config := New()
	if err := yaml.UnmarshalStrict(data, &config); err != nil {
		return Config{}, fmt.Errorf("parse client config %s: %w", path, err)
	}
	if config.APIVersion != APIVersion {
		return Config{}, fmt.Errorf("unsupported client config apiVersion %q", config.APIVersion)
	}
	if config.Kind != Kind {
		return Config{}, fmt.Errorf("unsupported client config kind %q", config.Kind)
	}
	if config.Contexts == nil {
		config.Contexts = make(map[string]Context)
	}
	if config.CurrentContext != "" {
		if _, ok := config.Contexts[config.CurrentContext]; !ok {
			return Config{}, fmt.Errorf("current context %q does not exist", config.CurrentContext)
		}
	}
	return config, nil
}

func Save(path string, config Config) error {
	if config.APIVersion == "" {
		config.APIVersion = APIVersion
	}
	if config.Kind == "" {
		config.Kind = Kind
	}
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode client config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create client config directory: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write client config %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure client config %s: %w", path, err)
	}
	return nil
}

func (c Config) Resolve(name string, environment func(string) (string, bool)) (Effective, error) {
	result := Effective{
		Server:        "127.0.0.1:18443",
		Snaplen:       4096,
		TTL:           5 * time.Minute,
		ReorderWindow: 300 * time.Millisecond,
		Direction:     "egress",
		Follow:        true,
		MaxPods:       100,
	}
	if name == "" {
		name = c.CurrentContext
	}
	result.ContextName = name
	if name != "" {
		selected, ok := c.Contexts[name]
		if !ok {
			return Effective{}, fmt.Errorf("context %q does not exist", name)
		}
		result.Server = valueOr(selected.Server, result.Server)
		result.Insecure = selected.Insecure
		result.TLSCA = selected.TLSCA
		result.TLSServerName = selected.TLSServerName
		result.TokenFile = selected.TokenFile
		if selected.Defaults.Snaplen != 0 {
			result.Snaplen = selected.Defaults.Snaplen
		}
		if err := applyDuration(selected.Defaults.TTL, &result.TTL, "ttl"); err != nil {
			return Effective{}, err
		}
		if err := applyDuration(selected.Defaults.ReorderWindow, &result.ReorderWindow, "reorderWindow"); err != nil {
			return Effective{}, err
		}
		result.Direction = valueOr(selected.Defaults.Direction, result.Direction)
		if selected.Defaults.Follow != nil {
			result.Follow = *selected.Defaults.Follow
		}
		if selected.Defaults.MaxPods != 0 {
			result.MaxPods = selected.Defaults.MaxPods
		}
	}
	if result.TokenFile != "" {
		token, err := readToken(result.TokenFile)
		if err != nil {
			return Effective{}, err
		}
		result.Token = token
	}
	var err error
	applyStringEnv(environment, "CAPMESH_SERVER", &result.Server)
	applyStringEnv(environment, "CAPMESH_TLS_CA", &result.TLSCA)
	applyStringEnv(environment, "CAPMESH_TLS_SERVER_NAME", &result.TLSServerName)
	applyStringEnv(environment, "CAPMESH_TOKEN_FILE", &result.TokenFile)
	if value, ok := environment("CAPMESH_TOKEN_FILE"); ok && strings.TrimSpace(value) != "" {
		result.Token, err = readToken(value)
		if err != nil {
			return Effective{}, err
		}
	}
	applyStringEnv(environment, "CAPMESH_TOKEN", &result.Token)
	if err := applyBoolEnv(environment, "CAPMESH_INSECURE", &result.Insecure); err != nil {
		return Effective{}, err
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{{"CAPMESH_TTL", &result.TTL}, {"CAPMESH_REORDER_WINDOW", &result.ReorderWindow}} {
		if value, ok := environment(item.name); ok && value != "" {
			if err := applyDuration(value, item.target, item.name); err != nil {
				return Effective{}, err
			}
		}
	}
	if err := applyUintEnv(environment, "CAPMESH_SNAPLEN", &result.Snaplen); err != nil {
		return Effective{}, err
	}
	if err := applyUintEnv(environment, "CAPMESH_MAX_PODS", &result.MaxPods); err != nil {
		return Effective{}, err
	}
	applyStringEnv(environment, "CAPMESH_DIRECTION", &result.Direction)
	if err := applyBoolEnv(environment, "CAPMESH_FOLLOW", &result.Follow); err != nil {
		return Effective{}, err
	}
	return result, nil
}

func (c Config) ContextNames() []string {
	names := make([]string, 0, len(c.Contexts))
	for name := range c.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func Marshal(value any) ([]byte, error) { return yaml.Marshal(value) }

func readToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file %s: %w", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func applyDuration(value string, target *time.Duration, field string) error {
	if value == "" {
		return nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", field, value, err)
	}
	*target = parsed
	return nil
}

func applyStringEnv(environment func(string) (string, bool), name string, target *string) {
	if value, ok := environment(name); ok {
		*target = value
	}
}

func applyBoolEnv(environment func(string) (string, bool), name string, target *bool) error {
	value, ok := environment(name)
	if !ok || value == "" {
		return nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", name, value, err)
	}
	*target = parsed
	return nil
}

func applyUintEnv(environment func(string) (string, bool), name string, target *uint) error {
	value, ok := environment(name)
	if !ok || value == "" {
		return nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", name, value, err)
	}
	*target = uint(parsed)
	return nil
}

package envconfig

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Apply uses environment variables as flag defaults. Calling FlagSet.Parse
// afterwards preserves the expected precedence: CLI flags override env values.
func Apply(flags *flag.FlagSet, bindings map[string]string) error {
	keys := make([]string, 0, len(bindings))
	for environment := range bindings {
		keys = append(keys, environment)
	}
	sort.Strings(keys)
	for _, environment := range keys {
		value, exists := os.LookupEnv(environment)
		if !exists {
			continue
		}
		name := bindings[environment]
		if err := flags.Set(name, value); err != nil {
			return fmt.Errorf("invalid %s for --%s: %w", environment, name, err)
		}
	}
	return nil
}

// ApplyList applies a comma-separated environment variable to a repeatable
// flag such as --interface alias=physical.
func ApplyList(flags *flag.FlagSet, environment, name string) error {
	value, exists := os.LookupEnv(environment)
	if !exists || strings.TrimSpace(value) == "" {
		return nil
	}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if err := flags.Set(name, item); err != nil {
			return fmt.Errorf("invalid %s item %q for --%s: %w", environment, item, name, err)
		}
	}
	return nil
}

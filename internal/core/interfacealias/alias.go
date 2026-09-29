package interfacealias

import (
	"fmt"
	"regexp"
	"strings"
)

const MaxMappings = 10

const MaxLength = 53

var aliasPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,51}[a-z0-9])?$`)

// Normalize returns the canonical form of a logical interface alias. The
// original A/B/C aliases remain uppercase for backwards compatibility; new
// aliases are case-insensitive and represented in lowercase.
func Normalize(value string) (string, error) {
	alias := strings.ToLower(strings.TrimSpace(value))
	if alias == "a" || alias == "b" || alias == "c" {
		return strings.ToUpper(alias), nil
	}
	if !aliasPattern.MatchString(alias) {
		return "", fmt.Errorf("interface alias %q must contain 1-%d letters, numbers, or hyphens and start and end with a letter or number", value, MaxLength)
	}
	return alias, nil
}

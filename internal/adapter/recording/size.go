package recording

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var sizePattern = regexp.MustCompile(`(?i)^([0-9]+)(B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)?$`)

func ParseSize(value string) (int64, error) {
	match := sizePattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return 0, fmt.Errorf("invalid byte size %q (examples: 100MiB, 10GiB)", value)
	}
	n, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", value, err)
	}
	multipliers := map[string]uint64{
		"": 1, "B": 1,
		"KB": 1000, "MB": 1000 * 1000, "GB": 1000 * 1000 * 1000, "TB": 1000 * 1000 * 1000 * 1000,
		"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
	}
	multiplier := multipliers[strings.ToUpper(match[2])]
	if n > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size %q is too large", value)
	}
	return int64(n * multiplier), nil
}

package capture

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type IPRouteResolver struct {
	Binary string
}

func (r IPRouteResolver) Resolve(ctx context.Context, podIP string) (string, error) {
	binary := r.Binary
	if binary == "" {
		binary = "ip"
	}
	output, err := exec.CommandContext(ctx, binary, "route", "get", podIP).Output()
	if err != nil {
		return "", err
	}
	return parseRouteInterface(string(output))
}

func parseRouteInterface(output string) (string, error) {
	fields := strings.Fields(string(output))
	for index := 0; index+1 < len(fields); index++ {
		if fields[index] == "dev" && fields[index+1] != "" {
			return fields[index+1], nil
		}
	}
	return "", fmt.Errorf("route output contains no interface: %s", strings.TrimSpace(string(output)))
}

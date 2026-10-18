package capture

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/bpf"
)

type FilterCompiler interface {
	Compile(context.Context, string, string, uint32) ([]bpf.RawInstruction, error)
}

type DumpcapFilterCompiler struct {
	Binary string
	mu     sync.Mutex
	cache  map[string][]bpf.RawInstruction
}

const maxFilterCacheEntries = 256

func NewDumpcapFilterCompiler(binary string) *DumpcapFilterCompiler {
	return &DumpcapFilterCompiler{Binary: binary, cache: make(map[string][]bpf.RawInstruction)}
}

func (c *DumpcapFilterCompiler) Compile(ctx context.Context, interfaceName, filter string, snaplen uint32) ([]bpf.RawInstruction, error) {
	if snaplen < 1 || snaplen > 65535 {
		return nil, errors.New("filter snaplen must be between 1 and 65535")
	}
	filter = strings.TrimSpace(filter)
	if filter == "" {
		// BPF_RET | BPF_K. Attaching even for an empty expression makes the
		// kernel apply snaplen before copying a packet into the mmap ring.
		return []bpf.RawInstruction{{Op: 0x06, K: snaplen}}, nil
	}
	key := interfaceName + "\x00" + strconv.FormatUint(uint64(snaplen), 10) + "\x00" + filter
	c.mu.Lock()
	if cached, ok := c.cache[key]; ok {
		result := append([]bpf.RawInstruction(nil), cached...)
		c.mu.Unlock()
		return result, nil
	}
	c.mu.Unlock()

	command := exec.CommandContext(ctx, c.Binary, "-i", interfaceName, "-s", strconv.FormatUint(uint64(snaplen), 10), "-f", filter, "-ddd")
	output, err := command.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) && len(exitError.Stderr) > 0 {
			return nil, fmt.Errorf("compile capture filter: %w: %s", err, strings.TrimSpace(string(exitError.Stderr)))
		}
		return nil, fmt.Errorf("compile capture filter: %w", err)
	}
	instructions, err := parseDecimalBPF(string(output))
	if err != nil {
		return nil, fmt.Errorf("parse dumpcap filter bytecode: %w", err)
	}
	c.mu.Lock()
	if len(c.cache) >= maxFilterCacheEntries {
		c.cache = make(map[string][]bpf.RawInstruction)
	}
	c.cache[key] = append([]bpf.RawInstruction(nil), instructions...)
	c.mu.Unlock()
	return instructions, nil
}

func parseDecimalBPF(output string) ([]bpf.RawInstruction, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return nil, errors.New("missing instruction count or bytecode")
	}
	count, err := strconv.ParseUint(strings.TrimSpace(lines[0]), 10, 16)
	if err != nil || count == 0 {
		return nil, fmt.Errorf("invalid instruction count %q", strings.TrimSpace(lines[0]))
	}
	if len(lines)-1 != int(count) {
		return nil, fmt.Errorf("instruction count is %d but output contains %d instructions", count, len(lines)-1)
	}
	instructions := make([]bpf.RawInstruction, 0, count)
	for index, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) != 4 {
			return nil, fmt.Errorf("instruction %d has %d fields, want 4", index, len(fields))
		}
		code, codeErr := strconv.ParseUint(fields[0], 10, 16)
		jt, jtErr := strconv.ParseUint(fields[1], 10, 8)
		jf, jfErr := strconv.ParseUint(fields[2], 10, 8)
		k, kErr := strconv.ParseUint(fields[3], 10, 32)
		if codeErr != nil || jtErr != nil || jfErr != nil || kErr != nil {
			return nil, fmt.Errorf("instruction %d contains an invalid integer", index)
		}
		instructions = append(instructions, bpf.RawInstruction{Op: uint16(code), Jt: uint8(jt), Jf: uint8(jf), K: uint32(k)})
	}
	return instructions, nil
}

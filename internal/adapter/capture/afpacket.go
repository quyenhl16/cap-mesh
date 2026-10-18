package capture

import (
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

var (
	ErrAFPacketUnsupported = errors.New("AF_PACKET is only supported on Linux")
	ErrRingBudgetExceeded  = errors.New("AF_PACKET ring memory budget exceeded")
	ErrAFPacketInitialize  = errors.New("AF_PACKET initialization failed")
	ErrInvalidCapture      = errors.New("invalid capture request")
	ErrFilterCompile       = errors.New("capture filter compilation failed")
)

const (
	defaultRingSize    = 64 << 20
	defaultBlockSize   = 1 << 20
	defaultFrameSize   = 4 << 10
	defaultRingBudget  = 512 << 20
	defaultStatsPeriod = time.Second
)

var byteSizePattern = regexp.MustCompile(`(?i)^([0-9]+)(B|KB|MB|GB|KIB|MIB|GIB)?$`)

func ParseByteSize(value string) (int64, error) {
	match := byteSizePattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return 0, fmt.Errorf("invalid byte size %q (examples: 64MiB, 1GiB)", value)
	}
	number, err := strconv.ParseUint(match[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", value, err)
	}
	multipliers := map[string]uint64{
		"": 1, "B": 1,
		"KB": 1000, "MB": 1000 * 1000, "GB": 1000 * 1000 * 1000,
		"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30,
	}
	multiplier := multipliers[strings.ToUpper(match[2])]
	if number > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size %q is too large", value)
	}
	return int64(number * multiplier), nil
}

type AFPacketConfig struct {
	RingSize      int
	BlockSize     int
	FrameSize     int
	BlockTimeout  time.Duration
	PollTimeout   time.Duration
	StatsInterval time.Duration
}

func DefaultAFPacketConfig() AFPacketConfig {
	return AFPacketConfig{
		RingSize:      defaultRingSize,
		BlockSize:     defaultBlockSize,
		FrameSize:     defaultFrameSize,
		BlockTimeout:  time.Millisecond,
		PollTimeout:   time.Millisecond,
		StatsInterval: defaultStatsPeriod,
	}
}

func (c AFPacketConfig) Validate() error {
	pageSize := os.Getpagesize()
	switch {
	case c.RingSize <= 0:
		return errors.New("AF_PACKET ring size must be positive")
	case c.BlockSize <= 0:
		return errors.New("AF_PACKET block size must be positive")
	case c.FrameSize <= 0:
		return errors.New("AF_PACKET frame size must be positive")
	case c.RingSize%c.BlockSize != 0:
		return fmt.Errorf("AF_PACKET ring size %d must be divisible by block size %d", c.RingSize, c.BlockSize)
	case c.BlockSize%pageSize != 0:
		return fmt.Errorf("AF_PACKET block size %d must be divisible by page size %d", c.BlockSize, pageSize)
	case c.BlockSize%c.FrameSize != 0:
		return fmt.Errorf("AF_PACKET block size %d must be divisible by frame size %d", c.BlockSize, c.FrameSize)
	case c.BlockTimeout < time.Millisecond:
		return errors.New("AF_PACKET block timeout must be at least 1ms")
	case c.PollTimeout < time.Millisecond:
		return errors.New("AF_PACKET poll timeout must be at least 1ms")
	case c.StatsInterval <= 0:
		return errors.New("AF_PACKET stats interval must be positive")
	}
	return nil
}

type RingBudget struct {
	mu   sync.Mutex
	max  int64
	used int64
}

func NewRingBudget(max int64) (*RingBudget, error) {
	if max <= 0 {
		return nil, errors.New("AF_PACKET ring budget must be positive")
	}
	return &RingBudget{max: max}, nil
}

func DefaultRingBudget() *RingBudget {
	budget, _ := NewRingBudget(defaultRingBudget)
	return budget
}

func (b *RingBudget) acquire(size int64) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size <= 0 || size > b.max-b.used {
		return nil, fmt.Errorf("%w: requested=%d used=%d limit=%d", ErrRingBudgetExceeded, size, b.used, b.max)
	}
	b.used += size
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= size
			b.mu.Unlock()
		})
	}, nil
}

func (b *RingBudget) Usage() (used, limit int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.max
}

type AFPacket struct {
	Config   AFPacketConfig
	Compiler FilterCompiler
	Budget   *RingBudget
	Metrics  ports.CaptureEngineMetrics
}

func NewAFPacket(config AFPacketConfig, compiler FilterCompiler, budget *RingBudget, metrics ports.CaptureEngineMetrics) (*AFPacket, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if compiler == nil {
		return nil, errors.New("AF_PACKET filter compiler is required")
	}
	if budget == nil {
		return nil, errors.New("AF_PACKET ring budget is required")
	}
	if _, limit := budget.Usage(); int64(config.RingSize) > limit {
		return nil, fmt.Errorf("%w: ring=%d limit=%d", ErrRingBudgetExceeded, config.RingSize, limit)
	}
	return &AFPacket{Config: config, Compiler: compiler, Budget: budget, Metrics: metrics}, nil
}

func validateCaptureRequest(request ports.CaptureRequest) error {
	switch {
	case request.InterfaceName == "":
		return fmt.Errorf("%w: interface is required", ErrInvalidCapture)
	case request.Snaplen < 1 || request.Snaplen > 65535:
		return fmt.Errorf("%w: snaplen must be between 1 and 65535", ErrInvalidCapture)
	case request.MaxPackets < 1 || request.MaxBytes < 1 || request.MaxDelay <= 0:
		return fmt.Errorf("%w: batch limits must be positive", ErrInvalidCapture)
	}
	return nil
}

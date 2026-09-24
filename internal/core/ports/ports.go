package ports

import (
	"context"
	"errors"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrUnavailable   = errors.New("unavailable")
)

type SessionRepository interface {
	Create(context.Context, domain.Session) error
	Get(context.Context, string) (domain.Session, error)
	Update(context.Context, domain.Session) error
	List(context.Context) ([]domain.Session, error)
}

type AgentCommand struct {
	Kind             string
	SessionID        string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
}

type AgentCommander interface {
	ConnectedNodes() []string
	Send(context.Context, string, AgentCommand) error
}

type PacketPublisher interface {
	OpenSession(string, time.Duration, int) error
	Publish(domain.PacketBatch)
	Subscribe(context.Context, string) (<-chan domain.PacketBatch, error)
	CloseSession(string)
}

type Metrics interface {
	PacketsReceived(int)
	PacketsEmitted(int)
	LatePacket()
	SubscriberDrop()
	SetReorderBuffer(int)
	ObserveSubscriberQueue(float64)
}

type CaptureEngine interface {
	Capture(context.Context, string, string, uint32) (<-chan domain.Packet, <-chan error, error)
}

type AgentReporter interface {
	SendBatch(context.Context, domain.PacketBatch) error
	SendStatus(context.Context, string, string, string) error
}

type CaptureProgress struct {
	SessionID       string
	NodeName        string
	InterfaceName   string
	PacketsTotal    uint64
	BytesTotal      uint64
	PacketsInterval uint64
	BytesInterval   uint64
	StartedAt       time.Time
	ObservedAt      time.Time
	Interval        time.Duration
	Final           bool
}

type CaptureProgressReporter interface {
	ReportCaptureProgress(CaptureProgress)
}

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
	SourceID         string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
	Source           domain.CaptureSource
}

type AgentCommander interface {
	ConnectedNodes() []string
	Send(context.Context, string, AgentCommand) error
}

type PacketPublisher interface {
	OpenSession(string, time.Duration, int) error
	Publish(domain.PacketBatch)
	Subscribe(context.Context, string) (<-chan domain.PacketBatch, error)
	SubscribeLossAware(context.Context, string, int) (PacketSubscription, error)
	CloseSession(string)
}

type PacketSubscription struct {
	Batches <-chan domain.PacketBatch
	Dropped <-chan struct{}
}

type CaptureRecorder interface {
	Start(domain.Session) error
}

type RecordingUsageProvider interface {
	Usage(string) (domain.RecordingUsage, bool)
}

type WorkloadResolver interface {
	Resolve(context.Context, domain.WorkloadTarget, string) ([]domain.CaptureSource, error)
}

type PodInterfaceResolver interface {
	Resolve(context.Context, string) (string, error)
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
	SendStatus(context.Context, string, string, string, string) error
}

type CaptureProgress struct {
	SessionID       string
	SourceID        string
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

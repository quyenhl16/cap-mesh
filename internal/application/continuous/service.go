package continuous

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type State string

const (
	StateStopped  State = "STOPPED"
	StateStarting State = "STARTING"
	StateRunning  State = "RUNNING"
	StateStopping State = "STOPPING"
	StateFailed   State = "FAILED"
)

type StartInput struct {
	Targets       []domain.CaptureTarget
	Filter        string
	Snaplen       uint32
	ReorderWindow time.Duration
}

type Capture struct {
	SessionID    string
	Status       State
	StartedAt    time.Time
	RetainedSize int64
	SegmentCount int
	Message      string
}

type Service struct {
	mu                  sync.Mutex
	sessions            *appsession.Service
	usage               ports.RecordingUsageProvider
	recordingConfigured bool
	state               Capture
}

func NewService(sessions *appsession.Service, usage ports.RecordingUsageProvider, recordingConfigured bool) *Service {
	return &Service{sessions: sessions, usage: usage, recordingConfigured: recordingConfigured, state: Capture{Status: StateStopped}}
}

func (s *Service) Restore(session domain.Session) error {
	if session.Mode != domain.SessionModeContinuous || session.ID == "" {
		return fmt.Errorf("invalid continuous session recovery state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Status != StateStopped {
		return fmt.Errorf("%w: continuous capture is %s", ports.ErrAlreadyExists, s.state.Status)
	}
	s.state = Capture{SessionID: session.ID, Status: StateRunning, StartedAt: session.CreatedAt, Message: session.Message}
	return nil
}

func (s *Service) Start(ctx context.Context, input StartInput) (Capture, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recordingConfigured || s.usage == nil {
		return s.snapshotLocked(), fmt.Errorf("%w: server-side recording is disabled", ports.ErrUnavailable)
	}
	if s.state.Status != StateStopped {
		return s.snapshotLocked(), fmt.Errorf("%w: continuous capture is %s; stop it before starting again", ports.ErrAlreadyExists, s.state.Status)
	}
	s.state = Capture{Status: StateStarting}
	captureSession, err := s.sessions.Create(ctx, appsession.CreateInput{
		Targets:       input.Targets,
		Filter:        input.Filter,
		Snaplen:       input.Snaplen,
		ReorderWindow: input.ReorderWindow,
		Continuous:    true,
	})
	if err != nil {
		s.state = Capture{Status: StateStopped, Message: err.Error()}
		return s.snapshotLocked(), err
	}
	s.state = Capture{SessionID: captureSession.ID, Status: StateRunning, StartedAt: captureSession.CreatedAt, Message: captureSession.Message}
	return s.snapshotLocked(), nil
}

func (s *Service) Stop(ctx context.Context) (Capture, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Status == StateStopped {
		return s.snapshotLocked(), nil
	}
	sessionID := s.state.SessionID
	s.state.Status = StateStopping
	if sessionID != "" {
		if _, err := s.sessions.Stop(ctx, sessionID); err != nil {
			s.state.Status = StateFailed
			s.state.Message = err.Error()
			return s.snapshotLocked(), err
		}
	}
	last := s.snapshotLocked()
	last.Status = StateStopped
	last.Message = ""
	s.state = last
	return s.snapshotLocked(), nil
}

func (s *Service) Get(ctx context.Context) Capture {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.SessionID != "" && s.state.Status == StateRunning {
		if captureSession, err := s.sessions.Get(ctx, s.state.SessionID); err == nil {
			s.state.Message = captureSession.Message
			if captureSession.Status == domain.SessionFailed {
				s.state.Status = StateFailed
			}
		}
	}
	return s.snapshotLocked()
}

func (s *Service) RecordingCompleted(sessionID, status, message string) {
	s.mu.Lock()
	if s.state.SessionID != sessionID || s.state.Status == StateStopped || s.state.Status == StateStopping {
		s.mu.Unlock()
		return
	}
	s.state.Status = StateFailed
	if message == "" {
		s.state.Message = "continuous recorder stopped unexpectedly with status " + status
	} else {
		s.state.Message = message
	}
	s.mu.Unlock()
	go func() { _, _ = s.sessions.Stop(context.Background(), sessionID) }()
}

func (s *Service) snapshotLocked() Capture {
	result := s.state
	if result.SessionID != "" && s.usage != nil {
		if usage, ok := s.usage.Usage(result.SessionID); ok {
			result.RetainedSize = usage.RetainedSize
			result.SegmentCount = usage.SegmentCount
		}
	}
	return result
}

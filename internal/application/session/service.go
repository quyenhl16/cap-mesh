package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type CreateInput struct {
	Nodes            []string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
	TTL              time.Duration
	ReorderWindow    time.Duration
}

type Service struct {
	repository     ports.SessionRepository
	agents         ports.AgentCommander
	packets        ports.PacketPublisher
	subscriberSize int
	now            func() time.Time
}

func NewService(repository ports.SessionRepository, agents ports.AgentCommander, packets ports.PacketPublisher, subscriberSize int) *Service {
	return &Service{repository: repository, agents: agents, packets: packets, subscriberSize: subscriberSize, now: time.Now}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (domain.Session, error) {
	if err := validate(input); err != nil {
		return domain.Session{}, err
	}
	connected := s.agents.ConnectedNodes()
	nodes := append([]string(nil), input.Nodes...)
	if len(nodes) == 0 {
		nodes = connected
	}
	if len(nodes) == 0 {
		return domain.Session{}, fmt.Errorf("%w: no agents connected", ports.ErrUnavailable)
	}
	for _, node := range nodes {
		if !slices.Contains(connected, node) {
			return domain.Session{}, fmt.Errorf("%w: agent %q is not connected", ports.ErrUnavailable, node)
		}
	}
	now := s.now()
	session := domain.Session{ID: newID(), Nodes: nodes, LogicalInterface: strings.ToUpper(input.LogicalInterface), Filter: input.Filter, Snaplen: input.Snaplen, ReorderWindow: input.ReorderWindow, CreatedAt: now, ExpiresAt: now.Add(input.TTL), Status: domain.SessionStarting}
	if err := s.repository.Create(ctx, session); err != nil {
		return domain.Session{}, err
	}
	if err := s.packets.OpenSession(session.ID, input.ReorderWindow, s.subscriberSize); err != nil {
		return domain.Session{}, err
	}
	started := 0
	var failures []string
	for _, node := range nodes {
		err := s.agents.Send(ctx, node, ports.AgentCommand{Kind: "start", SessionID: session.ID, LogicalInterface: session.LogicalInterface, Filter: session.Filter, Snaplen: session.Snaplen})
		if err != nil {
			failures = append(failures, node+": "+err.Error())
			continue
		}
		started++
	}
	if started == 0 {
		_ = session.Transition(domain.SessionFailed)
		session.Message = strings.Join(failures, "; ")
		s.packets.CloseSession(session.ID)
	} else {
		_ = session.Transition(domain.SessionRunning)
		if len(failures) > 0 {
			session.Message = "partial: " + strings.Join(failures, "; ")
		}
	}
	if err := s.repository.Update(ctx, session); err != nil {
		return domain.Session{}, err
	}
	if session.Status == domain.SessionRunning {
		time.AfterFunc(input.TTL, func() { _, _ = s.Stop(context.Background(), session.ID) })
	}
	return session, nil
}

func (s *Service) Stop(ctx context.Context, id string) (domain.Session, error) {
	session, err := s.repository.Get(ctx, id)
	if err != nil {
		return domain.Session{}, err
	}
	if session.Status == domain.SessionStopped || session.Status == domain.SessionFailed {
		return session, nil
	}
	if err := session.Transition(domain.SessionStopping); err != nil {
		return domain.Session{}, err
	}
	_ = s.repository.Update(ctx, session)
	for _, node := range session.Nodes {
		_ = s.agents.Send(ctx, node, ports.AgentCommand{Kind: "stop", SessionID: session.ID})
	}
	s.packets.CloseSession(id)
	_ = session.Transition(domain.SessionStopped)
	if err := s.repository.Update(ctx, session); err != nil {
		return domain.Session{}, err
	}
	return session, nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Session, error) {
	return s.repository.Get(ctx, id)
}

func (s *Service) AgentDisconnected(ctx context.Context, node string) {
	sessions, err := s.repository.List(ctx)
	if err != nil {
		return
	}
	for _, captureSession := range sessions {
		if captureSession.Status != domain.SessionRunning || !slices.Contains(captureSession.Nodes, node) {
			continue
		}
		captureSession.Message = "partial: agent disconnected: " + node
		connected := s.agents.ConnectedNodes()
		hasActiveNode := false
		for _, sessionNode := range captureSession.Nodes {
			hasActiveNode = hasActiveNode || slices.Contains(connected, sessionNode)
		}
		if !hasActiveNode {
			_ = captureSession.Transition(domain.SessionFailed)
			captureSession.Message = "all capture agents disconnected"
			s.packets.CloseSession(captureSession.ID)
		}
		_ = s.repository.Update(ctx, captureSession)
	}
}

func validate(input CreateInput) error {
	switch {
	case input.LogicalInterface != "A" && input.LogicalInterface != "B" && input.LogicalInterface != "C":
		return errors.New("logical interface must be A, B, or C")
	case input.Snaplen < 1 || input.Snaplen > 65535:
		return errors.New("snaplen must be between 1 and 65535")
	case input.TTL < time.Second || input.TTL > 24*time.Hour:
		return errors.New("TTL must be between 1 second and 24 hours")
	case input.ReorderWindow < 0 || input.ReorderWindow > 5*time.Second:
		return errors.New("reorder window must be between 0 and 5 seconds")
	case len(input.Filter) > 4096:
		return errors.New("filter is too long")
	case len(input.Nodes) > 100:
		return errors.New("too many nodes")
	}
	return nil
}

func newID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return "session-" + hex.EncodeToString(value[:])
}

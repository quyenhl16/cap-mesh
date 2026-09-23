package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/quyenhl16/capmesh/internal/core/domain"
	"github.com/quyenhl16/capmesh/internal/core/ports"
)

type StartRequest struct {
	SessionID        string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
}

type Service struct {
	mu              sync.Mutex
	nodeName        string
	interfaces      map[string]string
	engine          ports.CaptureEngine
	reporter        ports.AgentReporter
	batchMaxPackets int
	batchMaxDelay   time.Duration
	captures        map[string]context.CancelFunc
}

func NewService(nodeName string, interfaces map[string]string, engine ports.CaptureEngine, reporter ports.AgentReporter, maxPackets int, maxDelay time.Duration) *Service {
	return &Service{nodeName: nodeName, interfaces: interfaces, engine: engine, reporter: reporter, batchMaxPackets: maxPackets, batchMaxDelay: maxDelay, captures: make(map[string]context.CancelFunc)}
}

func (s *Service) Start(parent context.Context, request StartRequest) error {
	iface, ok := s.interfaces[request.LogicalInterface]
	if !ok || iface == "" {
		return fmt.Errorf("logical interface %q is not configured", request.LogicalInterface)
	}
	s.mu.Lock()
	if _, exists := s.captures[request.SessionID]; exists {
		s.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	s.captures[request.SessionID] = cancel
	s.mu.Unlock()

	packets, captureErrors, err := s.engine.Capture(ctx, iface, request.Filter, request.Snaplen)
	if err != nil {
		s.remove(request.SessionID)
		cancel()
		return err
	}
	go s.batch(ctx, request.SessionID, iface, packets, captureErrors)
	return s.reporter.SendStatus(parent, request.SessionID, "RUNNING", "")
}

func (s *Service) Stop(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	cancel, exists := s.captures[sessionID]
	if exists {
		delete(s.captures, sessionID)
	}
	s.mu.Unlock()
	if exists {
		cancel()
	}
	return s.reporter.SendStatus(ctx, sessionID, "STOPPED", "")
}

func (s *Service) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, cancel := range s.captures {
		cancel()
		delete(s.captures, id)
	}
}

func (s *Service) batch(ctx context.Context, sessionID, iface string, packets <-chan domain.Packet, captureErrors <-chan error) {
	defer s.remove(sessionID)
	timer := time.NewTimer(s.batchMaxDelay)
	if !timer.Stop() {
		<-timer.C
	}
	var batch []domain.Packet
	errorsChannel := captureErrors
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		out := domain.PacketBatch{SessionID: sessionID, NodeName: s.nodeName, InterfaceName: iface, Packets: batch}
		batch = nil
		return s.reporter.SendBatch(ctx, out)
	}
	for {
		var timerC <-chan time.Time
		if len(batch) > 0 {
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			_ = flush()
			return
		case packet, ok := <-packets:
			if !ok {
				_ = flush()
				return
			}
			if len(batch) == 0 {
				timer.Reset(s.batchMaxDelay)
			}
			batch = append(batch, packet)
			if len(batch) >= s.batchMaxPackets {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if err := flush(); err != nil {
					_ = s.reporter.SendStatus(context.Background(), sessionID, "FAILED", err.Error())
					return
				}
			}
		case <-timerC:
			if err := flush(); err != nil {
				_ = s.reporter.SendStatus(context.Background(), sessionID, "FAILED", err.Error())
				return
			}
		case err, ok := <-errorsChannel:
			if !ok {
				errorsChannel = nil
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				_ = s.reporter.SendStatus(context.Background(), sessionID, "FAILED", err.Error())
			}
			_ = flush()
			return
		}
	}
}

func (s *Service) remove(sessionID string) {
	s.mu.Lock()
	delete(s.captures, sessionID)
	s.mu.Unlock()
}

package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/interfacealias"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type StartRequest struct {
	SessionID        string
	LogicalInterface string
	Filter           string
	Snaplen          uint32
	Source           domain.CaptureSource
}

type Service struct {
	mu                sync.Mutex
	nodeName          string
	interfaces        map[string]string
	engine            ports.CaptureEngine
	reporter          ports.AgentReporter
	progress          ports.CaptureProgressReporter
	interfaceResolver ports.PodInterfaceResolver
	batchMaxPackets   int
	batchMaxDelay     time.Duration
	progressInterval  time.Duration
	captures          map[string]map[string]context.CancelFunc
}

func (s *Service) SetPodInterfaceResolver(resolver ports.PodInterfaceResolver) {
	s.interfaceResolver = resolver
}

func NewService(nodeName string, interfaces map[string]string, engine ports.CaptureEngine, reporter ports.AgentReporter, progress ports.CaptureProgressReporter, maxPackets int, maxDelay, progressInterval time.Duration) *Service {
	return &Service{nodeName: nodeName, interfaces: interfaces, engine: engine, reporter: reporter, progress: progress, batchMaxPackets: maxPackets, batchMaxDelay: maxDelay, progressInterval: progressInterval, captures: make(map[string]map[string]context.CancelFunc)}
}

func (s *Service) Start(parent context.Context, request StartRequest) error {
	source := request.Source
	logicalInterface := source.LogicalInterface
	if logicalInterface == "" {
		logicalInterface = request.LogicalInterface
	}
	iface := source.InterfaceName
	if iface == "" && source.PodIP != "" {
		if s.interfaceResolver == nil {
			return fmt.Errorf("Calico interface for pod IP %s is unresolved", source.PodIP)
		}
		var err error
		iface, err = s.interfaceResolver.Resolve(parent, source.PodIP)
		if err != nil {
			return fmt.Errorf("resolve interface for pod IP %s: %w", source.PodIP, err)
		}
	}
	if iface == "" {
		var err error
		logicalInterface, err = interfacealias.Normalize(logicalInterface)
		if err != nil {
			return err
		}
		var ok bool
		iface, ok = s.interfaces[logicalInterface]
		if !ok || iface == "" {
			return fmt.Errorf("logical interface %q is not configured", logicalInterface)
		}
	}
	if source.ID == "" {
		source.ID = "interface:" + logicalInterface
	}
	if source.TargetType == "" {
		source.TargetType = "interface"
	}
	source.NodeName = s.nodeName
	source.LogicalInterface = logicalInterface
	source.InterfaceName = iface
	s.mu.Lock()
	sources := s.captures[request.SessionID]
	if sources == nil {
		sources = make(map[string]context.CancelFunc)
		s.captures[request.SessionID] = sources
	}
	if _, exists := sources[source.ID]; exists {
		s.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	sources[source.ID] = cancel
	s.mu.Unlock()

	packets, captureErrors, err := s.engine.Capture(ctx, iface, request.Filter, request.Snaplen)
	if err != nil {
		s.remove(request.SessionID, source.ID)
		cancel()
		return err
	}
	go s.batch(ctx, request.SessionID, source, packets, captureErrors)
	return s.reporter.SendStatus(parent, request.SessionID, source.ID, "RUNNING", "")
}

func (s *Service) Stop(ctx context.Context, sessionID, sourceID string) error {
	s.mu.Lock()
	sources := s.captures[sessionID]
	var cancels []context.CancelFunc
	if sourceID == "" {
		for _, cancel := range sources {
			cancels = append(cancels, cancel)
		}
		delete(s.captures, sessionID)
	} else if cancel, exists := sources[sourceID]; exists {
		cancels = append(cancels, cancel)
		delete(sources, sourceID)
		if len(sources) == 0 {
			delete(s.captures, sessionID)
		}
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return s.reporter.SendStatus(ctx, sessionID, sourceID, "STOPPED", "")
}

func (s *Service) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sessionID, sources := range s.captures {
		for _, cancel := range sources {
			cancel()
		}
		delete(s.captures, sessionID)
	}
}

func (s *Service) batch(ctx context.Context, sessionID string, source domain.CaptureSource, packets <-chan domain.Packet, captureErrors <-chan error) {
	defer s.remove(sessionID, source.ID)
	startedAt := time.Now()
	lastReportAt := startedAt
	var packetsTotal, bytesTotal, packetsInterval, bytesInterval uint64
	reportProgress := func(final bool) {
		if s.progress == nil {
			return
		}
		now := time.Now()
		s.progress.ReportCaptureProgress(ports.CaptureProgress{
			SessionID:       sessionID,
			SourceID:        source.ID,
			NodeName:        s.nodeName,
			InterfaceName:   source.InterfaceName,
			PacketsTotal:    packetsTotal,
			BytesTotal:      bytesTotal,
			PacketsInterval: packetsInterval,
			BytesInterval:   bytesInterval,
			StartedAt:       startedAt,
			ObservedAt:      now,
			Interval:        now.Sub(lastReportAt),
			Final:           final,
		})
		packetsInterval = 0
		bytesInterval = 0
		lastReportAt = now
	}
	defer reportProgress(true)
	var progressTicker *time.Ticker
	var progressC <-chan time.Time
	if s.progress != nil && s.progressInterval > 0 {
		progressTicker = time.NewTicker(s.progressInterval)
		progressC = progressTicker.C
		defer progressTicker.Stop()
	}
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
		out := domain.PacketBatch{SessionID: sessionID, NodeName: s.nodeName, InterfaceName: source.InterfaceName, Source: source, Packets: batch}
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
			packetsTotal++
			bytesTotal += uint64(packet.CapturedLength)
			packetsInterval++
			bytesInterval += uint64(packet.CapturedLength)
			batch = append(batch, packet)
			if len(batch) >= s.batchMaxPackets {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if err := flush(); err != nil {
					_ = s.reporter.SendStatus(context.Background(), sessionID, source.ID, "FAILED", err.Error())
					return
				}
			}
		case <-timerC:
			if err := flush(); err != nil {
				_ = s.reporter.SendStatus(context.Background(), sessionID, source.ID, "FAILED", err.Error())
				return
			}
		case <-progressC:
			reportProgress(false)
		case err, ok := <-errorsChannel:
			if !ok {
				errorsChannel = nil
				continue
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				_ = s.reporter.SendStatus(context.Background(), sessionID, source.ID, "FAILED", err.Error())
			}
			_ = flush()
			return
		}
	}
}

func (s *Service) remove(sessionID, sourceID string) {
	s.mu.Lock()
	if sources := s.captures[sessionID]; sources != nil {
		delete(sources, sourceID)
		if len(sources) == 0 {
			delete(s.captures, sessionID)
		}
	}
	s.mu.Unlock()
}

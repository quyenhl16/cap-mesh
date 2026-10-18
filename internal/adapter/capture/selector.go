package capture

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

const (
	EngineDumpcap  = "dumpcap"
	EngineAFPacket = "afpacket"
	EngineAuto     = "auto"
)

type engineSelector struct {
	mode     string
	afpacket ports.CaptureEngine
	dumpcap  ports.CaptureEngine
	metrics  ports.CaptureEngineMetrics
}

func SelectEngine(mode string, afpacket, dumpcap ports.CaptureEngine, metrics ports.CaptureEngineMetrics) (ports.CaptureEngine, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case EngineDumpcap:
		if dumpcap == nil {
			return nil, errors.New("dumpcap capture engine is unavailable")
		}
	case EngineAFPacket:
		if afpacket == nil {
			return nil, errors.New("AF_PACKET capture engine is unavailable")
		}
	case EngineAuto:
		if afpacket == nil || dumpcap == nil {
			return nil, errors.New("auto capture engine requires AF_PACKET and dumpcap")
		}
	default:
		return nil, fmt.Errorf("unsupported capture engine %q (expected dumpcap, afpacket, or auto)", mode)
	}
	return &engineSelector{mode: mode, afpacket: afpacket, dumpcap: dumpcap, metrics: metrics}, nil
}

func (s *engineSelector) Capture(ctx context.Context, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	switch s.mode {
	case EngineDumpcap:
		return s.capture(ctx, EngineDumpcap, s.dumpcap, request)
	case EngineAFPacket:
		return s.capture(ctx, EngineAFPacket, s.afpacket, request)
	default:
		batches, captureErrors, err := s.afpacket.Capture(ctx, request)
		if err == nil {
			s.selected(EngineAFPacket)
			return batches, captureErrors, nil
		}
		if !fallbackEligible(err) {
			return nil, nil, err
		}
		if s.metrics != nil {
			s.metrics.CaptureEngineFallback(fallbackReason(err))
		}
		return s.capture(ctx, EngineDumpcap, s.dumpcap, request)
	}
}

func fallbackEligible(err error) bool {
	return errors.Is(err, ErrAFPacketUnsupported) ||
		errors.Is(err, ErrRingBudgetExceeded) ||
		errors.Is(err, ErrAFPacketInitialize)
}

func (s *engineSelector) capture(ctx context.Context, name string, engine ports.CaptureEngine, request ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	batches, captureErrors, err := engine.Capture(ctx, request)
	if err == nil {
		s.selected(name)
	}
	return batches, captureErrors, err
}

func (s *engineSelector) selected(engine string) {
	if s.metrics != nil {
		s.metrics.CaptureEngineSelected(engine)
	}
}

func fallbackReason(err error) string {
	switch {
	case errors.Is(err, ErrAFPacketUnsupported):
		return "unsupported"
	case errors.Is(err, ErrRingBudgetExceeded):
		return "ring_budget"
	case errors.Is(err, ErrAFPacketInitialize):
		return "initialization"
	default:
		return "other"
	}
}

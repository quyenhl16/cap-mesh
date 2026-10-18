package capture

import (
	"context"
	"errors"
	"testing"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type selectorEngine struct {
	err   error
	calls int
}

func (e *selectorEngine) Capture(context.Context, ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	e.calls++
	if e.err != nil {
		return nil, nil, e.err
	}
	batches := make(chan domain.CapturedBatch)
	errorsChannel := make(chan error)
	close(batches)
	close(errorsChannel)
	return batches, errorsChannel, nil
}

type selectorMetrics struct {
	selected []string
	fallback []string
}

func (m *selectorMetrics) CaptureEngineSelected(engine string) {
	m.selected = append(m.selected, engine)
}
func (m *selectorMetrics) CaptureEngineFallback(reason string) {
	m.fallback = append(m.fallback, reason)
}
func (*selectorMetrics) AFPacketRingOpened(int) {}
func (*selectorMetrics) AFPacketRingClosed(int) {}
func (*selectorMetrics) ObserveAFPacketStats(uint64, uint64, uint64, uint64) {
}

func TestAutoEngineFallsBackOnlyOnInitializationFailure(t *testing.T) {
	primary := &selectorEngine{err: ErrAFPacketUnsupported}
	fallback := &selectorEngine{}
	metrics := &selectorMetrics{}
	engine, err := SelectEngine(EngineAuto, primary, fallback, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Capture(context.Background(), ports.CaptureRequest{}); err != nil {
		t.Fatal(err)
	}
	if primary.calls != 1 || fallback.calls != 1 {
		t.Fatalf("calls primary=%d fallback=%d, want 1/1", primary.calls, fallback.calls)
	}
	if len(metrics.fallback) != 1 || metrics.fallback[0] != "unsupported" {
		t.Fatalf("fallback metrics = %v", metrics.fallback)
	}
	if len(metrics.selected) != 1 || metrics.selected[0] != EngineDumpcap {
		t.Fatalf("selected metrics = %v", metrics.selected)
	}
}

func TestStrictAFPacketDoesNotFallback(t *testing.T) {
	primaryError := errors.New("open failed")
	primary := &selectorEngine{err: primaryError}
	fallback := &selectorEngine{}
	engine, err := SelectEngine(EngineAFPacket, primary, fallback, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Capture(context.Background(), ports.CaptureRequest{}); !errors.Is(err, primaryError) {
		t.Fatalf("Capture error = %v, want %v", err, primaryError)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback calls = %d, want 0", fallback.calls)
	}
}

func TestAutoEngineDoesNotFallbackForInvalidFilter(t *testing.T) {
	primary := &selectorEngine{err: ErrFilterCompile}
	fallback := &selectorEngine{}
	engine, err := SelectEngine(EngineAuto, primary, fallback, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Capture(context.Background(), ports.CaptureRequest{}); !errors.Is(err, ErrFilterCompile) {
		t.Fatalf("Capture error = %v, want ErrFilterCompile", err)
	}
	if fallback.calls != 0 {
		t.Fatalf("fallback calls = %d, want 0", fallback.calls)
	}
}

func TestSelectEngineRejectsUnknownMode(t *testing.T) {
	if _, err := SelectEngine("unknown", &selectorEngine{}, &selectorEngine{}, nil); err == nil {
		t.Fatal("unknown engine unexpectedly accepted")
	}
}

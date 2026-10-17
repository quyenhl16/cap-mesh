package grpcclient

import (
	"context"
	"errors"
	"testing"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func TestReporterDropsBatchWhenBoundedQueueIsFull(t *testing.T) {
	reporter := NewReporter(1)
	reporter.setConnected(true)
	batch := domain.PacketBatch{Packets: []domain.Packet{{SequenceNumber: 1}}}
	if err := reporter.SendBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := reporter.SendBatch(context.Background(), batch); !errors.Is(err, ports.ErrQueueFull) {
		t.Fatalf("error = %v, want %v", err, ports.ErrQueueFull)
	}
	if got := len(reporter.outbound); got != 1 {
		t.Fatalf("queue depth = %d, want 1", got)
	}
}

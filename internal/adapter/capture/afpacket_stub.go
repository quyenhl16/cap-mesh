//go:build !linux

package capture

import (
	"context"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func (a *AFPacket) Capture(context.Context, ports.CaptureRequest) (<-chan domain.CapturedBatch, <-chan error, error) {
	return nil, nil, ErrAFPacketUnsupported
}

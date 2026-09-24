package logging

import (
	"log/slog"

	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type CaptureProgress struct {
	logger *slog.Logger
}

func NewCaptureProgress(logger *slog.Logger) *CaptureProgress {
	return &CaptureProgress{logger: logger}
}

func (r *CaptureProgress) ReportCaptureProgress(progress ports.CaptureProgress) {
	if r == nil || r.logger == nil {
		return
	}
	message := "capture progress"
	if progress.Final {
		message = "capture completed"
	}
	intervalSeconds := progress.Interval.Seconds()
	packetsPerSecond := float64(0)
	bytesPerSecond := float64(0)
	if intervalSeconds > 0 {
		packetsPerSecond = float64(progress.PacketsInterval) / intervalSeconds
		bytesPerSecond = float64(progress.BytesInterval) / intervalSeconds
	}
	r.logger.Info(
		message,
		"node", progress.NodeName,
		"session_id", progress.SessionID,
		"interface", progress.InterfaceName,
		"packets_total", progress.PacketsTotal,
		"bytes_total", progress.BytesTotal,
		"packets_interval", progress.PacketsInterval,
		"bytes_interval", progress.BytesInterval,
		"packets_per_second", packetsPerSecond,
		"bytes_per_second", bytesPerSecond,
		"elapsed", progress.ObservedAt.Sub(progress.StartedAt),
		"final", progress.Final,
	)
}

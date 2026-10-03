package domain

import "time"

type WorkloadLogTarget struct {
	Namespace  string
	Kind       string
	Name       string
	Containers []string
	MaxPods    uint32
}

type WorkloadPod struct {
	Target       WorkloadLogTarget
	Name         string
	UID          string
	Containers   []string
	RestartCount map[string]int32
}

type PodLogRequest struct {
	Namespace string
	PodName   string
	Container string
	SinceTime time.Time
}

type LogRecord struct {
	Target       WorkloadLogTarget
	PodName      string
	PodUID       string
	Container    string
	RestartCount int32
	ReceivedAt   time.Time
	Data         []byte
}

type LogCaptureRun struct {
	RunID           string
	StartedAt       time.Time
	Targets         []WorkloadLogTarget
	SinceSeconds    uint32
	SegmentSize     int64
	MaxRetainedSize int64
}

type LogRecordingUsage struct {
	RetainedSize int64
	SegmentCount int
}

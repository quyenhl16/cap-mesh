package domain

import "time"

type SessionRecordingInfo struct {
	SessionID    string
	Directory    string
	SizeBytes    int64
	SegmentCount int
	CreatedAt    time.Time
	FinishedAt   time.Time
	Status       string
	DesiredState string
	Active       bool
	Deletable    bool
}

type SessionRecordingCatalog struct {
	Recordings    []SessionRecordingInfo
	TotalSize     int64
	ConfiguredMax int64
}

type RecordingCapacity struct {
	UsedSize          int64
	MaxSize           int64
	DeletableSize     int64
	DeletableSessions int
}

type RecordingCleanupFailure struct {
	SessionID string
	Directory string
	Error     string
}

type RecordingCleanupResult struct {
	ConfiguredMax int64
	TargetSize    int64
	SizeBefore    int64
	SizeAfter     int64
	DeletedSize   int64
	Deleted       []SessionRecordingInfo
	Failures      []RecordingCleanupFailure
	DryRun        bool
}

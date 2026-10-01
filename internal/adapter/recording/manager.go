package recording

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/adapter/pcapng"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Config struct {
	Directory      string
	SegmentSize    int64
	MaxSessionSize int64
	QueueSize      int
	FlushInterval  time.Duration
	SyncInterval   time.Duration
}

type Manager struct {
	config     Config
	packets    ports.PacketPublisher
	logger     *slog.Logger
	metrics    *Metrics
	mu         sync.Mutex
	active     map[string]*sessionRecorder
	completed  map[string]domain.RecordingUsage
	onComplete func(string, string, string)
	closing    bool
	wg         sync.WaitGroup
}

type metadata struct {
	SchemaVersion    int                    `json:"schema_version"`
	SessionID        string                 `json:"session_id"`
	Status           string                 `json:"status"`
	Nodes            []string               `json:"nodes"`
	InterfaceAlias   string                 `json:"interface_alias"`
	Targets          []domain.CaptureTarget `json:"targets,omitempty"`
	Filter           string                 `json:"filter,omitempty"`
	Snaplen          uint32                 `json:"snaplen"`
	StartedAt        time.Time              `json:"started_at"`
	FinishedAt       *time.Time             `json:"finished_at,omitempty"`
	SegmentSizeLimit int64                  `json:"segment_size_limit"`
	SessionSizeLimit int64                  `json:"session_size_limit"`
	TotalFileSize    int64                  `json:"total_file_size"`
	PacketCount      uint64                 `json:"packet_count"`
	CapturedBytes    uint64                 `json:"captured_bytes"`
	Truncated        bool                   `json:"truncated"`
	Error            string                 `json:"error,omitempty"`
	Rolling          bool                   `json:"rolling,omitempty"`
	RunID            string                 `json:"run_id,omitempty"`
	Segments         []segmentMetadata      `json:"segments"`
}

type segmentMetadata struct {
	Sequence      int       `json:"sequence"`
	File          string    `json:"file"`
	Size          int64     `json:"size"`
	PacketCount   uint64    `json:"packet_count"`
	CapturedBytes uint64    `json:"captured_bytes"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
}

type sessionRecorder struct {
	config       Config
	sessionDir   string
	subscription ports.PacketSubscription
	context      context.Context
	cancel       context.CancelFunc
	logger       *slog.Logger
	metrics      *Metrics
	metadata     metadata
	sequence     int
	file         *os.File
	counter      *countingWriter
	writer       *pcapng.Writer
	segmentStart time.Time
	segmentPkts  uint64
	segmentBytes uint64
	rolling      bool
	runID        string
	retainedSize atomic.Int64
	segmentCount atomic.Int64
	finalStatus  string
	finalError   string
}

type countingWriter struct {
	file *os.File
	n    int64
}

func (w *countingWriter) Write(value []byte) (int, error) {
	n, err := w.file.Write(value)
	w.n += int64(n)
	return n, err
}

var safeSessionID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func NewManager(config Config, packets ports.PacketPublisher, logger *slog.Logger, metrics *Metrics) (*Manager, error) {
	if config.Directory == "" {
		return nil, errors.New("record directory is required")
	}
	if config.SegmentSize < 0 || config.MaxSessionSize < 0 {
		return nil, errors.New("recording size limits must not be negative")
	}
	if config.QueueSize < 1 {
		return nil, errors.New("record queue size must be positive")
	}
	if config.FlushInterval <= 0 || config.SyncInterval <= 0 {
		return nil, errors.New("record flush and sync intervals must be positive")
	}
	root, err := filepath.Abs(config.Directory)
	if err != nil {
		return nil, fmt.Errorf("resolve record directory: %w", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create record directory: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	config.Directory = root
	return &Manager{config: config, packets: packets, logger: logger, metrics: metrics, active: make(map[string]*sessionRecorder), completed: make(map[string]domain.RecordingUsage)}, nil
}

func (m *Manager) OnComplete(handler func(string, string, string)) {
	m.mu.Lock()
	m.onComplete = handler
	m.mu.Unlock()
}

func (m *Manager) Usage(sessionID string) (domain.RecordingUsage, bool) {
	m.mu.Lock()
	recorder, active := m.active[sessionID]
	usage, completed := m.completed[sessionID]
	m.mu.Unlock()
	if active && recorder != nil {
		return recorder.usage(), true
	}
	return usage, completed
}

func (m *Manager) Start(session domain.Session) error {
	if !safeSessionID.MatchString(session.ID) {
		return fmt.Errorf("unsafe session ID %q", session.ID)
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return errors.New("recording manager is shutting down")
	}
	if _, exists := m.active[session.ID]; exists {
		m.mu.Unlock()
		return ports.ErrAlreadyExists
	}
	// Reserve the ID while filesystem and subscription setup is in progress.
	m.active[session.ID] = nil
	m.mu.Unlock()

	fail := func(err error) error {
		m.mu.Lock()
		delete(m.active, session.ID)
		m.mu.Unlock()
		return err
	}
	rolling := session.Mode == domain.SessionModeContinuous
	var sessionDir string
	if rolling {
		if m.config.SegmentSize <= 0 || m.config.MaxSessionSize <= 0 {
			return fail(errors.New("continuous recording requires positive segment and maximum sizes"))
		}
		if m.config.SegmentSize > m.config.MaxSessionSize {
			return fail(errors.New("continuous recording segment size must not exceed maximum size"))
		}
		sessionDir = filepath.Join(m.config.Directory, "continuous")
		if err := os.MkdirAll(sessionDir, 0o750); err != nil {
			return fail(fmt.Errorf("create continuous recording directory: %w", err))
		}
	} else {
		dateDir := filepath.Join(m.config.Directory, session.CreatedAt.UTC().Format("2006-01-02"))
		if err := os.MkdirAll(dateDir, 0o750); err != nil {
			return fail(fmt.Errorf("create recording date directory: %w", err))
		}
		sessionDir = filepath.Join(dateDir, session.ID)
		if err := os.Mkdir(sessionDir, 0o750); err != nil {
			return fail(fmt.Errorf("create recording session directory: %w", err))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := m.packets.SubscribeLossAware(ctx, session.ID, m.config.QueueSize)
	if err != nil {
		cancel()
		return fail(fmt.Errorf("subscribe recorder: %w", err))
	}
	recorder := &sessionRecorder{
		config:       m.config,
		sessionDir:   sessionDir,
		subscription: subscription,
		context:      ctx,
		cancel:       cancel,
		logger:       m.logger,
		metrics:      m.metrics,
		rolling:      rolling,
		runID:        session.CreatedAt.UTC().Format("20060102T150405.000000000Z") + "-" + session.ID,
		metadata: metadata{
			SchemaVersion:    1,
			SessionID:        session.ID,
			Status:           "RECORDING",
			Nodes:            append([]string(nil), session.Nodes...),
			InterfaceAlias:   session.LogicalInterface,
			Targets:          session.Targets,
			Filter:           session.Filter,
			Snaplen:          session.Snaplen,
			StartedAt:        session.CreatedAt.UTC(),
			SegmentSizeLimit: m.config.SegmentSize,
			SessionSizeLimit: m.config.MaxSessionSize,
			Segments:         make([]segmentMetadata, 0),
			Rolling:          rolling,
		},
	}
	recorder.metadata.RunID = recorder.runID
	if rolling {
		if err := recorder.loadRollingSegments(); err != nil {
			cancel()
			return fail(err)
		}
		if err := recorder.pruneOldest(m.config.SegmentSize); err != nil {
			cancel()
			return fail(err)
		}
	}
	if err := recorder.writeMetadata(false); err != nil {
		cancel()
		return fail(err)
	}
	if err := recorder.openSegment(); err != nil {
		cancel()
		return fail(err)
	}

	m.mu.Lock()
	m.active[session.ID] = recorder
	if rolling {
		// The application exposes only the singleton's current or most recent run.
		// Discard older usage snapshots so repeated restarts cannot grow this map.
		clear(m.completed)
	} else {
		delete(m.completed, session.ID)
	}
	m.wg.Add(1)
	m.mu.Unlock()
	if m.metrics != nil {
		m.metrics.active.Inc()
	}
	go func() {
		defer m.wg.Done()
		recorder.run()
		usage := recorder.usage()
		m.mu.Lock()
		delete(m.active, session.ID)
		if recorder.rolling {
			m.completed[session.ID] = usage
		}
		handler := m.onComplete
		m.mu.Unlock()
		if handler != nil {
			handler(session.ID, recorder.finalStatus, recorder.finalError)
		}
	}()
	m.logger.Info("capture recording started", "session_id", session.ID, "directory", sessionDir)
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closing = true
	for _, recorder := range m.active {
		if recorder != nil {
			recorder.cancel()
		}
	}
	m.mu.Unlock()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *sessionRecorder) run() {
	defer r.cancel()
	flushTicker := time.NewTicker(r.config.FlushInterval)
	syncTicker := time.NewTicker(r.config.SyncInterval)
	defer flushTicker.Stop()
	defer syncTicker.Stop()
	dropped := false
	dropCounted := false
	markDropped := func() {
		dropped = true
		if !dropCounted && r.metrics != nil {
			r.metrics.queueDrops.Inc()
			dropCounted = true
		}
	}
	droppedChannel := r.subscription.Dropped
	for {
		select {
		case batch, ok := <-r.subscription.Batches:
			if !ok {
				if r.context.Err() != nil {
					r.complete("PARTIAL", "recording interrupted by server shutdown", true)
					return
				}
				if !dropped && droppedChannel != nil {
					select {
					case <-droppedChannel:
						markDropped()
					default:
					}
				}
				if dropped {
					r.complete("PARTIAL", "recording queue overflow", true)
				} else {
					r.complete("COMPLETED", "", false)
				}
				return
			}
			if err := r.writeBatch(batch); err != nil {
				r.complete("FAILED", err.Error(), true)
				return
			}
			if !r.rolling && r.reachedSessionLimit() {
				r.metadata.Truncated = true
				r.complete("TRUNCATED", "max_session_size_reached", false)
				return
			}
			if r.reachedSegmentLimit() {
				if err := r.rotate(); err != nil {
					r.complete("FAILED", err.Error(), true)
					return
				}
			}
		case <-droppedChannel:
			markDropped()
			droppedChannel = nil
		case <-flushTicker.C:
			if err := r.flush(); err != nil {
				r.complete("FAILED", err.Error(), true)
				return
			}
			if !r.rolling && r.reachedSessionLimit() {
				r.metadata.Truncated = true
				r.complete("TRUNCATED", "max_session_size_reached", false)
				return
			}
			if r.reachedSegmentLimit() {
				if err := r.rotate(); err != nil {
					r.complete("FAILED", err.Error(), true)
					return
				}
			}
		case <-syncTicker.C:
			if err := r.sync(); err != nil {
				r.complete("FAILED", err.Error(), true)
				return
			}
		case <-r.context.Done():
			r.complete("PARTIAL", "recording interrupted by server shutdown", true)
			return
		}
	}
}

func (r *sessionRecorder) writeBatch(batch domain.PacketBatch) error {
	if err := r.writer.WriteBatch(batch); err != nil {
		return err
	}
	var capturedBytes uint64
	for _, packet := range batch.Packets {
		r.segmentPkts++
		r.metadata.PacketCount++
		r.segmentBytes += uint64(packet.CapturedLength)
		r.metadata.CapturedBytes += uint64(packet.CapturedLength)
		capturedBytes += uint64(packet.CapturedLength)
	}
	if r.metrics != nil {
		r.metrics.packets.Add(float64(len(batch.Packets)))
		r.metrics.capturedBytes.Add(float64(capturedBytes))
	}
	return nil
}

func (r *sessionRecorder) openSegment() error {
	r.sequence++
	path := filepath.Join(r.sessionDir, r.segmentName(true, false))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("create recording segment: %w", err)
	}
	r.file = file
	r.counter = &countingWriter{file: file}
	r.writer = pcapng.NewWriter(r.counter, 4096, r.config.FlushInterval)
	r.segmentStart = time.Now().UTC()
	r.segmentPkts = 0
	r.segmentBytes = 0
	return nil
}

func (r *sessionRecorder) flush() error {
	if r.writer == nil {
		return nil
	}
	return r.writer.Flush()
}

func (r *sessionRecorder) sync() error {
	if err := r.flush(); err != nil {
		return err
	}
	if r.file != nil {
		return r.file.Sync()
	}
	return nil
}

func (r *sessionRecorder) reachedSegmentLimit() bool {
	return r.config.SegmentSize > 0 && r.writer != nil && r.writer.BytesWritten() >= r.config.SegmentSize
}

func (r *sessionRecorder) reachedSessionLimit() bool {
	return r.config.MaxSessionSize > 0 && r.writer != nil && r.metadata.TotalFileSize+r.writer.BytesWritten() >= r.config.MaxSessionSize
}

func (r *sessionRecorder) rotate() error {
	if err := r.finishSegment(false); err != nil {
		return err
	}
	if r.rolling {
		if err := r.pruneOldest(r.config.SegmentSize); err != nil {
			return err
		}
	}
	if err := r.writeMetadata(false); err != nil {
		return err
	}
	return r.openSegment()
}

func (r *sessionRecorder) finishSegment(partial bool) error {
	if r.file == nil {
		return nil
	}
	partPath := r.file.Name()
	if r.segmentPkts == 0 {
		_ = r.file.Close()
		r.file = nil
		r.writer = nil
		r.counter = nil
		if err := os.Remove(partPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove empty recording segment: %w", err)
		}
		return nil
	}
	if err := r.writer.Flush(); err != nil {
		_ = r.file.Close()
		return err
	}
	if err := r.file.Sync(); err != nil {
		_ = r.file.Close()
		return fmt.Errorf("sync recording segment: %w", err)
	}
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close recording segment: %w", err)
	}
	finishedAt := time.Now().UTC()
	name := r.segmentName(false, partial)
	finalPath := filepath.Join(r.sessionDir, name)
	if err := os.Rename(partPath, finalPath); err != nil {
		return fmt.Errorf("finalize recording segment: %w", err)
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		return fmt.Errorf("stat recording segment: %w", err)
	}
	r.metadata.TotalFileSize += info.Size()
	r.metadata.Segments = append(r.metadata.Segments, segmentMetadata{
		Sequence:      r.sequence,
		File:          name,
		Size:          info.Size(),
		PacketCount:   r.segmentPkts,
		CapturedBytes: r.segmentBytes,
		StartedAt:     r.segmentStart,
		FinishedAt:    finishedAt,
	})
	if r.metrics != nil {
		r.metrics.segments.Inc()
		r.metrics.fileBytes.Add(float64(info.Size()))
	}
	r.retainedSize.Store(r.metadata.TotalFileSize)
	r.segmentCount.Store(int64(len(r.metadata.Segments)))
	r.file = nil
	r.writer = nil
	r.counter = nil
	return nil
}

func (r *sessionRecorder) complete(status, message string, partial bool) {
	if err := r.finishSegment(partial); err != nil {
		status = "FAILED"
		if message == "" {
			message = err.Error()
		} else {
			message += "; " + err.Error()
		}
	}
	if r.rolling {
		if err := r.pruneOldest(0); err != nil {
			status = "FAILED"
			if message == "" {
				message = err.Error()
			} else {
				message += "; " + err.Error()
			}
		}
	}
	finishedAt := time.Now().UTC()
	r.metadata.Status = status
	r.metadata.Error = message
	r.metadata.FinishedAt = &finishedAt
	r.finalStatus = status
	r.finalError = message
	if err := r.writeMetadata(true); err != nil {
		r.logger.Error("finalize recording metadata failed", "session_id", r.metadata.SessionID, "error", err)
	}
	if r.metrics != nil {
		r.metrics.active.Dec()
		r.metrics.completed.WithLabelValues(status).Inc()
	}
	if status == "COMPLETED" {
		r.logger.Info("capture recording completed", "session_id", r.metadata.SessionID, "segments", len(r.metadata.Segments), "file_size", r.metadata.TotalFileSize, "packets", r.metadata.PacketCount)
	} else {
		r.logger.Warn("capture recording ended", "session_id", r.metadata.SessionID, "status", status, "error", message, "segments", len(r.metadata.Segments), "file_size", r.metadata.TotalFileSize)
	}
}

func (r *sessionRecorder) usage() domain.RecordingUsage {
	return domain.RecordingUsage{RetainedSize: r.retainedSize.Load(), SegmentCount: int(r.segmentCount.Load())}
}

func (r *sessionRecorder) segmentName(part, partial bool) string {
	base := fmt.Sprintf("capture-%06d", r.sequence)
	if r.rolling {
		base = fmt.Sprintf("trace-%s-%06d", r.runID, r.sequence)
	}
	if partial {
		base += ".partial"
	}
	name := base + ".pcapng"
	if part {
		name += ".part"
	}
	return name
}

func (r *sessionRecorder) loadRollingSegments() error {
	entries, err := os.ReadDir(r.sessionDir)
	if err != nil {
		return fmt.Errorf("scan continuous recording directory: %w", err)
	}
	var segments []segmentMetadata
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && strings.HasPrefix(name, "trace-") && strings.HasSuffix(name, ".pcapng.part") {
			if err := os.Remove(filepath.Join(r.sessionDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove stale continuous segment %s: %w", name, err)
			}
			continue
		}
		if entry.IsDir() || !strings.HasPrefix(name, "trace-") || !strings.HasSuffix(name, ".pcapng") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stat continuous segment %s: %w", name, err)
		}
		segments = append(segments, segmentMetadata{File: name, Size: info.Size(), StartedAt: info.ModTime().UTC(), FinishedAt: info.ModTime().UTC()})
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].FinishedAt.Equal(segments[j].FinishedAt) {
			return segments[i].File < segments[j].File
		}
		return segments[i].FinishedAt.Before(segments[j].FinishedAt)
	})
	for index := range segments {
		segments[index].Sequence = index + 1
		r.metadata.TotalFileSize += segments[index].Size
	}
	r.metadata.Segments = segments
	r.retainedSize.Store(r.metadata.TotalFileSize)
	r.segmentCount.Store(int64(len(segments)))
	return nil
}

func (r *sessionRecorder) pruneOldest(reserve int64) error {
	if !r.rolling || r.config.MaxSessionSize <= 0 {
		return nil
	}
	for r.metadata.TotalFileSize+reserve > r.config.MaxSessionSize && len(r.metadata.Segments) > 0 {
		oldest := r.metadata.Segments[0]
		path := filepath.Join(r.sessionDir, oldest.File)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete oldest continuous segment %s: %w", oldest.File, err)
		}
		r.metadata.TotalFileSize -= oldest.Size
		r.metadata.Segments = r.metadata.Segments[1:]
		if r.metrics != nil {
			r.metrics.deletedSegments.Inc()
			r.metrics.deletedBytes.Add(float64(oldest.Size))
		}
	}
	r.retainedSize.Store(r.metadata.TotalFileSize)
	r.segmentCount.Store(int64(len(r.metadata.Segments)))
	return nil
}

func (r *sessionRecorder) writeMetadata(final bool) error {
	data, err := json.MarshalIndent(r.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode recording metadata: %w", err)
	}
	partPath := filepath.Join(r.sessionDir, "metadata.json.part")
	if err := os.WriteFile(partPath, append(data, '\n'), 0o640); err != nil {
		return fmt.Errorf("write recording metadata: %w", err)
	}
	if final || r.rolling {
		finalPath := filepath.Join(r.sessionDir, "metadata.json")
		if r.rolling {
			if err := os.Remove(finalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("replace recording metadata: %w", err)
			}
		}
		if err := os.Rename(partPath, finalPath); err != nil {
			return fmt.Errorf("finalize recording metadata: %w", err)
		}
	}
	return nil
}

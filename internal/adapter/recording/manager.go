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
	"sync"
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
	config  Config
	packets ports.PacketPublisher
	logger  *slog.Logger
	metrics *Metrics
	mu      sync.Mutex
	active  map[string]context.CancelFunc
	closing bool
	wg      sync.WaitGroup
}

type metadata struct {
	SchemaVersion    int               `json:"schema_version"`
	SessionID        string            `json:"session_id"`
	Status           string            `json:"status"`
	Nodes            []string          `json:"nodes"`
	InterfaceAlias   string            `json:"interface_alias"`
	Filter           string            `json:"filter,omitempty"`
	Snaplen          uint32            `json:"snaplen"`
	StartedAt        time.Time         `json:"started_at"`
	FinishedAt       *time.Time        `json:"finished_at,omitempty"`
	SegmentSizeLimit int64             `json:"segment_size_limit"`
	SessionSizeLimit int64             `json:"session_size_limit"`
	TotalFileSize    int64             `json:"total_file_size"`
	PacketCount      uint64            `json:"packet_count"`
	CapturedBytes    uint64            `json:"captured_bytes"`
	Truncated        bool              `json:"truncated"`
	Error            string            `json:"error,omitempty"`
	Segments         []segmentMetadata `json:"segments"`
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
	return &Manager{config: config, packets: packets, logger: logger, metrics: metrics, active: make(map[string]context.CancelFunc)}, nil
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
	dateDir := filepath.Join(m.config.Directory, session.CreatedAt.UTC().Format("2006-01-02"))
	if err := os.MkdirAll(dateDir, 0o750); err != nil {
		return fail(fmt.Errorf("create recording date directory: %w", err))
	}
	sessionDir := filepath.Join(dateDir, session.ID)
	if err := os.Mkdir(sessionDir, 0o750); err != nil {
		return fail(fmt.Errorf("create recording session directory: %w", err))
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
		metadata: metadata{
			SchemaVersion:    1,
			SessionID:        session.ID,
			Status:           "RECORDING",
			Nodes:            append([]string(nil), session.Nodes...),
			InterfaceAlias:   session.LogicalInterface,
			Filter:           session.Filter,
			Snaplen:          session.Snaplen,
			StartedAt:        session.CreatedAt.UTC(),
			SegmentSizeLimit: m.config.SegmentSize,
			SessionSizeLimit: m.config.MaxSessionSize,
			Segments:         make([]segmentMetadata, 0),
		},
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
	m.active[session.ID] = cancel
	m.wg.Add(1)
	m.mu.Unlock()
	if m.metrics != nil {
		m.metrics.active.Inc()
	}
	go func() {
		defer m.wg.Done()
		recorder.run()
		m.mu.Lock()
		delete(m.active, session.ID)
		m.mu.Unlock()
	}()
	m.logger.Info("capture recording started", "session_id", session.ID, "directory", sessionDir)
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closing = true
	for _, cancel := range m.active {
		if cancel != nil {
			cancel()
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
			if r.reachedSessionLimit() {
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
			if r.reachedSessionLimit() {
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
	path := filepath.Join(r.sessionDir, fmt.Sprintf("capture-%06d.pcapng.part", r.sequence))
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
	name := fmt.Sprintf("capture-%06d.pcapng", r.sequence)
	if partial {
		name = fmt.Sprintf("capture-%06d.partial.pcapng", r.sequence)
	}
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
	finishedAt := time.Now().UTC()
	r.metadata.Status = status
	r.metadata.Error = message
	r.metadata.FinishedAt = &finishedAt
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

func (r *sessionRecorder) writeMetadata(final bool) error {
	data, err := json.MarshalIndent(r.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode recording metadata: %w", err)
	}
	partPath := filepath.Join(r.sessionDir, "metadata.json.part")
	if err := os.WriteFile(partPath, append(data, '\n'), 0o640); err != nil {
		return fmt.Errorf("write recording metadata: %w", err)
	}
	if final {
		if err := os.Rename(partPath, filepath.Join(r.sessionDir, "metadata.json")); err != nil {
			return fmt.Errorf("finalize recording metadata: %w", err)
		}
	}
	return nil
}

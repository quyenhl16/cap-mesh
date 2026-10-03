package logrecording

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type Config struct {
	Directory       string
	SegmentSize     int64
	MaxRetainedSize int64
	SyncInterval    time.Duration
}

type Factory struct {
	config  Config
	logger  *slog.Logger
	metrics *Metrics
}

type segment struct {
	path       string
	size       int64
	finishedAt time.Time
}

type streamWriter struct {
	key      string
	dir      string
	sequence int
	file     *os.File
	size     int64
	openedAt time.Time
}

type recorder struct {
	mu        sync.Mutex
	config    Config
	root      string
	runDir    string
	run       domain.LogCaptureRun
	logger    *slog.Logger
	metrics   *Metrics
	writers   map[string]*streamWriter
	finalized []segment
	totalSize int64
	closed    bool
	stopSync  chan struct{}
	doneSync  chan struct{}
	metadata  metadata
}

type metadata struct {
	SchemaVersion int                        `json:"schema_version"`
	RunID         string                     `json:"run_id"`
	Status        string                     `json:"status"`
	StartedAt     time.Time                  `json:"started_at"`
	FinishedAt    *time.Time                 `json:"finished_at,omitempty"`
	Targets       []domain.WorkloadLogTarget `json:"targets"`
	SegmentSize   int64                      `json:"segment_size_limit"`
	MaxSize       int64                      `json:"max_retained_size"`
	RetainedSize  int64                      `json:"retained_size"`
	SegmentCount  int                        `json:"segment_count"`
	Error         string                     `json:"error,omitempty"`
}

const logRecoveryFile = "log-recovery.json"

type recoveryMetadata struct {
	SchemaVersion int                  `json:"schema_version"`
	DesiredState  string               `json:"desired_state"`
	Generation    uint64               `json:"generation"`
	Run           domain.LogCaptureRun `json:"run"`
}

var unsafePath = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func NewFactory(config Config, logger *slog.Logger, metrics *Metrics) (*Factory, error) {
	if config.Directory == "" {
		return nil, errors.New("log recording directory is required")
	}
	if config.SegmentSize <= 0 || config.MaxRetainedSize <= 0 {
		return nil, errors.New("log segment and retained size limits must be positive")
	}
	if config.SegmentSize > config.MaxRetainedSize {
		return nil, errors.New("log segment size must not exceed retained size")
	}
	if config.SyncInterval <= 0 {
		return nil, errors.New("log sync interval must be positive")
	}
	root, err := filepath.Abs(filepath.Join(config.Directory, "logs", "continuous"))
	if err != nil {
		return nil, fmt.Errorf("resolve log directory: %w", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	config.Directory = root
	if logger == nil {
		logger = slog.Default()
	}
	return &Factory{config: config, logger: logger, metrics: metrics}, nil
}

func (f *Factory) RecoverableRuns() ([]domain.LogCaptureRun, error) {
	entries, err := os.ReadDir(f.config.Directory)
	if err != nil {
		return nil, err
	}
	var runs []domain.LogCaptureRun
	var readErr error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(f.config.Directory, entry.Name(), logRecoveryFile)
		state, err := readRecovery(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			readErr = errors.Join(readErr, fmt.Errorf("read %s: %w", path, err))
			continue
		}
		if state.DesiredState == "RUNNING" && state.Run.RunID != "" {
			runs = append(runs, state.Run)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].StartedAt.After(runs[j].StartedAt) })
	return runs, readErr
}

func (f *Factory) SetRunDesiredState(runID, desired string) error {
	if desired != "RUNNING" && desired != "STOPPED" {
		return fmt.Errorf("invalid desired state %q", desired)
	}
	path := filepath.Join(f.config.Directory, safe(runID), logRecoveryFile)
	state, err := readRecovery(path)
	if err != nil {
		return err
	}
	state.DesiredState = desired
	state.Generation++
	return writeRecovery(path, state)
}

func (f *Factory) Start(run domain.LogCaptureRun) (ports.WorkloadLogRecorder, error) {
	runDir := filepath.Join(f.config.Directory, safe(run.RunID))
	if err := os.MkdirAll(runDir, 0o750); err != nil {
		return nil, fmt.Errorf("create log capture run directory: %w", err)
	}
	r := &recorder{
		config: f.config, root: f.config.Directory, runDir: runDir, run: run,
		logger: f.logger, metrics: f.metrics, writers: make(map[string]*streamWriter),
		stopSync: make(chan struct{}), doneSync: make(chan struct{}),
		metadata: metadata{SchemaVersion: 1, RunID: run.RunID, Status: "RECORDING", StartedAt: run.StartedAt.UTC(), Targets: run.Targets, SegmentSize: f.config.SegmentSize, MaxSize: f.config.MaxRetainedSize},
	}
	if err := r.scan(); err != nil {
		return nil, err
	}
	if err := r.prune(0); err != nil {
		return nil, err
	}
	if err := r.writeMetadata(); err != nil {
		return nil, err
	}
	recoveryPath := filepath.Join(runDir, logRecoveryFile)
	state := recoveryMetadata{SchemaVersion: 1, DesiredState: "RUNNING", Generation: 1, Run: run}
	if previous, readErr := readRecovery(recoveryPath); readErr == nil {
		state.Generation = previous.Generation + 1
	}
	if err := writeRecovery(recoveryPath, state); err != nil {
		return nil, fmt.Errorf("persist workload log recovery state: %w", err)
	}
	if r.metrics != nil {
		r.metrics.SetActive(true)
	}
	go r.syncLoop()
	return r, nil
}

func readRecovery(path string) (recoveryMetadata, error) {
	var best recoveryMetadata
	found := false
	var readErr error
	for _, candidate := range []string{path, path + ".part"} {
		data, err := os.ReadFile(candidate)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			readErr = errors.Join(readErr, err)
			continue
		}
		var state recoveryMetadata
		if err := json.Unmarshal(data, &state); err != nil {
			readErr = errors.Join(readErr, fmt.Errorf("decode %s: %w", candidate, err))
			continue
		}
		if !found || state.Generation > best.Generation {
			best = state
			found = true
		}
	}
	if found {
		return best, nil
	}
	if readErr != nil {
		return recoveryMetadata{}, readErr
	}
	return recoveryMetadata{}, os.ErrNotExist
}

func writeRecovery(path string, state recoveryMetadata) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	part := path + ".part"
	if err := os.WriteFile(part, append(data, '\n'), 0o640); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(part, path)
}

func (r *recorder) Write(record domain.LogRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("log recorder is closed")
	}
	key := streamKey(record)
	writer := r.writers[key]
	if writer == nil {
		writer = &streamWriter{key: key, dir: r.recordDirectory(record)}
		r.writers[key] = writer
	}
	if writer.file != nil && writer.size > 0 && writer.size+int64(len(record.Data)) > r.config.SegmentSize {
		if err := r.finish(writer); err != nil {
			return err
		}
	}
	if err := r.prune(int64(len(record.Data))); err != nil {
		return err
	}
	if writer.file == nil {
		if err := r.open(writer); err != nil {
			return err
		}
	}
	n, err := writer.file.Write(record.Data)
	writer.size += int64(n)
	r.totalSize += int64(n)
	if r.metrics != nil {
		r.metrics.lines.Inc()
		r.metrics.bytes.Add(float64(n))
	}
	if err != nil {
		return fmt.Errorf("write log segment: %w", err)
	}
	if n != len(record.Data) {
		return errors.New("short write to log segment")
	}
	if writer.size >= r.config.SegmentSize {
		if err := r.finish(writer); err != nil {
			return err
		}
		return r.prune(0)
	}
	return nil
}

func (r *recorder) Usage() domain.LogRecordingUsage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return domain.LogRecordingUsage{RetainedSize: r.totalSize, SegmentCount: len(r.finalized) + activeSegmentCount(r.writers)}
}

func (r *recorder) Close(status, message string) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	close(r.stopSync)
	<-r.doneSync
	r.mu.Lock()
	defer r.mu.Unlock()
	var closeErr error
	for _, writer := range r.writers {
		if err := r.finish(writer); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	if err := r.prune(0); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	finished := time.Now().UTC()
	r.metadata.Status = status
	r.metadata.Error = message
	r.metadata.FinishedAt = &finished
	if closeErr != nil {
		r.metadata.Status = "FAILED"
		if r.metadata.Error == "" {
			r.metadata.Error = closeErr.Error()
		}
	}
	if err := r.writeMetadata(); err != nil {
		closeErr = errors.Join(closeErr, err)
	}
	if r.metrics != nil {
		r.metrics.SetActive(false)
	}
	return closeErr
}

func (r *recorder) open(writer *streamWriter) error {
	if err := os.MkdirAll(writer.dir, 0o750); err != nil {
		return fmt.Errorf("create log stream directory: %w", err)
	}
	if writer.sequence == 0 {
		entries, err := os.ReadDir(writer.dir)
		if err != nil {
			return fmt.Errorf("scan log stream directory: %w", err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasPrefix(name, "log-") || !strings.HasSuffix(name, ".log") {
				continue
			}
			value := strings.TrimSuffix(strings.TrimPrefix(name, "log-"), ".log")
			if sequence, parseErr := strconv.Atoi(value); parseErr == nil && sequence > writer.sequence {
				writer.sequence = sequence
			}
		}
	}
	writer.sequence++
	path := filepath.Join(writer.dir, fmt.Sprintf("log-%06d.log.part", writer.sequence))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("create log segment: %w", err)
	}
	writer.file = file
	writer.size = 0
	writer.openedAt = time.Now().UTC()
	return nil
}

func (r *recorder) finish(writer *streamWriter) error {
	if writer.file == nil {
		return nil
	}
	path := writer.file.Name()
	if err := writer.file.Sync(); err != nil {
		_ = writer.file.Close()
		return fmt.Errorf("sync log segment: %w", err)
	}
	if err := writer.file.Close(); err != nil {
		return fmt.Errorf("close log segment: %w", err)
	}
	writer.file = nil
	if writer.size == 0 {
		return os.Remove(path)
	}
	finalPath := strings.TrimSuffix(path, ".part")
	if err := os.Rename(path, finalPath); err != nil {
		return fmt.Errorf("finalize log segment: %w", err)
	}
	r.finalized = append(r.finalized, segment{path: finalPath, size: writer.size, finishedAt: time.Now().UTC()})
	writer.size = 0
	if r.metrics != nil {
		r.metrics.segments.Inc()
	}
	return nil
}

func (r *recorder) scan() error {
	return filepath.WalkDir(r.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".log.part") {
			return os.Remove(path)
		}
		if !strings.HasSuffix(entry.Name(), ".log") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		r.finalized = append(r.finalized, segment{path: path, size: info.Size(), finishedAt: info.ModTime().UTC()})
		r.totalSize += info.Size()
		return nil
	})
}

func (r *recorder) prune(reserve int64) error {
	for r.totalSize+reserve > r.config.MaxRetainedSize {
		sort.Slice(r.finalized, func(i, j int) bool {
			if r.finalized[i].finishedAt.Equal(r.finalized[j].finishedAt) {
				return r.finalized[i].path < r.finalized[j].path
			}
			return r.finalized[i].finishedAt.Before(r.finalized[j].finishedAt)
		})
		if len(r.finalized) == 0 {
			oldestActive := r.oldestActiveWriter()
			if oldestActive == nil {
				// A single record may be larger than the entire quota. It is
				// written intact and handled as a finalized segment afterwards.
				break
			}
			if err := r.finish(oldestActive); err != nil {
				return err
			}
			continue
		}
		oldest := r.finalized[0]
		if err := os.Remove(oldest.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete oldest log segment: %w", err)
		}
		r.finalized = r.finalized[1:]
		r.totalSize -= oldest.size
		removeEmptyParents(filepath.Dir(oldest.path), r.root)
		if r.metrics != nil {
			r.metrics.deletedSegments.Inc()
			r.metrics.deletedBytes.Add(float64(oldest.size))
		}
	}
	return nil
}

func (r *recorder) oldestActiveWriter() *streamWriter {
	var oldest *streamWriter
	for _, writer := range r.writers {
		if writer.file == nil || writer.size == 0 {
			continue
		}
		if oldest == nil || writer.openedAt.Before(oldest.openedAt) || (writer.openedAt.Equal(oldest.openedAt) && writer.key < oldest.key) {
			oldest = writer
		}
	}
	return oldest
}

func (r *recorder) syncLoop() {
	ticker := time.NewTicker(r.config.SyncInterval)
	defer ticker.Stop()
	defer close(r.doneSync)
	for {
		select {
		case <-ticker.C:
			r.mu.Lock()
			for _, writer := range r.writers {
				if writer.file != nil {
					if err := writer.file.Sync(); err != nil {
						r.logger.Error("sync workload log failed", "file", writer.file.Name(), "error", err)
					}
				}
			}
			r.mu.Unlock()
		case <-r.stopSync:
			return
		}
	}
}

func (r *recorder) writeMetadata() error {
	r.metadata.RetainedSize = r.totalSize
	r.metadata.SegmentCount = len(r.finalized) + activeSegmentCount(r.writers)
	data, err := json.MarshalIndent(r.metadata, "", "  ")
	if err != nil {
		return err
	}
	part := filepath.Join(r.runDir, "metadata.json.part")
	if err := os.WriteFile(part, append(data, '\n'), 0o640); err != nil {
		return err
	}
	final := filepath.Join(r.runDir, "metadata.json")
	if err := os.Remove(final); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(part, final)
}

func (r *recorder) recordDirectory(record domain.LogRecord) string {
	uid := record.PodUID
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return filepath.Join(r.runDir, safe(record.Target.Namespace), safe(record.Target.Kind+"-"+record.Target.Name), safe(record.PodName+"_"+uid), safe(record.Container), fmt.Sprintf("restart-%d", record.RestartCount))
}

func streamKey(record domain.LogRecord) string {
	return strings.Join([]string{record.Target.Namespace, record.Target.Kind, record.Target.Name, record.PodUID, record.Container, fmt.Sprint(record.RestartCount)}, "\x00")
}

func safe(value string) string {
	value = strings.Trim(unsafePath.ReplaceAllString(value, "_"), "._-")
	if value == "" {
		return "unknown"
	}
	return value
}

func activeSegmentCount(writers map[string]*streamWriter) int {
	count := 0
	for _, writer := range writers {
		if writer.file != nil && writer.size > 0 {
			count++
		}
	}
	return count
}

func removeEmptyParents(directory, root string) {
	for directory != root && strings.HasPrefix(directory, root+string(os.PathSeparator)) {
		if err := os.Remove(directory); err != nil {
			return
		}
		directory = filepath.Dir(directory)
	}
}

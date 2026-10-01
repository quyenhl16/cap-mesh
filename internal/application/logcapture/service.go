package logcapture

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

type State string

const (
	StateStopped  State = "STOPPED"
	StateStarting State = "STARTING"
	StateRunning  State = "RUNNING"
	StateStopping State = "STOPPING"
	StateFailed   State = "FAILED"
)

type Config struct {
	ReconcileInterval time.Duration
	QueueSize         int
	SegmentSize       int64
	MaxRetainedSize   int64
}

type StartInput struct {
	Targets      []domain.WorkloadLogTarget
	SinceSeconds uint32
}

type Capture struct {
	RunID         string
	Status        State
	StartedAt     time.Time
	RetainedSize  int64
	SegmentCount  int
	ActiveStreams int
	Message       string
	Targets       []domain.WorkloadLogTarget
}

type Metrics interface {
	SetStreams(int)
	Reconnect()
	Error()
}

type Service struct {
	operation sync.Mutex
	mu        sync.Mutex
	source    ports.WorkloadLogSource
	factory   ports.WorkloadLogRecorderFactory
	config    Config
	logger    *slog.Logger
	metrics   Metrics
	state     Capture
	job       *job
}

type job struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	source    ports.WorkloadLogSource
	recorder  ports.WorkloadLogRecorder
	config    Config
	input     StartInput
	startedAt time.Time
	logger    *slog.Logger
	metrics   Metrics
	records   chan domain.LogRecord
	streams   map[string]*logStream
	streamWG  sync.WaitGroup
	onStreams func(int)
	onMessage func(string)
	onDone    func(error)
}

type logStream struct {
	cancel context.CancelFunc
}

func NewService(source ports.WorkloadLogSource, factory ports.WorkloadLogRecorderFactory, config Config, logger *slog.Logger, metrics Metrics) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{source: source, factory: factory, config: config, logger: logger, metrics: metrics, state: Capture{Status: StateStopped}}
}

func (s *Service) Start(ctx context.Context, input StartInput) (Capture, error) {
	s.operation.Lock()
	defer s.operation.Unlock()
	s.mu.Lock()
	if s.state.Status != StateStopped {
		result := s.snapshotLocked()
		s.mu.Unlock()
		return result, fmt.Errorf("%w: workload log capture is %s; stop it before starting again", ports.ErrAlreadyExists, result.Status)
	}
	s.state = Capture{Status: StateStarting, Targets: cloneTargets(input.Targets)}
	s.mu.Unlock()

	if err := s.validate(input); err != nil {
		return s.startFailed(err)
	}
	startedAt := time.Now().UTC()
	runID := startedAt.Format("20060102T150405.000000000Z")
	recorder, err := s.factory.Start(domain.LogCaptureRun{RunID: runID, StartedAt: startedAt, Targets: cloneTargets(input.Targets), SegmentSize: s.config.SegmentSize, MaxRetainedSize: s.config.MaxRetainedSize})
	if err != nil {
		return s.startFailed(err)
	}
	jobCtx, cancel := context.WithCancel(context.Background())
	j := &job{
		ctx: jobCtx, cancel: cancel, done: make(chan struct{}), source: s.source, recorder: recorder,
		config: s.config, input: input, startedAt: startedAt, logger: s.logger, metrics: s.metrics,
		records: make(chan domain.LogRecord, s.config.QueueSize), streams: make(map[string]*logStream),
	}
	j.onStreams = s.setStreams
	j.onMessage = s.setMessage
	j.onDone = s.jobDone
	if err := j.reconcile(ctx); err != nil {
		cancel()
		_ = recorder.Close("FAILED", err.Error())
		return s.startFailed(err)
	}

	s.mu.Lock()
	s.job = j
	s.state = Capture{RunID: runID, Status: StateRunning, StartedAt: startedAt, ActiveStreams: len(j.streams), Targets: cloneTargets(input.Targets)}
	result := s.snapshotLocked()
	s.mu.Unlock()
	go j.run()
	return result, nil
}

func (s *Service) Stop(ctx context.Context) (Capture, error) {
	s.operation.Lock()
	defer s.operation.Unlock()
	s.mu.Lock()
	if s.state.Status == StateStopped {
		result := s.snapshotLocked()
		s.mu.Unlock()
		return result, nil
	}
	if s.job == nil {
		s.state.Status = StateStopped
		result := s.snapshotLocked()
		s.mu.Unlock()
		return result, nil
	}
	s.state.Status = StateStopping
	j := s.job
	j.cancel()
	s.mu.Unlock()
	select {
	case <-j.done:
	case <-ctx.Done():
		return s.Get(), ctx.Err()
	}
	return s.Get(), nil
}

func (s *Service) Get() Capture {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Service) Shutdown(ctx context.Context) error {
	_, err := s.Stop(ctx)
	return err
}

func (s *Service) validate(input StartInput) error {
	if s.source == nil || s.factory == nil {
		return fmt.Errorf("%w: workload log capture is not configured", ports.ErrUnavailable)
	}
	if s.config.ReconcileInterval <= 0 || s.config.QueueSize < 1 {
		return errors.New("invalid workload log capture configuration")
	}
	if len(input.Targets) == 0 {
		return errors.New("at least one workload log target is required")
	}
	seen := make(map[string]struct{})
	for i, target := range input.Targets {
		target.Namespace = strings.TrimSpace(target.Namespace)
		target.Kind = strings.ToLower(strings.TrimSpace(target.Kind))
		target.Name = strings.TrimSpace(target.Name)
		if target.Namespace == "" || target.Name == "" || (target.Kind != "statefulset" && target.Kind != "deployment") {
			return fmt.Errorf("target %d must have namespace, kind statefulset/deployment, and name", i+1)
		}
		if target.MaxPods == 0 {
			target.MaxPods = 100
		}
		key := target.Namespace + "/" + target.Kind + "/" + target.Name
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate workload log target %q", key)
		}
		seen[key] = struct{}{}
		input.Targets[i] = target
	}
	return nil
}

func (s *Service) startFailed(err error) (Capture, error) {
	s.mu.Lock()
	s.state = Capture{Status: StateStopped, Message: err.Error()}
	result := s.snapshotLocked()
	s.mu.Unlock()
	return result, err
}

func (s *Service) snapshotLocked() Capture {
	result := s.state
	result.Targets = cloneTargets(result.Targets)
	if s.job != nil {
		usage := s.job.recorder.Usage()
		result.RetainedSize = usage.RetainedSize
		result.SegmentCount = usage.SegmentCount
	}
	return result
}

func (s *Service) setStreams(count int) {
	s.mu.Lock()
	s.state.ActiveStreams = count
	s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.SetStreams(count)
	}
}

func (s *Service) setMessage(message string) {
	s.mu.Lock()
	if s.state.Status == StateRunning {
		s.state.Message = message
	}
	s.mu.Unlock()
}

func (s *Service) jobDone(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	usage := domain.LogRecordingUsage{}
	if s.job != nil {
		usage = s.job.recorder.Usage()
	}
	if err != nil {
		s.state.Status = StateFailed
		s.state.Message = err.Error()
	} else {
		s.state.Status = StateStopped
		s.state.Message = ""
	}
	s.state.ActiveStreams = 0
	s.state.RetainedSize = usage.RetainedSize
	s.state.SegmentCount = usage.SegmentCount
	s.job = nil
}

func (j *job) run() {
	defer close(j.done)
	ticker := time.NewTicker(j.config.ReconcileInterval)
	defer ticker.Stop()
	var runErr error
	running := true
	for running {
		select {
		case record := <-j.records:
			if err := j.recorder.Write(record); err != nil {
				runErr = err
				j.cancel()
				running = false
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(j.ctx, j.config.ReconcileInterval)
			err := j.reconcile(ctx)
			cancel()
			if err != nil {
				j.logger.Warn("reconcile workload log streams failed", "error", err)
				j.onMessage(err.Error())
				if j.metrics != nil {
					j.metrics.Error()
				}
			} else {
				j.onMessage("")
			}
		case <-j.ctx.Done():
			running = false
		}
	}
	for _, stream := range j.streams {
		stream.cancel()
	}
	j.streamWG.Wait()
	for {
		select {
		case record := <-j.records:
			if runErr == nil {
				runErr = j.recorder.Write(record)
			}
		default:
			status, message := "STOPPED", ""
			if runErr != nil {
				status, message = "FAILED", runErr.Error()
			}
			if err := j.recorder.Close(status, message); err != nil {
				runErr = errors.Join(runErr, err)
			}
			j.onStreams(0)
			j.onDone(runErr)
			return
		}
	}
}

func (j *job) reconcile(ctx context.Context) error {
	desired := make(map[string]domain.LogRecord)
	for _, target := range j.input.Targets {
		pods, err := j.source.ResolvePods(ctx, target)
		if err != nil {
			return fmt.Errorf("resolve %s/%s/%s: %w", target.Namespace, target.Kind, target.Name, err)
		}
		limit := target.MaxPods
		if limit == 0 {
			limit = 100
		}
		if len(pods) > int(limit) {
			return fmt.Errorf("workload %s/%s/%s has %d pods, exceeding max_pods %d", target.Namespace, target.Kind, target.Name, len(pods), limit)
		}
		for _, pod := range pods {
			for _, container := range selectedContainers(pod.Containers, target.Containers) {
				record := domain.LogRecord{Target: target, PodName: pod.Name, PodUID: pod.UID, Container: container, RestartCount: pod.RestartCount[container]}
				desired[streamKey(record)] = record
			}
		}
	}
	for key, stream := range j.streams {
		if _, exists := desired[key]; !exists {
			stream.cancel()
			delete(j.streams, key)
		}
	}
	for key, record := range desired {
		if _, exists := j.streams[key]; exists {
			continue
		}
		streamCtx, cancel := context.WithCancel(j.ctx)
		j.streams[key] = &logStream{cancel: cancel}
		j.streamWG.Add(1)
		go func(record domain.LogRecord) {
			defer j.streamWG.Done()
			j.stream(streamCtx, record)
		}(record)
	}
	j.onStreams(len(j.streams))
	return nil
}

func (j *job) stream(ctx context.Context, record domain.LogRecord) {
	since := j.startedAt.Add(-time.Duration(j.input.SinceSeconds) * time.Second)
	backoff := time.Second
	connected := false
	for ctx.Err() == nil {
		body, err := j.source.StreamLogs(ctx, domain.PodLogRequest{Namespace: record.Target.Namespace, PodName: record.PodName, Container: record.Container, SinceTime: since})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			j.logger.Warn("open Kubernetes pod log stream failed", "namespace", record.Target.Namespace, "pod", record.PodName, "container", record.Container, "error", err)
			if j.metrics != nil {
				j.metrics.Error()
			}
			if !j.waitForCurrentStream(ctx, record, backoff) {
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		if connected && j.metrics != nil {
			j.metrics.Reconnect()
		}
		connected = true
		backoff = time.Second
		reader := bufio.NewReader(body)
		for {
			data, readErr := reader.ReadBytes('\n')
			if len(data) > 0 {
				now := time.Now().UTC()
				record.ReceivedAt = now
				record.Data = append([]byte(nil), data...)
				select {
				case j.records <- record:
					since = now.Add(-time.Second)
				case <-ctx.Done():
					_ = body.Close()
					return
				}
			}
			if readErr != nil {
				_ = body.Close()
				if ctx.Err() != nil {
					return
				}
				if !errors.Is(readErr, io.EOF) {
					j.logger.Warn("Kubernetes pod log stream ended", "namespace", record.Target.Namespace, "pod", record.PodName, "container", record.Container, "error", readErr)
				}
				break
			}
		}
		if !j.waitForCurrentStream(ctx, record, backoff) {
			return
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (j *job) waitForCurrentStream(ctx context.Context, record domain.LogRecord, delay time.Duration) bool {
	if !wait(ctx, delay) {
		return false
	}
	for ctx.Err() == nil {
		checkCtx, cancel := context.WithTimeout(ctx, j.config.ReconcileInterval)
		pods, err := j.source.ResolvePods(checkCtx, record.Target)
		cancel()
		if err != nil {
			j.logger.Warn("verify Kubernetes pod log stream failed", "namespace", record.Target.Namespace, "pod", record.PodName, "container", record.Container, "error", err)
			if j.metrics != nil {
				j.metrics.Error()
			}
			if !wait(ctx, min(delay*2, 30*time.Second)) {
				return false
			}
			continue
		}
		for _, pod := range pods {
			if pod.UID != record.PodUID {
				continue
			}
			for _, container := range pod.Containers {
				if container == record.Container && pod.RestartCount[container] == record.RestartCount {
					return true
				}
			}
		}
		return false
	}
	return false
}

func selectedContainers(available, requested []string) []string {
	if len(requested) == 0 {
		return append([]string(nil), available...)
	}
	have := make(map[string]struct{}, len(available))
	for _, name := range available {
		have[name] = struct{}{}
	}
	var result []string
	seen := make(map[string]struct{})
	for _, name := range requested {
		name = strings.TrimSpace(name)
		if _, ok := have[name]; !ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func streamKey(record domain.LogRecord) string {
	return strings.Join([]string{record.Target.Namespace, record.Target.Kind, record.Target.Name, record.PodUID, record.Container, fmt.Sprint(record.RestartCount)}, "\x00")
}

func cloneTargets(targets []domain.WorkloadLogTarget) []domain.WorkloadLogTarget {
	result := make([]domain.WorkloadLogTarget, len(targets))
	copy(result, targets)
	for i := range result {
		result[i].Containers = append([]string(nil), result[i].Containers...)
	}
	return result
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

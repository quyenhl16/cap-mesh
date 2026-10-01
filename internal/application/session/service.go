package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/interfacealias"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

const (
	defaultMaxPods           = 100
	defaultReconcileInterval = 5 * time.Second
	maxTargetsPerSession     = 110
	maxSourcesPerNode        = 50
)

type CreateInput struct {
	Nodes            []string
	LogicalInterface string
	Targets          []domain.CaptureTarget
	Filter           string
	Snaplen          uint32
	TTL              time.Duration
	ReorderWindow    time.Duration
	Continuous       bool
}

type ListInput struct {
	Status string
	Mode   string
}

type runtimeSession struct {
	cancel  context.CancelFunc
	sources map[string]domain.CaptureSource
}

type Service struct {
	repository        ports.SessionRepository
	agents            ports.AgentCommander
	packets           ports.PacketPublisher
	recorder          ports.CaptureRecorder
	resolver          ports.WorkloadResolver
	subscriberSize    int
	reconcileInterval time.Duration
	now               func() time.Time
	mu                sync.Mutex
	runtime           map[string]*runtimeSession
}

func NewService(repository ports.SessionRepository, agents ports.AgentCommander, packets ports.PacketPublisher, recorder ports.CaptureRecorder, subscriberSize int) *Service {
	return &Service{repository: repository, agents: agents, packets: packets, recorder: recorder, subscriberSize: subscriberSize, reconcileInterval: defaultReconcileInterval, now: time.Now, runtime: make(map[string]*runtimeSession)}
}

func (s *Service) SetWorkloadResolver(resolver ports.WorkloadResolver, interval time.Duration) {
	s.resolver = resolver
	if interval > 0 {
		s.reconcileInterval = interval
	}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (domain.Session, error) {
	targets, logicalInterface, err := normalizeTargets(input)
	if err != nil {
		return domain.Session{}, err
	}
	input.Targets = targets
	input.LogicalInterface = logicalInterface
	if err := validate(input); err != nil {
		return domain.Session{}, err
	}

	connected := s.agents.ConnectedNodes()
	sources, err := s.resolveInitialSources(ctx, targets, connected)
	if err != nil {
		return domain.Session{}, err
	}
	now := s.now()
	mode := domain.SessionModeNormal
	var expiresAt time.Time
	if input.Continuous {
		mode = domain.SessionModeContinuous
	} else {
		expiresAt = now.Add(input.TTL)
	}
	session := domain.Session{
		ID:               newID(),
		Nodes:            sourceNodes(sources),
		LogicalInterface: logicalInterface,
		Targets:          targets,
		Filter:           input.Filter,
		Snaplen:          input.Snaplen,
		ReorderWindow:    input.ReorderWindow,
		CreatedAt:        now,
		ExpiresAt:        expiresAt,
		Status:           domain.SessionStarting,
		Mode:             mode,
	}
	if err := s.repository.Create(ctx, session); err != nil {
		return domain.Session{}, err
	}
	if err := s.packets.OpenSession(session.ID, input.ReorderWindow, s.subscriberSize); err != nil {
		return domain.Session{}, err
	}
	if s.recorder != nil {
		if err := s.recorder.Start(session); err != nil {
			s.packets.CloseSession(session.ID)
			_ = session.Transition(domain.SessionFailed)
			session.Message = "start recorder: " + err.Error()
			_ = s.repository.Update(ctx, session)
			return domain.Session{}, fmt.Errorf("start recorder: %w", err)
		}
	}

	runtimeCtx, cancel := context.WithCancel(context.Background())
	runtime := &runtimeSession{cancel: cancel, sources: make(map[string]domain.CaptureSource)}
	s.mu.Lock()
	s.runtime[session.ID] = runtime
	s.mu.Unlock()

	started := 0
	var failures []string
	for _, source := range sources {
		if err := s.startSource(ctx, session, source); err != nil {
			failures = append(failures, source.ID+": "+err.Error())
			continue
		}
		s.setRuntimeSource(session.ID, source)
		started++
	}
	hasFollower := hasFollowingWorkload(targets)
	if started == 0 && !hasFollower {
		cancel()
		s.removeRuntime(session.ID)
		_ = session.Transition(domain.SessionFailed)
		if len(failures) == 0 {
			session.Message = "no capture sources resolved"
		} else {
			session.Message = strings.Join(failures, "; ")
		}
		s.packets.CloseSession(session.ID)
	} else {
		_ = session.Transition(domain.SessionRunning)
		if len(failures) > 0 {
			session.Message = "partial: " + strings.Join(failures, "; ")
		} else if started == 0 {
			session.Message = "waiting for workload pods"
		}
	}
	if err := s.repository.Update(ctx, session); err != nil {
		cancel()
		s.removeRuntime(session.ID)
		return domain.Session{}, err
	}
	if session.Status == domain.SessionRunning {
		if hasFollower {
			go s.reconcileLoop(runtimeCtx, session.ID)
		}
		if !input.Continuous {
			time.AfterFunc(input.TTL, func() { _, _ = s.Stop(context.Background(), session.ID) })
		}
	}
	return session, nil
}

func (s *Service) Stop(ctx context.Context, id string) (domain.Session, error) {
	session, err := s.repository.Get(ctx, id)
	if err != nil {
		return domain.Session{}, err
	}
	if session.Status == domain.SessionStopped || session.Status == domain.SessionFailed {
		return session, nil
	}
	if err := session.Transition(domain.SessionStopping); err != nil {
		return domain.Session{}, err
	}
	_ = s.repository.Update(ctx, session)

	s.mu.Lock()
	runtime := s.runtime[id]
	delete(s.runtime, id)
	s.mu.Unlock()
	if runtime != nil {
		runtime.cancel()
		for _, source := range runtime.sources {
			_ = s.agents.Send(ctx, source.NodeName, ports.AgentCommand{Kind: "stop", SessionID: session.ID, SourceID: source.ID})
		}
	} else {
		for _, node := range session.Nodes {
			_ = s.agents.Send(ctx, node, ports.AgentCommand{Kind: "stop", SessionID: session.ID})
		}
	}
	s.packets.CloseSession(id)
	_ = session.Transition(domain.SessionStopped)
	if err := s.repository.Update(ctx, session); err != nil {
		return domain.Session{}, err
	}
	return session, nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Session, error) {
	return s.repository.Get(ctx, id)
}

func (s *Service) List(ctx context.Context, input ListInput) ([]domain.Session, error) {
	status := domain.SessionStatus(strings.ToUpper(strings.TrimSpace(input.Status)))
	mode := domain.SessionMode(strings.ToUpper(strings.TrimSpace(input.Mode)))
	if status != "" && status != domain.SessionStarting && status != domain.SessionRunning && status != domain.SessionStopping && status != domain.SessionStopped && status != domain.SessionFailed {
		return nil, fmt.Errorf("invalid session status %q", input.Status)
	}
	if mode != "" && mode != domain.SessionModeNormal && mode != domain.SessionModeContinuous {
		return nil, fmt.Errorf("invalid session mode %q", input.Mode)
	}
	sessions, err := s.repository.List(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]domain.Session, 0, len(sessions))
	for _, captureSession := range sessions {
		if status != "" && captureSession.Status != status {
			continue
		}
		if mode != "" && captureSession.Mode != mode {
			continue
		}
		filtered = append(filtered, captureSession)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].ID < filtered[j].ID
		}
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	return filtered, nil
}

func (s *Service) AgentDisconnected(ctx context.Context, node string) {
	s.mu.Lock()
	for _, runtime := range s.runtime {
		for sourceID, source := range runtime.sources {
			if source.NodeName == node && source.TargetType == "workload" {
				delete(runtime.sources, sourceID)
			}
		}
	}
	s.mu.Unlock()
	sessions, err := s.repository.List(ctx)
	if err != nil {
		return
	}
	for _, captureSession := range sessions {
		if captureSession.Status != domain.SessionRunning || !slices.Contains(captureSession.Nodes, node) {
			continue
		}
		captureSession.Message = "partial: agent disconnected: " + node
		connected := s.agents.ConnectedNodes()
		hasActiveNode := false
		for _, sessionNode := range captureSession.Nodes {
			hasActiveNode = hasActiveNode || slices.Contains(connected, sessionNode)
		}
		if !hasActiveNode && !hasFollowingWorkload(captureSession.Targets) {
			_ = captureSession.Transition(domain.SessionFailed)
			captureSession.Message = "all capture agents disconnected"
			s.packets.CloseSession(captureSession.ID)
		}
		_ = s.repository.Update(ctx, captureSession)
	}
}

func (s *Service) CaptureStatus(ctx context.Context, node, sessionID, sourceID, state, message string) {
	if state != "FAILED" || sourceID == "" {
		return
	}
	s.deleteRuntimeSource(sessionID, sourceID)
	session, err := s.repository.Get(ctx, sessionID)
	if err != nil || session.Status != domain.SessionRunning {
		return
	}
	session.Message = "partial: source " + sourceID + " on " + node + " failed: " + message
	if s.runtimeSourceCount(sessionID) == 0 && !hasFollowingWorkload(session.Targets) {
		_ = session.Transition(domain.SessionFailed)
		session.Message = "all capture sources failed: " + message
		s.packets.CloseSession(sessionID)
		s.mu.Lock()
		if runtime := s.runtime[sessionID]; runtime != nil {
			runtime.cancel()
			delete(s.runtime, sessionID)
		}
		s.mu.Unlock()
	}
	_ = s.repository.Update(ctx, session)
	s.updateSessionNodes(ctx, sessionID)
}

func (s *Service) runtimeSourceCount(sessionID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if runtime := s.runtime[sessionID]; runtime != nil {
		return len(runtime.sources)
	}
	return 0
}

func (s *Service) resolveInitialSources(ctx context.Context, targets []domain.CaptureTarget, connected []string) ([]domain.CaptureSource, error) {
	var sources []domain.CaptureSource
	workloadPods := 0
	for _, target := range targets {
		if target.Interface != nil {
			nodes := append([]string(nil), target.Interface.Nodes...)
			if len(nodes) == 0 {
				nodes = append(nodes, connected...)
			}
			if len(nodes) == 0 {
				return nil, fmt.Errorf("%w: no agents connected", ports.ErrUnavailable)
			}
			for _, node := range nodes {
				if !slices.Contains(connected, node) {
					return nil, fmt.Errorf("%w: agent %q is not connected", ports.ErrUnavailable, node)
				}
				sources = append(sources, domain.CaptureSource{ID: interfaceSourceID(target.ID, node, target.Interface.LogicalInterface), TargetID: target.ID, TargetType: "interface", NodeName: node, LogicalInterface: target.Interface.LogicalInterface})
			}
			continue
		}
		if target.Workload != nil {
			if s.resolver == nil {
				return nil, errors.New("workload capture is not configured on the server")
			}
			resolved, err := s.resolver.Resolve(ctx, *target.Workload, target.ID)
			if err != nil {
				return nil, fmt.Errorf("resolve workload target %q: %w", target.ID, err)
			}
			if len(resolved) > int(target.Workload.MaxPods) {
				return nil, fmt.Errorf("workload target %q resolved %d pods, maximum is %d", target.ID, len(resolved), target.Workload.MaxPods)
			}
			workloadPods += len(resolved)
			if workloadPods > defaultMaxPods {
				return nil, fmt.Errorf("workload targets resolved %d pods, session maximum is %d", workloadPods, defaultMaxPods)
			}
			sources = append(sources, resolved...)
		}
	}
	result := uniqueSources(sources)
	if err := validateSourceDistribution(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) startSource(ctx context.Context, session domain.Session, source domain.CaptureSource) error {
	if !slices.Contains(s.agents.ConnectedNodes(), source.NodeName) {
		return fmt.Errorf("%w: agent %q is not connected", ports.ErrUnavailable, source.NodeName)
	}
	filter := session.Filter
	if source.TargetType == "workload" {
		target := findTarget(session.Targets, source.TargetID)
		if target == nil || target.Workload == nil {
			return fmt.Errorf("workload target %q not found", source.TargetID)
		}
		filter = workloadFilter(target.Workload.Direction, source.PodIP, filter)
	}
	return s.agents.Send(ctx, source.NodeName, ports.AgentCommand{Kind: "start", SessionID: session.ID, SourceID: source.ID, LogicalInterface: source.LogicalInterface, Filter: filter, Snaplen: session.Snaplen, Source: source})
}

func (s *Service) reconcileLoop(ctx context.Context, sessionID string) {
	ticker := time.NewTicker(s.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reconcile(ctx, sessionID)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) reconcile(ctx context.Context, sessionID string) {
	session, err := s.repository.Get(ctx, sessionID)
	if err != nil || session.Status != domain.SessionRunning {
		return
	}
	for _, target := range session.Targets {
		if target.Workload == nil || !target.Workload.Follow {
			continue
		}
		resolved, err := s.resolver.Resolve(ctx, *target.Workload, target.ID)
		if err != nil || len(resolved) > int(target.Workload.MaxPods) || s.workloadSourceCountExcluding(sessionID, target.ID)+len(resolved) > defaultMaxPods {
			continue
		}
		projected := s.runtimeSourcesExcluding(sessionID, target.ID)
		projected = append(projected, resolved...)
		if validateSourceDistribution(projected) != nil {
			continue
		}
		desired := make(map[string]domain.CaptureSource, len(resolved))
		for _, source := range resolved {
			desired[source.ID] = source
		}
		current := s.runtimeSources(sessionID, target.ID)
		for id, source := range current {
			wanted, exists := desired[id]
			if exists && wanted.NodeName == source.NodeName && wanted.InterfaceName == source.InterfaceName && wanted.PodIP == source.PodIP {
				delete(desired, id)
				continue
			}
			_ = s.agents.Send(ctx, source.NodeName, ports.AgentCommand{Kind: "stop", SessionID: sessionID, SourceID: source.ID})
			s.deleteRuntimeSource(sessionID, source.ID)
		}
		for _, source := range desired {
			if err := s.startSource(ctx, session, source); err == nil {
				s.setRuntimeSource(sessionID, source)
			}
		}
	}
	s.updateSessionNodes(ctx, sessionID)
}

func (s *Service) workloadSourceCountExcluding(sessionID, targetID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	if runtime := s.runtime[sessionID]; runtime != nil {
		for _, source := range runtime.sources {
			if source.TargetType == "workload" && source.TargetID != targetID {
				count++
			}
		}
	}
	return count
}

func (s *Service) runtimeSourcesExcluding(sessionID, targetID string) []domain.CaptureSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sources []domain.CaptureSource
	if runtime := s.runtime[sessionID]; runtime != nil {
		for _, source := range runtime.sources {
			if source.TargetID != targetID {
				sources = append(sources, source)
			}
		}
	}
	return sources
}

func (s *Service) runtimeSources(sessionID, targetID string) map[string]domain.CaptureSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]domain.CaptureSource)
	if runtime := s.runtime[sessionID]; runtime != nil {
		for id, source := range runtime.sources {
			if source.TargetID == targetID {
				out[id] = source
			}
		}
	}
	return out
}

func (s *Service) setRuntimeSource(sessionID string, source domain.CaptureSource) {
	s.mu.Lock()
	if runtime := s.runtime[sessionID]; runtime != nil {
		runtime.sources[source.ID] = source
	}
	s.mu.Unlock()
}

func (s *Service) deleteRuntimeSource(sessionID, sourceID string) {
	s.mu.Lock()
	if runtime := s.runtime[sessionID]; runtime != nil {
		delete(runtime.sources, sourceID)
	}
	s.mu.Unlock()
}

func (s *Service) removeRuntime(sessionID string) {
	s.mu.Lock()
	delete(s.runtime, sessionID)
	s.mu.Unlock()
}

func (s *Service) updateSessionNodes(ctx context.Context, sessionID string) {
	s.mu.Lock()
	runtime := s.runtime[sessionID]
	var sources []domain.CaptureSource
	if runtime != nil {
		for _, source := range runtime.sources {
			sources = append(sources, source)
		}
	}
	s.mu.Unlock()
	session, err := s.repository.Get(ctx, sessionID)
	if err != nil {
		return
	}
	session.Nodes = sourceNodes(sources)
	if len(session.Nodes) == 0 && hasFollowingWorkload(session.Targets) && (session.Message == "" || session.Message == "waiting for workload pods") {
		session.Message = "waiting for workload pods"
	} else if session.Message == "waiting for workload pods" {
		session.Message = ""
	}
	_ = s.repository.Update(ctx, session)
}

func normalizeTargets(input CreateInput) ([]domain.CaptureTarget, string, error) {
	targets := append([]domain.CaptureTarget(nil), input.Targets...)
	if len(targets) == 0 {
		logical, err := interfacealias.Normalize(input.LogicalInterface)
		if err != nil {
			return nil, "", err
		}
		return []domain.CaptureTarget{{ID: "legacy-interface", Interface: &domain.InterfaceTarget{Nodes: append([]string(nil), input.Nodes...), LogicalInterface: logical}}}, logical, nil
	}
	logicalInterface := ""
	ids := make(map[string]struct{})
	for i := range targets {
		if targets[i].ID == "" {
			targets[i].ID = fmt.Sprintf("target-%d", i+1)
		}
		if _, exists := ids[targets[i].ID]; exists {
			return nil, "", fmt.Errorf("duplicate target ID %q", targets[i].ID)
		}
		ids[targets[i].ID] = struct{}{}
		if (targets[i].Interface == nil) == (targets[i].Workload == nil) {
			return nil, "", fmt.Errorf("target %q must contain exactly one target type", targets[i].ID)
		}
		if targets[i].Interface != nil {
			alias, err := interfacealias.Normalize(targets[i].Interface.LogicalInterface)
			if err != nil {
				return nil, "", fmt.Errorf("target %q: %w", targets[i].ID, err)
			}
			targets[i].Interface.LogicalInterface = alias
			if logicalInterface == "" {
				logicalInterface = alias
			}
		}
		if targets[i].Workload != nil {
			workload := targets[i].Workload
			workload.Namespace = strings.TrimSpace(workload.Namespace)
			workload.Kind = strings.ToLower(strings.TrimSpace(workload.Kind))
			workload.Name = strings.TrimSpace(workload.Name)
			workload.Direction = strings.ToLower(strings.TrimSpace(workload.Direction))
			if workload.Direction == "" {
				workload.Direction = "egress"
			}
			if workload.MaxPods == 0 {
				workload.MaxPods = defaultMaxPods
			}
		}
	}
	return targets, logicalInterface, nil
}

func validate(input CreateInput) error {
	switch {
	case input.Snaplen < 1 || input.Snaplen > 65535:
		return errors.New("snaplen must be between 1 and 65535")
	case !input.Continuous && (input.TTL < time.Second || input.TTL > 24*time.Hour):
		return errors.New("TTL must be between 1 second and 24 hours")
	case input.ReorderWindow < 0 || input.ReorderWindow > 5*time.Second:
		return errors.New("reorder window must be between 0 and 5 seconds")
	case len(input.Filter) > 4096:
		return errors.New("filter is too long")
	case len(input.Targets) == 0:
		return errors.New("at least one capture target is required")
	case len(input.Targets) > maxTargetsPerSession:
		return fmt.Errorf("too many capture targets: maximum is %d", maxTargetsPerSession)
	}
	for _, target := range input.Targets {
		if target.Interface != nil {
			if len(target.Interface.Nodes) > 100 {
				return fmt.Errorf("target %q has too many nodes", target.ID)
			}
			continue
		}
		workload := target.Workload
		if workload.Namespace == "" || workload.Name == "" {
			return fmt.Errorf("target %q requires workload namespace and name", target.ID)
		}
		if workload.Kind != "statefulset" && workload.Kind != "deployment" {
			return fmt.Errorf("target %q workload kind must be statefulset or deployment", target.ID)
		}
		if workload.Direction != "egress" && workload.Direction != "ingress" && workload.Direction != "both" {
			return fmt.Errorf("target %q direction must be egress, ingress, or both", target.ID)
		}
		if workload.MaxPods > defaultMaxPods {
			return fmt.Errorf("target %q max pods must not exceed %d", target.ID, defaultMaxPods)
		}
	}
	return nil
}

func interfaceSourceID(targetID, node, alias string) string {
	return "interface:" + targetID + ":" + node + ":" + alias
}

func workloadFilter(direction, podIP, extra string) string {
	base := "host " + podIP
	switch direction {
	case "egress":
		base = "src host " + podIP
	case "ingress":
		base = "dst host " + podIP
	}
	if strings.TrimSpace(extra) == "" {
		return base
	}
	return "(" + base + ") and (" + extra + ")"
}

func findTarget(targets []domain.CaptureTarget, id string) *domain.CaptureTarget {
	for i := range targets {
		if targets[i].ID == id {
			return &targets[i]
		}
	}
	return nil
}

func hasFollowingWorkload(targets []domain.CaptureTarget) bool {
	for _, target := range targets {
		if target.Workload != nil && target.Workload.Follow {
			return true
		}
	}
	return false
}

func uniqueSources(input []domain.CaptureSource) []domain.CaptureSource {
	seen := make(map[string]struct{}, len(input))
	out := make([]domain.CaptureSource, 0, len(input))
	for _, source := range input {
		if source.ID == "" {
			continue
		}
		if _, exists := seen[source.ID]; exists {
			continue
		}
		seen[source.ID] = struct{}{}
		out = append(out, source)
	}
	return out
}

func sourceNodes(sources []domain.CaptureSource) []string {
	seen := make(map[string]struct{})
	for _, source := range sources {
		if source.NodeName != "" {
			seen[source.NodeName] = struct{}{}
		}
	}
	nodes := make([]string, 0, len(seen))
	for node := range seen {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func validateSourceDistribution(sources []domain.CaptureSource) error {
	counts := make(map[string]int)
	for _, source := range sources {
		counts[source.NodeName]++
		if counts[source.NodeName] > maxSourcesPerNode {
			return fmt.Errorf("node %q has %d capture sources, maximum is %d", source.NodeName, counts[source.NodeName], maxSourcesPerNode)
		}
	}
	return nil
}

func newID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return "session-" + hex.EncodeToString(value[:])
}

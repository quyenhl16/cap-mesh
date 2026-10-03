package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/grpcapi"
	appcontinuous "github.com/quyenhl16/cap-mesh/internal/application/continuous"
	applogcapture "github.com/quyenhl16/cap-mesh/internal/application/logcapture"
	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type CaptureServer struct {
	capmeshv1.UnimplementedCaptureServiceServer
	sessions   *appsession.Service
	packets    ports.PacketPublisher
	logger     *slog.Logger
	continuous *appcontinuous.Service
	agents     *AgentRegistry
	logCapture *applogcapture.Service
	recordings ports.SessionRecordingCatalog
}

func (s *CaptureServer) SetSessionRecordingCatalog(catalog ports.SessionRecordingCatalog) {
	s.recordings = catalog
}

func (s *CaptureServer) SetContinuousCapture(service *appcontinuous.Service) {
	s.continuous = service
}

func (s *CaptureServer) SetAgentRegistry(registry *AgentRegistry) {
	s.agents = registry
}

func (s *CaptureServer) SetWorkloadLogCapture(service *applogcapture.Service) {
	s.logCapture = service
}

func NewCaptureServer(sessions *appsession.Service, packets ports.PacketPublisher, logger *slog.Logger) *CaptureServer {
	return &CaptureServer{sessions: sessions, packets: packets, logger: logger}
}

func (s *CaptureServer) CreateSession(ctx context.Context, request *capmeshv1.CreateSessionRequest) (*capmeshv1.CaptureSession, error) {
	session, err := s.sessions.Create(ctx, appsession.CreateInput{Nodes: request.GetNodes(), LogicalInterface: request.GetLogicalInterface(), Targets: grpcapi.CaptureTargetsFromProto(request.GetTargets()), Filter: request.GetFilter(), Snaplen: request.GetSnaplen(), TTL: time.Duration(request.GetTtlSeconds()) * time.Second, ReorderWindow: time.Duration(request.GetReorderWindowMs()) * time.Millisecond})
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("capture session created", "session_id", session.ID, "nodes", session.Nodes, "interface", session.LogicalInterface)
	return grpcapi.SessionToProto(session), nil
}

func (s *CaptureServer) StopSession(ctx context.Context, request *capmeshv1.StopSessionRequest) (*capmeshv1.CaptureSession, error) {
	session, err := s.sessions.Stop(ctx, request.GetSessionId())
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("capture session stopped", "session_id", session.ID)
	return grpcapi.SessionToProto(session), nil
}

func (s *CaptureServer) GetSession(ctx context.Context, request *capmeshv1.GetSessionRequest) (*capmeshv1.CaptureSession, error) {
	session, err := s.sessions.Get(ctx, request.GetSessionId())
	if err != nil {
		return nil, rpcError(err)
	}
	return grpcapi.SessionToProto(session), nil
}

func (s *CaptureServer) StreamPackets(request *capmeshv1.StreamPacketsRequest, server capmeshv1.CaptureService_StreamPacketsServer) error {
	if _, err := s.sessions.Get(server.Context(), request.GetSessionId()); err != nil {
		return rpcError(err)
	}
	packets, err := s.packets.Subscribe(server.Context(), request.GetSessionId())
	if err != nil {
		return rpcError(err)
	}
	if err := server.SendHeader(metadata.Pairs("capmesh-session-id", request.GetSessionId())); err != nil {
		return err
	}
	s.logger.Info("client subscribed", "session_id", request.GetSessionId())
	for batch := range packets {
		if err := server.Send(grpcapi.PacketBatchToProto(batch)); err != nil {
			return err
		}
	}
	return nil
}

func (s *CaptureServer) StartContinuousCapture(ctx context.Context, request *capmeshv1.StartContinuousCaptureRequest) (*capmeshv1.ContinuousCapture, error) {
	if s.continuous == nil {
		return nil, status.Error(codes.Unavailable, "continuous capture is not configured")
	}
	capture, err := s.continuous.Start(ctx, appcontinuous.StartInput{Targets: grpcapi.CaptureTargetsFromProto(request.GetTargets()), Filter: request.GetFilter(), Snaplen: request.GetSnaplen(), ReorderWindow: time.Duration(request.GetReorderWindowMs()) * time.Millisecond})
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("continuous capture started", "session_id", capture.SessionID)
	return continuousCaptureToProto(capture), nil
}

func (s *CaptureServer) StopContinuousCapture(ctx context.Context, _ *capmeshv1.StopContinuousCaptureRequest) (*capmeshv1.ContinuousCapture, error) {
	if s.continuous == nil {
		return nil, status.Error(codes.Unavailable, "continuous capture is not configured")
	}
	capture, err := s.continuous.Stop(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("continuous capture stopped")
	return continuousCaptureToProto(capture), nil
}

func (s *CaptureServer) GetContinuousCapture(ctx context.Context, _ *capmeshv1.GetContinuousCaptureRequest) (*capmeshv1.ContinuousCapture, error) {
	if s.continuous == nil {
		return nil, status.Error(codes.Unavailable, "continuous capture is not configured")
	}
	return continuousCaptureToProto(s.continuous.Get(ctx)), nil
}

func (s *CaptureServer) ListAgents(context.Context, *capmeshv1.ListAgentsRequest) (*capmeshv1.ListAgentsResponse, error) {
	if s.agents == nil {
		return nil, status.Error(codes.Unavailable, "agent registry is not configured")
	}
	connected := s.agents.ConnectedAgents()
	response := &capmeshv1.ListAgentsResponse{Agents: make([]*capmeshv1.AgentInfo, 0, len(connected))}
	for _, agent := range connected {
		response.Agents = append(response.Agents, &capmeshv1.AgentInfo{
			NodeName:      agent.NodeName,
			Interfaces:    agent.Interfaces,
			ConnectedAtNs: agent.ConnectedAt.UnixNano(),
			LastSeenAtNs:  agent.LastSeenAt.UnixNano(),
			Status:        "CONNECTED",
		})
	}
	return response, nil
}

func (s *CaptureServer) ListSessions(ctx context.Context, request *capmeshv1.ListSessionsRequest) (*capmeshv1.ListSessionsResponse, error) {
	sessions, err := s.sessions.List(ctx, appsession.ListInput{Status: request.GetStatus(), Mode: request.GetMode()})
	if err != nil {
		return nil, rpcError(err)
	}
	response := &capmeshv1.ListSessionsResponse{Sessions: make([]*capmeshv1.CaptureSession, 0, len(sessions))}
	for _, captureSession := range sessions {
		response.Sessions = append(response.Sessions, grpcapi.SessionToProto(captureSession))
	}
	return response, nil
}

func (s *CaptureServer) ListSessionRecordings(ctx context.Context, _ *capmeshv1.ListSessionRecordingsRequest) (*capmeshv1.ListSessionRecordingsResponse, error) {
	if s.recordings == nil {
		return nil, status.Error(codes.Unavailable, "server-side recording is not configured")
	}
	catalog, err := s.recordings.ListSessionRecordings(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response := &capmeshv1.ListSessionRecordingsResponse{
		TotalSizeBytes:         uint64(max(catalog.TotalSize, 0)),
		ConfiguredMaxSizeBytes: uint64(max(catalog.ConfiguredMax, 0)),
		Recordings:             make([]*capmeshv1.SessionRecordingInfo, 0, len(catalog.Recordings)),
	}
	for _, item := range catalog.Recordings {
		response.Recordings = append(response.Recordings, sessionRecordingToProto(item))
	}
	return response, nil
}

func (s *CaptureServer) CleanSessionRecordings(ctx context.Context, request *capmeshv1.CleanSessionRecordingsRequest) (*capmeshv1.CleanSessionRecordingsResponse, error) {
	if s.recordings == nil {
		return nil, status.Error(codes.Unavailable, "server-side recording is not configured")
	}
	result, err := s.recordings.CleanSessionRecordings(ctx, request.GetDryRun())
	if err != nil {
		if errors.Is(err, ports.ErrUnavailable) {
			return nil, rpcError(err)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	response := &capmeshv1.CleanSessionRecordingsResponse{
		ConfiguredMaxSizeBytes: uint64(max(result.ConfiguredMax, 0)),
		TargetSizeBytes:        uint64(max(result.TargetSize, 0)),
		SizeBeforeBytes:        uint64(max(result.SizeBefore, 0)),
		SizeAfterBytes:         uint64(max(result.SizeAfter, 0)),
		DeletedSizeBytes:       uint64(max(result.DeletedSize, 0)),
		DryRun:                 result.DryRun,
		Deleted:                make([]*capmeshv1.SessionRecordingInfo, 0, len(result.Deleted)),
		Failures:               make([]*capmeshv1.RecordingCleanupFailure, 0, len(result.Failures)),
	}
	for _, item := range result.Deleted {
		response.Deleted = append(response.Deleted, sessionRecordingToProto(item))
	}
	for _, failure := range result.Failures {
		response.Failures = append(response.Failures, &capmeshv1.RecordingCleanupFailure{SessionId: failure.SessionID, Directory: failure.Directory, Error: failure.Error})
	}
	s.logger.Info("normal session recording cleanup completed", "dry_run", result.DryRun, "size_before", result.SizeBefore, "size_after", result.SizeAfter, "deleted_bytes", result.DeletedSize, "deleted_directories", len(result.Deleted), "failures", len(result.Failures))
	return response, nil
}

func sessionRecordingToProto(item domain.SessionRecordingInfo) *capmeshv1.SessionRecordingInfo {
	result := &capmeshv1.SessionRecordingInfo{
		SessionId:    item.SessionID,
		Directory:    item.Directory,
		SizeBytes:    uint64(max(item.SizeBytes, 0)),
		SegmentCount: uint32(item.SegmentCount),
		Status:       item.Status,
		DesiredState: item.DesiredState,
		Active:       item.Active,
		Deletable:    item.Deletable,
	}
	if !item.CreatedAt.IsZero() {
		result.CreatedAtNs = item.CreatedAt.UnixNano()
	}
	if !item.FinishedAt.IsZero() {
		result.FinishedAtNs = item.FinishedAt.UnixNano()
	}
	return result
}

func (s *CaptureServer) StartWorkloadLogCapture(ctx context.Context, request *capmeshv1.StartWorkloadLogCaptureRequest) (*capmeshv1.WorkloadLogCapture, error) {
	if s.logCapture == nil {
		return nil, status.Error(codes.Unavailable, "workload log capture is not configured")
	}
	capture, err := s.logCapture.Start(ctx, applogcapture.StartInput{Targets: workloadLogTargetsFromProto(request.GetTargets()), SinceSeconds: request.GetSinceSeconds()})
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("workload log capture started", "run_id", capture.RunID, "targets", len(capture.Targets))
	return workloadLogCaptureToProto(capture), nil
}

func (s *CaptureServer) StopWorkloadLogCapture(ctx context.Context, _ *capmeshv1.StopWorkloadLogCaptureRequest) (*capmeshv1.WorkloadLogCapture, error) {
	if s.logCapture == nil {
		return nil, status.Error(codes.Unavailable, "workload log capture is not configured")
	}
	capture, err := s.logCapture.Stop(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	s.logger.Info("workload log capture stopped", "run_id", capture.RunID)
	return workloadLogCaptureToProto(capture), nil
}

func (s *CaptureServer) GetWorkloadLogCapture(context.Context, *capmeshv1.GetWorkloadLogCaptureRequest) (*capmeshv1.WorkloadLogCapture, error) {
	if s.logCapture == nil {
		return nil, status.Error(codes.Unavailable, "workload log capture is not configured")
	}
	return workloadLogCaptureToProto(s.logCapture.Get()), nil
}

func continuousCaptureToProto(capture appcontinuous.Capture) *capmeshv1.ContinuousCapture {
	var startedAt int64
	if !capture.StartedAt.IsZero() {
		startedAt = capture.StartedAt.UnixNano()
	}
	return &capmeshv1.ContinuousCapture{SessionId: capture.SessionID, Status: string(capture.Status), StartedAtNs: startedAt, RetainedSize: uint64(max(capture.RetainedSize, 0)), SegmentCount: uint32(capture.SegmentCount), Message: capture.Message}
}

func workloadLogTargetsFromProto(targets []*capmeshv1.WorkloadLogTarget) []domain.WorkloadLogTarget {
	result := make([]domain.WorkloadLogTarget, 0, len(targets))
	for _, target := range targets {
		if target == nil {
			continue
		}
		result = append(result, domain.WorkloadLogTarget{Namespace: target.GetNamespace(), Kind: target.GetKind(), Name: target.GetName(), Containers: append([]string(nil), target.GetContainers()...), MaxPods: target.GetMaxPods()})
	}
	return result
}

func workloadLogCaptureToProto(capture applogcapture.Capture) *capmeshv1.WorkloadLogCapture {
	result := &capmeshv1.WorkloadLogCapture{RunId: capture.RunID, Status: string(capture.Status), RetainedSize: uint64(max(capture.RetainedSize, 0)), SegmentCount: uint32(capture.SegmentCount), ActiveStreams: uint32(capture.ActiveStreams), Message: capture.Message}
	if !capture.StartedAt.IsZero() {
		result.StartedAtNs = capture.StartedAt.UnixNano()
	}
	for _, target := range capture.Targets {
		result.Targets = append(result.Targets, &capmeshv1.WorkloadLogTarget{Namespace: target.Namespace, Kind: target.Kind, Name: target.Name, Containers: append([]string(nil), target.Containers...), MaxPods: target.MaxPods})
	}
	return result
}

func rpcError(err error) error {
	var capacityErr *ports.RecordingCapacityExceededError
	if errors.As(err, &capacityErr) {
		capacity := capacityErr.Capacity
		return status.Errorf(codes.ResourceExhausted, "normal recording storage limit reached: used=%d max=%d deletable_bytes=%d deletable_sessions=%d; back up recordings you need, then run capmesh-client clean recordings --dry-run and capmesh-client clean recordings", capacity.UsedSize, capacity.MaxSize, capacity.DeletableSize, capacity.DeletableSessions)
	}
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ports.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ports.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

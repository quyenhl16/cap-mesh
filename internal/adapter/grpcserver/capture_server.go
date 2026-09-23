package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcapi"
	appsession "github.com/quyenhl16/capmesh/internal/application/session"
	"github.com/quyenhl16/capmesh/internal/core/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type CaptureServer struct {
	capmeshv1.UnimplementedCaptureServiceServer
	sessions *appsession.Service
	packets  ports.PacketPublisher
	logger   *slog.Logger
}

func NewCaptureServer(sessions *appsession.Service, packets ports.PacketPublisher, logger *slog.Logger) *CaptureServer {
	return &CaptureServer{sessions: sessions, packets: packets, logger: logger}
}

func (s *CaptureServer) CreateSession(ctx context.Context, request *capmeshv1.CreateSessionRequest) (*capmeshv1.CaptureSession, error) {
	session, err := s.sessions.Create(ctx, appsession.CreateInput{Nodes: request.GetNodes(), LogicalInterface: request.GetLogicalInterface(), Filter: request.GetFilter(), Snaplen: request.GetSnaplen(), TTL: time.Duration(request.GetTtlSeconds()) * time.Second, ReorderWindow: time.Duration(request.GetReorderWindowMs()) * time.Millisecond})
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

func rpcError(err error) error {
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

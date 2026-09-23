package grpcserver

import (
	"io"
	"log/slog"

	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcapi"
	"github.com/quyenhl16/capmesh/internal/core/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AgentServer struct {
	capmeshv1.UnimplementedAgentServiceServer
	registry *AgentRegistry
	packets  ports.PacketPublisher
	logger   *slog.Logger
}

func NewAgentServer(registry *AgentRegistry, packets ports.PacketPublisher, logger *slog.Logger) *AgentServer {
	return &AgentServer{registry: registry, packets: packets, logger: logger}
}

func (s *AgentServer) Connect(stream capmeshv1.AgentService_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	registration := first.GetRegister()
	if registration == nil || registration.GetNodeName() == "" {
		return status.Error(codes.InvalidArgument, "first agent message must register a node")
	}
	node := registration.GetNodeName()
	sequences := make(map[string]uint64)
	connection := s.registry.register(node)
	defer s.registry.unregister(node, connection)
	s.logger.Info("agent connected", "node", node, "interfaces", registration.GetInterfaces())
	defer s.logger.Info("agent disconnected", "node", node)

	incoming := make(chan *capmeshv1.AgentMessage)
	receiveErrors := make(chan error, 1)
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				receiveErrors <- err
				return
			}
			select {
			case incoming <- message:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		select {
		case command := <-connection.commands:
			if err := stream.Send(commandToProto(command)); err != nil {
				return err
			}
		case message := <-incoming:
			if batch := message.GetBatch(); batch != nil {
				key := batch.GetSessionId() + "/" + batch.GetInterfaceName()
				for _, packet := range batch.GetPackets() {
					if previous := sequences[key]; previous != 0 && packet.GetSequenceNumber() != previous+1 {
						s.logger.Warn("packet sequence gap", "node", node, "session_id", batch.GetSessionId(), "interface", batch.GetInterfaceName(), "expected", previous+1, "actual", packet.GetSequenceNumber())
					}
					sequences[key] = packet.GetSequenceNumber()
				}
				s.packets.Publish(grpcapi.PacketBatchFromProto(batch))
			}
			if captureStatus := message.GetStatus(); captureStatus != nil {
				s.logger.Info("agent capture status", "node", node, "session_id", captureStatus.GetSessionId(), "state", captureStatus.GetState(), "error", captureStatus.GetError())
			}
		case err := <-receiveErrors:
			if err == io.EOF {
				return nil
			}
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func commandToProto(command ports.AgentCommand) *capmeshv1.AgentCommand {
	if command.Kind == "stop" {
		return &capmeshv1.AgentCommand{Stop: &capmeshv1.StopCapture{SessionId: command.SessionID}}
	}
	return &capmeshv1.AgentCommand{Start: &capmeshv1.StartCapture{SessionId: command.SessionID, LogicalInterface: command.LogicalInterface, Filter: command.Filter, Snaplen: command.Snaplen}}
}

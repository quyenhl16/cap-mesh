package grpcclient

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcapi"
	"github.com/quyenhl16/capmesh/internal/core/domain"
	"github.com/quyenhl16/capmesh/internal/core/ports"
	"google.golang.org/grpc"
)

type Reporter struct {
	mu        sync.RWMutex
	connected bool
	outbound  chan *capmeshv1.AgentMessage
}

func NewReporter(queueSize int) *Reporter {
	return &Reporter{outbound: make(chan *capmeshv1.AgentMessage, queueSize)}
}

func (r *Reporter) SendBatch(ctx context.Context, batch domain.PacketBatch) error {
	return r.send(ctx, &capmeshv1.AgentMessage{Batch: grpcapi.PacketBatchToProto(batch)})
}

func (r *Reporter) SendStatus(ctx context.Context, sessionID, state, message string) error {
	return r.send(ctx, &capmeshv1.AgentMessage{Status: &capmeshv1.CaptureStatus{SessionId: sessionID, State: state, Error: message}})
}

func (r *Reporter) send(ctx context.Context, message *capmeshv1.AgentMessage) error {
	r.mu.RLock()
	connected := r.connected
	r.mu.RUnlock()
	if !connected {
		return ports.ErrUnavailable
	}
	select {
	case r.outbound <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reporter) setConnected(value bool) {
	r.mu.Lock()
	r.connected = value
	r.mu.Unlock()
	if !value {
		for {
			select {
			case <-r.outbound:
			default:
				return
			}
		}
	}
}

type AgentTransport struct {
	client   capmeshv1.AgentServiceClient
	reporter *Reporter
	token    string
}

func NewAgentTransport(connection *grpc.ClientConn, reporter *Reporter, token string) *AgentTransport {
	return &AgentTransport{client: capmeshv1.NewAgentServiceClient(connection), reporter: reporter, token: token}
}

func (t *AgentTransport) Connect(ctx context.Context, node string, interfaces map[string]string, handle func(*capmeshv1.AgentCommand) error) error {
	stream, err := t.client.Connect(AuthContext(ctx, t.token))
	if err != nil {
		return err
	}
	if err := stream.Send(&capmeshv1.AgentMessage{Register: &capmeshv1.RegisterAgent{NodeName: node, Interfaces: interfaces}}); err != nil {
		return err
	}
	t.reporter.setConnected(true)
	defer t.reporter.setConnected(false)

	incoming := make(chan *capmeshv1.AgentCommand)
	receiveErrors := make(chan error, 1)
	go func() {
		for {
			command, err := stream.Recv()
			if err != nil {
				receiveErrors <- err
				return
			}
			select {
			case incoming <- command:
			case <-ctx.Done():
				return
			}
		}
	}()
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case message := <-t.reporter.outbound:
			if err := stream.Send(message); err != nil {
				return err
			}
		case command := <-incoming:
			if err := handle(command); err != nil {
				if start := command.GetStart(); start != nil {
					_ = t.reporter.SendStatus(ctx, start.GetSessionId(), "FAILED", err.Error())
				}
			}
		case <-heartbeat.C:
			if err := stream.Send(&capmeshv1.AgentMessage{Heartbeat: &capmeshv1.Heartbeat{TimestampNs: time.Now().UnixNano()}}); err != nil {
				return err
			}
		case err := <-receiveErrors:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

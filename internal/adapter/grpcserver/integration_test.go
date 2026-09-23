package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"github.com/quyenhl16/cap-mesh/internal/adapter/memory"
	metricadapter "github.com/quyenhl16/cap-mesh/internal/adapter/metrics"
	appsession "github.com/quyenhl16/cap-mesh/internal/application/session"
	appstream "github.com/quyenhl16/cap-mesh/internal/application/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestCaptureFlowOverGRPC(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packets := appstream.NewService(metricadapter.Noop{})
	agents := NewAgentRegistry()
	sessions := appsession.NewService(memory.NewSessionRepository(), agents, packets, 10)
	server := grpc.NewServer()
	capmeshv1.RegisterAgentServiceServer(server, NewAgentServer(agents, packets, logger))
	capmeshv1.RegisterCaptureServiceServer(server, NewCaptureServer(sessions, packets, logger))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///capmesh", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	agentStream, err := capmeshv1.NewAgentServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentStream.Send(&capmeshv1.AgentMessage{Register: &capmeshv1.RegisterAgent{NodeName: "worker-1", Interfaces: map[string]string{"A": "eth0"}}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(agents.ConnectedNodes()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	captureClient := capmeshv1.NewCaptureServiceClient(connection)
	session, err := captureClient.CreateSession(ctx, &capmeshv1.CreateSessionRequest{Nodes: []string{"worker-1"}, LogicalInterface: "A", Snaplen: 256, TtlSeconds: 60})
	if err != nil {
		t.Fatal(err)
	}
	command, err := agentStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if command.GetStart().GetSessionId() != session.GetId() {
		t.Fatalf("unexpected start command: %#v", command)
	}

	packetStream, err := captureClient.StreamPackets(ctx, &capmeshv1.StreamPacketsRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := packetStream.Header(); err != nil {
		t.Fatal(err)
	}
	wantData := []byte{0, 1, 2, 3}
	err = agentStream.Send(&capmeshv1.AgentMessage{Batch: &capmeshv1.PacketBatch{SessionId: session.GetId(), NodeName: "worker-1", InterfaceName: "eth0", Packets: []*capmeshv1.Packet{{TimestampNs: time.Now().Add(-time.Second).UnixNano(), CapturedLength: 4, OriginalLength: 4, LinkType: 1, SequenceNumber: 1, Data: wantData}}}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := packetStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.GetPackets()) != 1 || string(batch.GetPackets()[0].GetData()) != string(wantData) {
		t.Fatalf("unexpected packet batch: %#v", batch)
	}

	stopped, err := captureClient.StopSession(ctx, &capmeshv1.StopSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if stopped.GetStatus() != "STOPPED" {
		t.Fatalf("unexpected stopped session: %#v", stopped)
	}
	command, err = agentStream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if command.GetStop().GetSessionId() != session.GetId() {
		t.Fatalf("unexpected stop command: %#v", command)
	}
}

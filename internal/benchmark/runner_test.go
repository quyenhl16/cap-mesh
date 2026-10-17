package benchmark

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type benchmarkTestServer struct {
	capmeshv1.UnimplementedCaptureServiceServer
	mu          sync.Mutex
	subscribers []chan *capmeshv1.PacketBatch
	stopped     bool
}

func (s *benchmarkTestServer) CreateSession(context.Context, *capmeshv1.CreateSessionRequest) (*capmeshv1.CaptureSession, error) {
	return &capmeshv1.CaptureSession{Id: "test-session", Status: "RUNNING", Nodes: []string{"capture-node"}}, nil
}

func (s *benchmarkTestServer) StreamPackets(_ *capmeshv1.StreamPacketsRequest, stream capmeshv1.CaptureService_StreamPacketsServer) error {
	packets := make(chan *capmeshv1.PacketBatch, 1024)
	s.mu.Lock()
	s.subscribers = append(s.subscribers, packets)
	s.mu.Unlock()
	if err := stream.SendHeader(metadata.Pairs("capmesh-session-id", "test-session")); err != nil {
		return err
	}
	for batch := range packets {
		if err := stream.Send(batch); err != nil {
			return err
		}
	}
	return nil
}

func (s *benchmarkTestServer) StopSession(context.Context, *capmeshv1.StopSessionRequest) (*capmeshv1.CaptureSession, error) {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		for _, subscriber := range s.subscribers {
			close(subscriber)
		}
	}
	s.mu.Unlock()
	return &capmeshv1.CaptureSession{Id: "test-session", Status: "STOPPED"}, nil
}

func (s *benchmarkTestServer) publish(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	for _, subscriber := range s.subscribers {
		copyOfData := append([]byte(nil), data...)
		subscriber <- &capmeshv1.PacketBatch{Packets: []*capmeshv1.Packet{{Data: copyOfData}}}
	}
}

func TestRunEndToEndWithTestServer(t *testing.T) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	fake := &benchmarkTestServer{}
	udpDone := make(chan struct{})
	go func() {
		defer close(udpDone)
		buffer := make([]byte, 65535)
		for {
			length, _, err := udp.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			fake.publish(buffer[:length])
		}
	}()

	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	capmeshv1.RegisterCaptureServiceServer(server, fake)
	go server.Serve(tcp)
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := Run(ctx, RunConfig{
		Server: tcp.Addr().String(), Insecure: true,
		Receiver: udp.LocalAddr().String(), TargetNamespace: "capmesh",
		TargetKind: "deployment", TargetName: "receiver",
		PacketSize: 64, PacketRate: 1000, Duration: 50 * time.Millisecond,
		Settle: 10 * time.Millisecond, Drain: 50 * time.Millisecond,
		Subscribers: 2, Snaplen: 65535, MaxLossPercent: 0,
		Scenario: "test", Version: "test", GitCommit: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Verdict != "PASS" || result.Metrics.PacketsSent != 50 || result.Metrics.PacketsCaptured != 50 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(result.Metrics.SubscriberPackets) != 2 || result.Metrics.SubscriberPackets[1] != 50 {
		t.Fatalf("unexpected subscriber counts: %v", result.Metrics.SubscriberPackets)
	}
	_ = udp.Close()
	<-udpDone
}

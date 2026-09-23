package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	capmeshv1 "github.com/quyenhl16/capmesh/api/capmesh/v1"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcapi"
	"github.com/quyenhl16/capmesh/internal/adapter/grpcclient"
	"github.com/quyenhl16/capmesh/internal/adapter/pcapng"
)

func main() {
	server := flag.String("server", "127.0.0.1:8443", "capmesh-server address")
	sessionID := flag.String("session", "", "existing capture session ID")
	create := flag.Bool("create", false, "create a session before subscribing")
	nodes := flag.String("nodes", "", "comma-separated node names for a new session")
	logicalInterface := flag.String("interface", "A", "logical interface A, B, or C")
	filter := flag.String("filter", "", "BPF capture filter")
	snaplen := flag.Uint("snaplen", 256, "packet snapshot length")
	ttl := flag.Duration("ttl", 5*time.Minute, "capture session lifetime")
	reorderWindow := flag.Duration("reorder-window", 300*time.Millisecond, "packet reorder window")
	token := flag.String("token", os.Getenv("CAPMESH_TOKEN"), "shared bearer token")
	insecureTransport := flag.Bool("insecure", false, "disable TLS (local development only)")
	caFile := flag.String("tls-ca", "", "server CA certificate")
	serverName := flag.String("tls-server-name", "", "TLS server name override")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if !*create && *sessionID == "" {
		logger.Error("--session is required unless --create is used")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	connection, err := grpcclient.Dial(grpcclient.DialConfig{Address: *server, Insecure: *insecureTransport, CAFile: *caFile, ServerName: *serverName, Token: *token})
	if err != nil {
		logger.Error("connect failed", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	client := capmeshv1.NewCaptureServiceClient(connection)
	authContext := grpcclient.AuthContext(ctx, *token)
	created := false
	if *create {
		request := &capmeshv1.CreateSessionRequest{Nodes: splitNodes(*nodes), LogicalInterface: strings.ToUpper(*logicalInterface), Filter: *filter, Snaplen: uint32(*snaplen), TtlSeconds: uint32(ttl.Seconds()), ReorderWindowMs: uint32(reorderWindow.Milliseconds())}
		session, err := client.CreateSession(authContext, request)
		if err != nil {
			logger.Error("create session failed", "error", err)
			os.Exit(1)
		}
		*sessionID = session.GetId()
		created = true
		logger.Info("capture session created", "session_id", *sessionID)
	}
	if created {
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_, _ = client.StopSession(grpcclient.AuthContext(stopCtx, *token), &capmeshv1.StopSessionRequest{SessionId: *sessionID})
		}()
	}
	stream, err := client.StreamPackets(authContext, &capmeshv1.StreamPacketsRequest{SessionId: *sessionID})
	if err != nil {
		logger.Error("subscribe failed", "error", err)
		os.Exit(1)
	}
	writer := pcapng.NewWriter(os.Stdout, 64, 50*time.Millisecond)
	packetsReceived := 0
	type streamResult struct {
		batch *capmeshv1.PacketBatch
		err   error
	}
	incoming := make(chan streamResult)
	go func() {
		for {
			batch, err := stream.Recv()
			select {
			case incoming <- streamResult{batch: batch, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	flushTicker := time.NewTicker(50 * time.Millisecond)
	defer flushTicker.Stop()
	streaming := true
	for streaming {
		select {
		case result := <-incoming:
			if result.err != nil {
				if !errors.Is(result.err, io.EOF) && ctx.Err() == nil {
					logger.Error("packet stream failed", "error", result.err)
					os.Exit(1)
				}
				streaming = false
				continue
			}
			domainBatch := grpcapi.PacketBatchFromProto(result.batch)
			if err := writer.WriteBatch(domainBatch); err != nil {
				logger.Error("PCAPNG output failed", "error", err)
				os.Exit(1)
			}
			packetsReceived += len(domainBatch.Packets)
		case <-flushTicker.C:
			if err := writer.Flush(); err != nil {
				logger.Error("PCAPNG output failed", "error", err)
				os.Exit(1)
			}
		case <-ctx.Done():
			streaming = false
		}
	}
	if err := writer.Flush(); err != nil {
		logger.Error("flush PCAPNG failed", "error", err)
		os.Exit(1)
	}
	logger.Info("capture stream completed", "session_id", *sessionID, "packets_received", packetsReceived, "pcapng_blocks_written", packetsReceived)
}

func splitNodes(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	var nodes []string
	for _, node := range strings.Split(value, ",") {
		if node = strings.TrimSpace(node); node != "" {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

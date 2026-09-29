package recording

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gopacket/gopacket/pcapgo"
	metricadapter "github.com/quyenhl16/cap-mesh/internal/adapter/metrics"
	appstream "github.com/quyenhl16/cap-mesh/internal/application/stream"
	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func TestManagerRotatesAndFinalizesRecording(t *testing.T) {
	root := t.TempDir()
	packets := appstream.NewService(metricadapter.Noop{})
	if err := packets.OpenSession("session-test", 0, 128); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(Config{Directory: root, SegmentSize: 600, MaxSessionSize: 0, QueueSize: 128, FlushInterval: time.Millisecond, SyncInterval: time.Hour}, packets, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	session := domain.Session{ID: "session-test", Nodes: []string{"worker-1"}, LogicalInterface: "management", Snaplen: 256, CreatedAt: time.Now().UTC()}
	if err := manager.Start(session); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		packets.Publish(testBatch(session.ID, uint64(i+1), 256))
	}
	packets.CloseSession(session.ID)

	result := waitForMetadata(t, root, session)
	if result.Status != "COMPLETED" {
		t.Fatalf("status = %q, want COMPLETED (error: %s)", result.Status, result.Error)
	}
	if len(result.Segments) < 2 {
		t.Fatalf("segments = %d, want at least 2", len(result.Segments))
	}
	if result.PacketCount != 12 {
		t.Fatalf("packet count = %d, want 12", result.PacketCount)
	}
	for _, segment := range result.Segments {
		path := filepath.Join(root, session.CreatedAt.Format("2006-01-02"), session.ID, segment.File)
		assertReadablePCAPNG(t, path)
	}
}

func TestManagerStopsAtSessionSizeLimit(t *testing.T) {
	root := t.TempDir()
	packets := appstream.NewService(metricadapter.Noop{})
	if err := packets.OpenSession("session-limit", 0, 128); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(Config{Directory: root, SegmentSize: 0, MaxSessionSize: 600, QueueSize: 128, FlushInterval: time.Millisecond, SyncInterval: time.Hour}, packets, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	session := domain.Session{ID: "session-limit", Nodes: []string{"worker-1"}, LogicalInterface: "data", Snaplen: 256, CreatedAt: time.Now().UTC()}
	if err := manager.Start(session); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		packets.Publish(testBatch(session.ID, uint64(i+1), 256))
	}
	result := waitForMetadata(t, root, session)
	packets.CloseSession(session.ID)
	if result.Status != "TRUNCATED" || !result.Truncated {
		t.Fatalf("unexpected metadata: status=%q truncated=%v error=%q", result.Status, result.Truncated, result.Error)
	}
	if result.Error != "max_session_size_reached" {
		t.Fatalf("error = %q, want max_session_size_reached", result.Error)
	}
}

func testBatch(sessionID string, sequence uint64, size int) domain.PacketBatch {
	data := make([]byte, size)
	return domain.PacketBatch{
		SessionID:     sessionID,
		NodeName:      "worker-1",
		InterfaceName: "eth0",
		Packets: []domain.Packet{{
			Timestamp:      time.Now().Add(-time.Second),
			CapturedLength: uint32(size),
			OriginalLength: uint32(size),
			LinkType:       1,
			SequenceNumber: sequence,
			Data:           data,
		}},
	}
}

func waitForMetadata(t *testing.T, root string, session domain.Session) metadata {
	t.Helper()
	path := filepath.Join(root, session.CreatedAt.Format("2006-01-02"), session.ID, "metadata.json")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var result metadata
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
	return metadata{}
}

func assertReadablePCAPNG(t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := pcapgo.NewNgReader(file, pcapgo.DefaultNgReaderOptions)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if _, _, err := reader.ReadPacketData(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
}

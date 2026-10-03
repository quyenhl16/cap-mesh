package recording

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

func TestSessionRecordingCatalogListsOnlyNormalDirectories(t *testing.T) {
	root := t.TempDir()
	manager := catalogTestManager(t, root, 1024)
	created := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	createCatalogRecording(t, root, "2026-10-01", "session-old", created, "COMPLETED", "STOPPED", 100)
	if err := os.MkdirAll(filepath.Join(root, "continuous"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "continuous", "trace.pcapng"), make([]byte, 500), 0o640); err != nil {
		t.Fatal(err)
	}

	catalog, err := manager.ListSessionRecordings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Recordings) != 1 {
		t.Fatalf("recordings = %#v", catalog.Recordings)
	}
	item := catalog.Recordings[0]
	if item.SessionID != "session-old" || item.Directory != "2026-10-01/session-old" || !item.Deletable || item.SegmentCount != 1 {
		t.Fatalf("unexpected recording: %#v", item)
	}
	if catalog.TotalSize <= 100 || catalog.ConfiguredMax != 1024 {
		t.Fatalf("unexpected catalog totals: %#v", catalog)
	}
}

func TestCleanSessionRecordingsDeletesOldestToHalfAndSupportsDryRun(t *testing.T) {
	root := t.TempDir()
	manager := catalogTestManager(t, root, 1)
	base := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	createCatalogRecording(t, root, "2026-10-01", "session-old", base, "COMPLETED", "STOPPED", 300)
	createCatalogRecording(t, root, "2026-10-02", "session-middle", base.Add(time.Hour), "TRUNCATED", "STOPPED", 300)
	createCatalogRecording(t, root, "2026-10-03", "session-new", base.Add(2*time.Hour), "COMPLETED", "STOPPED", 300)
	catalog, err := manager.ListSessionRecordings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager.config.MaxNormalTotal = catalog.TotalSize

	preview, err := manager.CleanSessionRecordings(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Deleted) == 0 || preview.Deleted[0].SessionID != "session-old" || preview.SizeAfter > preview.TargetSize {
		t.Fatalf("unexpected cleanup preview: %#v", preview)
	}
	if _, err := os.Stat(filepath.Join(root, "2026-10-01", "session-old")); err != nil {
		t.Fatalf("dry-run deleted a directory: %v", err)
	}

	result, err := manager.CleanSessionRecordings(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != len(preview.Deleted) || result.SizeAfter > result.TargetSize {
		t.Fatalf("unexpected cleanup result: %#v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "2026-10-01", "session-old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest directory still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "2026-10-03", "session-new")); err != nil {
		t.Fatalf("newest directory was deleted: %v", err)
	}
}

func TestCapacityAndCleanupProtectRecoverableRecording(t *testing.T) {
	root := t.TempDir()
	manager := catalogTestManager(t, root, 1)
	created := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	createCatalogRecording(t, root, "2026-10-01", "session-running", created, "PARTIAL", "RUNNING", 300)
	createCatalogRecording(t, root, "2026-10-02", "session-active", created.Add(time.Hour), "COMPLETED", "STOPPED", 300)
	manager.mu.Lock()
	manager.active["session-active"] = nil
	manager.mu.Unlock()
	catalog, err := manager.ListSessionRecordings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager.config.MaxNormalTotal = catalog.TotalSize
	var capacityErr *ports.RecordingCapacityExceededError
	if err := manager.CheckNormalRecordingCapacity(context.Background()); !errors.As(err, &capacityErr) {
		t.Fatalf("capacity error = %v", err)
	}
	if capacityErr.Capacity.DeletableSessions != 0 {
		t.Fatalf("recoverable session reported deletable: %#v", capacityErr.Capacity)
	}
	result, err := manager.CleanSessionRecordings(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Deleted) != 0 || result.SizeAfter <= result.TargetSize {
		t.Fatalf("recoverable session was cleaned: %#v", result)
	}
}

func catalogTestManager(t *testing.T, root string, maxTotal int64) *Manager {
	t.Helper()
	manager, err := NewManager(Config{Directory: root, SegmentSize: 1024, MaxSessionSize: 4096, MaxNormalTotal: maxTotal, QueueSize: 8, FlushInterval: time.Second, SyncInterval: time.Second}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func createCatalogRecording(t *testing.T, root, date, sessionID string, created time.Time, status, desired string, payloadSize int) {
	t.Helper()
	directory := filepath.Join(root, date, sessionID)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	finished := created.Add(time.Minute)
	meta := metadata{SchemaVersion: 1, SessionID: sessionID, Status: status, StartedAt: created, FinishedAt: &finished}
	data, err := jsonMarshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "metadata.json"), data, 0o640); err != nil {
		t.Fatal(err)
	}
	state := recoveryMetadata{SchemaVersion: 1, DesiredState: desired, Generation: 1, Session: domain.Session{ID: sessionID, CreatedAt: created, Status: domain.SessionStopped}}
	if err := writeRecovery(filepath.Join(directory, recoveryFile), state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "capture-000001.pcapng"), make([]byte, payloadSize), 0o640); err != nil {
		t.Fatal(err)
	}
}

func jsonMarshal(value any) ([]byte, error) {
	// Keep test fixtures encoded exactly as production metadata.
	return json.MarshalIndent(value, "", "  ")
}

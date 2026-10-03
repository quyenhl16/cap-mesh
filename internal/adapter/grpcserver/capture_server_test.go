package grpcserver

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	capmeshv1 "github.com/quyenhl16/cap-mesh/api/capmesh/v1"
)

type fakeRecordingCatalog struct {
	catalog domain.SessionRecordingCatalog
	cleanup domain.RecordingCleanupResult
}

func (f fakeRecordingCatalog) ListSessionRecordings(context.Context) (domain.SessionRecordingCatalog, error) {
	return f.catalog, nil
}
func (f fakeRecordingCatalog) CleanSessionRecordings(context.Context, bool) (domain.RecordingCleanupResult, error) {
	return f.cleanup, nil
}
func (f fakeRecordingCatalog) CheckNormalRecordingCapacity(context.Context) error { return nil }

func TestRPCErrorMapsRecordingCapacityToResourceExhausted(t *testing.T) {
	err := rpcError(&ports.RecordingCapacityExceededError{Capacity: domain.RecordingCapacity{UsedSize: 120, MaxSize: 100, DeletableSize: 80, DeletableSessions: 2}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %s", status.Code(err))
	}
	message := status.Convert(err).Message()
	if !strings.Contains(message, "used=120") || !strings.Contains(message, "back up") || !strings.Contains(message, "clean recordings") {
		t.Fatalf("unexpected message: %q", message)
	}
}

func TestRecordingCatalogRPCsExposeSizesAndCleanupPlan(t *testing.T) {
	item := domain.SessionRecordingInfo{SessionID: "session-old", Directory: "2026-10-01/session-old", SizeBytes: 60, SegmentCount: 2, Status: "COMPLETED", Deletable: true}
	server := &CaptureServer{
		recordings: fakeRecordingCatalog{
			catalog: domain.SessionRecordingCatalog{Recordings: []domain.SessionRecordingInfo{item}, TotalSize: 100, ConfiguredMax: 100},
			cleanup: domain.RecordingCleanupResult{ConfiguredMax: 100, TargetSize: 50, SizeBefore: 100, SizeAfter: 40, DeletedSize: 60, Deleted: []domain.SessionRecordingInfo{item}, DryRun: true},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	listed, err := server.ListSessionRecordings(context.Background(), &capmeshv1.ListSessionRecordingsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if listed.GetTotalSizeBytes() != 100 || len(listed.GetRecordings()) != 1 || listed.GetRecordings()[0].GetDirectory() != item.Directory {
		t.Fatalf("unexpected list response: %#v", listed)
	}
	cleaned, err := server.CleanSessionRecordings(context.Background(), &capmeshv1.CleanSessionRecordingsRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !cleaned.GetDryRun() || cleaned.GetTargetSizeBytes() != 50 || len(cleaned.GetDeleted()) != 1 {
		t.Fatalf("unexpected cleanup response: %#v", cleaned)
	}
}

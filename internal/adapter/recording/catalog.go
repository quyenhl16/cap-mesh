package recording

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
	"github.com/quyenhl16/cap-mesh/internal/core/ports"
)

var dateDirectoryPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

type scannedRecording struct {
	info      domain.SessionRecordingInfo
	path      string
	oldestKey time.Time
}

func (m *Manager) ListSessionRecordings(ctx context.Context) (domain.SessionRecordingCatalog, error) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()
	return m.listSessionRecordings(ctx)
}

func (m *Manager) CheckNormalRecordingCapacity(ctx context.Context) error {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()
	if m.config.MaxNormalTotal == 0 {
		return nil
	}
	catalog, err := m.listSessionRecordings(ctx)
	if err != nil {
		return fmt.Errorf("%w: inspect normal recording storage: %v", ports.ErrUnavailable, err)
	}
	if catalog.TotalSize < catalog.ConfiguredMax {
		return nil
	}
	capacity := domain.RecordingCapacity{UsedSize: catalog.TotalSize, MaxSize: catalog.ConfiguredMax}
	for _, item := range catalog.Recordings {
		if item.Deletable {
			capacity.DeletableSize += item.SizeBytes
			capacity.DeletableSessions++
		}
	}
	return &ports.RecordingCapacityExceededError{Capacity: capacity}
}

func (m *Manager) CleanSessionRecordings(ctx context.Context, dryRun bool) (domain.RecordingCleanupResult, error) {
	m.storageMu.Lock()
	defer m.storageMu.Unlock()
	if m.config.MaxNormalTotal <= 0 {
		return domain.RecordingCleanupResult{}, fmt.Errorf("%w: normal recording total-size limit is disabled", ports.ErrUnavailable)
	}
	catalog, scanned, err := m.scanSessionRecordings(ctx)
	if err != nil {
		return domain.RecordingCleanupResult{}, err
	}
	result := domain.RecordingCleanupResult{
		ConfiguredMax: catalog.ConfiguredMax,
		TargetSize:    catalog.ConfiguredMax / 2,
		SizeBefore:    catalog.TotalSize,
		SizeAfter:     catalog.TotalSize,
		DryRun:        dryRun,
	}
	sort.Slice(scanned, func(i, j int) bool {
		if scanned[i].oldestKey.Equal(scanned[j].oldestKey) {
			return scanned[i].info.SessionID < scanned[j].info.SessionID
		}
		return scanned[i].oldestKey.Before(scanned[j].oldestKey)
	})
	for _, item := range scanned {
		if result.SizeAfter <= result.TargetSize {
			break
		}
		if !item.info.Deletable {
			continue
		}
		if dryRun {
			result.Deleted = append(result.Deleted, item.info)
			result.DeletedSize += item.info.SizeBytes
			result.SizeAfter = max(result.SizeAfter-item.info.SizeBytes, 0)
			continue
		}
		if err := m.validateDeletionCandidate(item); err != nil {
			result.Failures = append(result.Failures, domain.RecordingCleanupFailure{SessionID: item.info.SessionID, Directory: item.info.Directory, Error: err.Error()})
			continue
		}
		if err := os.RemoveAll(item.path); err != nil {
			result.Failures = append(result.Failures, domain.RecordingCleanupFailure{SessionID: item.info.SessionID, Directory: item.info.Directory, Error: err.Error()})
			continue
		}
		_ = os.Remove(filepath.Dir(item.path)) // Removes the date directory only when it is empty.
		result.Deleted = append(result.Deleted, item.info)
		result.DeletedSize += item.info.SizeBytes
		result.SizeAfter = max(result.SizeAfter-item.info.SizeBytes, 0)
		if m.metrics != nil {
			m.metrics.cleanupDeleted.Inc()
			m.metrics.cleanupBytes.Add(float64(item.info.SizeBytes))
		}
	}
	if !dryRun {
		after, err := m.listSessionRecordings(ctx)
		if err != nil {
			return result, err
		}
		result.SizeAfter = after.TotalSize
	}
	if result.SizeAfter > result.TargetSize {
		result.Failures = append(result.Failures, domain.RecordingCleanupFailure{Error: "cleanup target cannot be reached because remaining recordings are active, recoverable, or otherwise not deletable"})
	}
	if m.metrics != nil {
		m.metrics.normalStorage.Set(float64(result.SizeAfter))
		if len(result.Failures) > 0 {
			m.metrics.cleanupFailures.Add(float64(len(result.Failures)))
		}
	}
	return result, nil
}

func (m *Manager) listSessionRecordings(ctx context.Context) (domain.SessionRecordingCatalog, error) {
	catalog, _, err := m.scanSessionRecordings(ctx)
	return catalog, err
}

func (m *Manager) scanSessionRecordings(ctx context.Context) (domain.SessionRecordingCatalog, []scannedRecording, error) {
	active := m.activeSessionIDs()
	dateEntries, err := os.ReadDir(m.config.Directory)
	if err != nil {
		return domain.SessionRecordingCatalog{}, nil, err
	}
	var scanned []scannedRecording
	for _, dateEntry := range dateEntries {
		if err := ctx.Err(); err != nil {
			return domain.SessionRecordingCatalog{}, nil, err
		}
		if !dateEntry.IsDir() || !dateDirectoryPattern.MatchString(dateEntry.Name()) {
			continue
		}
		datePath := filepath.Join(m.config.Directory, dateEntry.Name())
		sessionEntries, err := os.ReadDir(datePath)
		if err != nil {
			return domain.SessionRecordingCatalog{}, nil, fmt.Errorf("scan recording date directory %s: %w", dateEntry.Name(), err)
		}
		for _, sessionEntry := range sessionEntries {
			if !sessionEntry.IsDir() || !safeSessionID.MatchString(sessionEntry.Name()) {
				continue
			}
			path := filepath.Join(datePath, sessionEntry.Name())
			item, err := m.inspectSessionDirectory(ctx, path, filepath.ToSlash(filepath.Join(dateEntry.Name(), sessionEntry.Name())), active[sessionEntry.Name()])
			if err != nil {
				return domain.SessionRecordingCatalog{}, nil, err
			}
			scanned = append(scanned, item)
		}
	}
	sort.Slice(scanned, func(i, j int) bool {
		if scanned[i].oldestKey.Equal(scanned[j].oldestKey) {
			return scanned[i].info.SessionID < scanned[j].info.SessionID
		}
		return scanned[i].oldestKey.Before(scanned[j].oldestKey)
	})
	catalog := domain.SessionRecordingCatalog{ConfiguredMax: m.config.MaxNormalTotal, Recordings: make([]domain.SessionRecordingInfo, 0, len(scanned))}
	for _, item := range scanned {
		catalog.Recordings = append(catalog.Recordings, item.info)
		catalog.TotalSize += item.info.SizeBytes
	}
	if m.metrics != nil {
		m.metrics.normalStorage.Set(float64(catalog.TotalSize))
		m.metrics.normalDirectories.Set(float64(len(catalog.Recordings)))
	}
	return catalog, scanned, nil
}

func (m *Manager) inspectSessionDirectory(ctx context.Context, path, relative string, active bool) (scannedRecording, error) {
	size, segments, hasSymlink, err := directoryUsage(ctx, path)
	if err != nil {
		return scannedRecording{}, fmt.Errorf("inspect recording directory %s: %w", relative, err)
	}
	item := domain.SessionRecordingInfo{SessionID: filepath.Base(path), Directory: relative, SizeBytes: size, SegmentCount: segments, Active: active}
	meta, metadataOK := readSessionMetadata(path)
	if metadataOK {
		item.SessionID = meta.SessionID
		item.CreatedAt = meta.StartedAt
		if meta.FinishedAt != nil {
			item.FinishedAt = *meta.FinishedAt
		}
		item.Status = meta.Status
	}
	recovery, recoveryErr := readRecovery(filepath.Join(path, recoveryFile))
	if recoveryErr == nil {
		item.DesiredState = recovery.DesiredState
		if item.CreatedAt.IsZero() {
			item.CreatedAt = recovery.Session.CreatedAt
		}
	} else if !errors.Is(recoveryErr, os.ErrNotExist) {
		item.DesiredState = "UNKNOWN"
	}
	item.Deletable = metadataOK && terminalRecordingStatus(item.Status) && !item.Active && item.DesiredState != desiredRunning && item.DesiredState != "UNKNOWN" && !hasSymlink
	order := item.FinishedAt
	if order.IsZero() {
		order = item.CreatedAt
	}
	if order.IsZero() {
		if info, statErr := os.Stat(path); statErr == nil {
			order = info.ModTime().UTC()
		}
	}
	return scannedRecording{info: item, path: path, oldestKey: order}, nil
}

func (m *Manager) validateDeletionCandidate(item scannedRecording) error {
	if m.isSessionActive(item.info.SessionID) {
		return errors.New("session became active")
	}
	parts := strings.Split(filepath.ToSlash(item.info.Directory), "/")
	if len(parts) != 2 || !dateDirectoryPattern.MatchString(parts[0]) || !safeSessionID.MatchString(parts[1]) {
		return errors.New("unsafe recording directory")
	}
	expected := filepath.Clean(filepath.Join(m.config.Directory, filepath.FromSlash(item.info.Directory)))
	if expected != filepath.Clean(item.path) {
		return errors.New("recording directory escaped configured root")
	}
	state, err := readRecovery(filepath.Join(item.path, recoveryFile))
	if err == nil && state.DesiredState == desiredRunning {
		return errors.New("session is marked for recovery")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read recovery state: %w", err)
	}
	return nil
}

func (m *Manager) activeSessionIDs() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]bool, len(m.active))
	for id := range m.active {
		result[id] = true
	}
	return result
}

func (m *Manager) isSessionActive(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, active := m.active[id]
	return active
}

func readSessionMetadata(directory string) (metadata, bool) {
	for _, name := range []string{"metadata.json.part", "metadata.json"} {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			continue
		}
		var result metadata
		if json.Unmarshal(data, &result) == nil && result.SessionID != "" {
			return result, true
		}
	}
	return metadata{}, false
}

func directoryUsage(ctx context.Context, root string) (int64, int, bool, error) {
	var size int64
	segments := 0
	hasSymlink := false
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			hasSymlink = true
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size += info.Size()
			if strings.HasSuffix(entry.Name(), ".pcapng") {
				segments++
			}
		}
		return nil
	})
	return size, segments, hasSymlink, err
}

func terminalRecordingStatus(status string) bool {
	switch status {
	case "COMPLETED", "STOPPED", "PARTIAL", "FAILED", "TRUNCATED":
		return true
	default:
		return false
	}
}

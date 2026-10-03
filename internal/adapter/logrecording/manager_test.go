package logrecording

import (
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/quyenhl16/cap-mesh/internal/core/domain"
)

func TestFactoryPersistsDesiredStateAcrossRecorderClose(t *testing.T) {
	root := t.TempDir()
	config := Config{Directory: root, SegmentSize: 1024, MaxRetainedSize: 4096, SyncInterval: time.Hour}
	factory, err := NewFactory(config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	run := domain.LogCaptureRun{RunID: "recover-run", StartedAt: time.Now().UTC(), SinceSeconds: 30, Targets: []domain.WorkloadLogTarget{{Namespace: "ns", Kind: "deployment", Name: "api"}}}
	recorder, err := factory.Start(run)
	if err != nil {
		t.Fatal(err)
	}
	record := domain.LogRecord{Target: run.Targets[0], PodName: "api-0", PodUID: "uid-1", Container: "main", Data: []byte("before restart\n")}
	if err := recorder.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close("STOPPED", "server shutdown"); err != nil {
		t.Fatal(err)
	}
	nextFactory, err := NewFactory(config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := nextFactory.RecoverableRuns()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].RunID != run.RunID || runs[0].SinceSeconds != run.SinceSeconds {
		t.Fatalf("unexpected recovered runs: %#v", runs)
	}
	restarted, err := nextFactory.Start(runs[0])
	if err != nil {
		t.Fatalf("restart recorder with existing segments: %v", err)
	}
	record.Data = []byte("after restart\n")
	if err := restarted.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close("STOPPED", "server shutdown"); err != nil {
		t.Fatal(err)
	}
	secondSegment := filepath.Join(root, "logs", "continuous", run.RunID, "ns", "deployment-api", "api-0_uid-1", "main", "restart-0", "log-000002.log")
	if _, err := os.Stat(secondSegment); err != nil {
		t.Fatalf("recovered recorder did not continue segment sequence: %v", err)
	}
	if err := nextFactory.SetRunDesiredState(run.RunID, "STOPPED"); err != nil {
		t.Fatal(err)
	}
	runs, err = nextFactory.RecoverableRuns()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("explicitly stopped run was recoverable: %#v", runs)
	}
}

func TestRecorderPreservesRawLogsAndSeparatesPodInstancesAndRestarts(t *testing.T) {
	root := t.TempDir()
	factory, err := NewFactory(Config{Directory: root, SegmentSize: 1024, MaxRetainedSize: 4096, SyncInterval: time.Hour}, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	run := domain.LogCaptureRun{RunID: "run-1", StartedAt: time.Now(), Targets: []domain.WorkloadLogTarget{{Namespace: "ns", Kind: "statefulset", Name: "app"}}}
	recorder, err := factory.Start(run)
	if err != nil {
		t.Fatal(err)
	}
	target := run.Targets[0]
	records := []domain.LogRecord{
		{Target: target, PodName: "app-0", PodUID: "aaaaaaaa-1111", Container: "main", RestartCount: 0, Data: []byte("plain first line\n")},
		{Target: target, PodName: "app-0", PodUID: "aaaaaaaa-1111", Container: "main", RestartCount: 1, Data: []byte("plain restarted line\n")},
		{Target: target, PodName: "app-0", PodUID: "bbbbbbbb-2222", Container: "main", RestartCount: 0, Data: []byte("plain recreated line\n")},
	}
	for _, record := range records {
		if err := recorder.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := recorder.Close("STOPPED", ""); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		filepath.Join("run-1", "ns", "statefulset-app", "app-0_aaaaaaaa", "main", "restart-0", "log-000001.log"): "plain first line\n",
		filepath.Join("run-1", "ns", "statefulset-app", "app-0_aaaaaaaa", "main", "restart-1", "log-000001.log"): "plain restarted line\n",
		filepath.Join("run-1", "ns", "statefulset-app", "app-0_bbbbbbbb", "main", "restart-0", "log-000001.log"): "plain recreated line\n",
	}
	base := filepath.Join(root, "logs", "continuous")
	for relative, expected := range want {
		data, err := os.ReadFile(filepath.Join(base, relative))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		if string(data) != expected {
			t.Fatalf("%s = %q, want %q", relative, data, expected)
		}
	}
}

func TestRecorderRotatesAtLineBoundaryAndPrunesOldestGlobally(t *testing.T) {
	root := t.TempDir()
	factory, err := NewFactory(Config{Directory: root, SegmentSize: 8, MaxRetainedSize: 12, SyncInterval: time.Hour}, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := domain.WorkloadLogTarget{Namespace: "ns", Kind: "deployment", Name: "api"}
	recorder, err := factory.Start(domain.LogCaptureRun{RunID: "run-1", StartedAt: time.Now(), Targets: []domain.WorkloadLogTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"one\n", "two\n", "tri\n", "for\n"} {
		if err := recorder.Write(domain.LogRecord{Target: target, PodName: "api-1", PodUID: "12345678-full", Container: "app", Data: []byte(line)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := recorder.Close("STOPPED", ""); err != nil {
		t.Fatal(err)
	}
	usage := recorder.Usage()
	if usage.RetainedSize > 12 {
		t.Fatalf("retained size = %d, want <= 12", usage.RetainedSize)
	}
	var contents []string
	err = filepath.WalkDir(filepath.Join(root, "logs", "continuous"), func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".log") {
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			contents = append(contents, string(data))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(contents)
	if len(contents) != 1 || contents[0] != "tri\nfor\n" {
		t.Fatalf("retained logs = %#v", contents)
	}
}

func TestRecorderKeepsQuotaWithSeveralActiveStreams(t *testing.T) {
	root := t.TempDir()
	factory, err := NewFactory(Config{Directory: root, SegmentSize: 8, MaxRetainedSize: 8, SyncInterval: time.Hour}, slog.Default(), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := domain.WorkloadLogTarget{Namespace: "ns", Kind: "statefulset", Name: "app"}
	recorder, err := factory.Start(domain.LogCaptureRun{RunID: "run-1", StartedAt: time.Now(), Targets: []domain.WorkloadLogTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []domain.LogRecord{
		{Target: target, PodName: "app-0", PodUID: "uid00000", Container: "main", Data: []byte("aaa\n")},
		{Target: target, PodName: "app-1", PodUID: "uid11111", Container: "main", Data: []byte("bbb\n")},
		{Target: target, PodName: "app-1", PodUID: "uid11111", Container: "main", Data: []byte("ccc\n")},
	} {
		if err := recorder.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if usage := recorder.Usage(); usage.RetainedSize > 8 {
		t.Fatalf("active retained size = %d, want <= 8", usage.RetainedSize)
	}
	if err := recorder.Close("STOPPED", ""); err != nil {
		t.Fatal(err)
	}
	if usage := recorder.Usage(); usage.RetainedSize > 8 {
		t.Fatalf("final retained size = %d, want <= 8", usage.RetainedSize)
	}
}

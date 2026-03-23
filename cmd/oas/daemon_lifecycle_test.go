package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
)

func TestDaemonPathsForIncludesStatePathIdentity(t *testing.T) {
	base := t.TempDir()
	cfgA := config.Config{
		MachineID: "shared",
		StatePath: filepath.Join(base, "state-a.db"),
	}
	cfgB := config.Config{
		MachineID: "shared",
		StatePath: filepath.Join(base, "state-b.db"),
	}

	pathsA := daemonPathsFor(cfgA)
	pathsB := daemonPathsFor(cfgB)

	if pathsA.pidPath == pathsB.pidPath {
		t.Fatalf("pidPath collision: %q", pathsA.pidPath)
	}
	if pathsA.logPath == pathsB.logPath {
		t.Fatalf("logPath collision: %q", pathsA.logPath)
	}
	if pathsA.metaPath == pathsB.metaPath {
		t.Fatalf("metaPath collision: %q", pathsA.metaPath)
	}
}

func TestDaemonStateKeyIncludesReadableBaseName(t *testing.T) {
	key := daemonStateKey("/tmp/oas/state-a.db")
	if !strings.HasPrefix(key, "state-a-") {
		t.Fatalf("daemonStateKey = %q, want readable state basename prefix", key)
	}
}

func TestParseDaemonStorageActivityEnforcedLine(t *testing.T) {
	line := "[2026-03-23T16:00:00Z] storage guard: reason=usage 101 > max 100 usage_bytes=101 free_bytes=200 pruned_records=7 safe_prune_offset=55"

	activity, ok := parseDaemonStorageActivity(line)
	if !ok {
		t.Fatal("parseDaemonStorageActivity() = false, want true")
	}
	if got, want := activity.Outcome, "enforced"; got != want {
		t.Fatalf("Outcome = %q, want %q", got, want)
	}
	if got, want := activity.Timestamp, "2026-03-23T16:00:00Z"; got != want {
		t.Fatalf("Timestamp = %q, want %q", got, want)
	}
	if got, want := activity.PrunedRecords, int64(7); got != want {
		t.Fatalf("PrunedRecords = %d, want %d", got, want)
	}
	if got, want := activity.SafePruneOffset, int64(55); got != want {
		t.Fatalf("SafePruneOffset = %d, want %d", got, want)
	}
	if got, want := activity.UsageBytes, int64(101); got != want {
		t.Fatalf("UsageBytes = %d, want %d", got, want)
	}
	if !strings.Contains(activity.Reason, "usage 101 > max 100") {
		t.Fatalf("Reason = %q, want usage-based reason", activity.Reason)
	}
}

func TestParseDaemonStorageActivityFailedLine(t *testing.T) {
	line := "[2026-03-23T16:00:01Z] storage guard failed (1/10): storage usage remains above target after pruning"

	activity, ok := parseDaemonStorageActivity(line)
	if !ok {
		t.Fatal("parseDaemonStorageActivity() = false, want true")
	}
	if got, want := activity.Outcome, "failed"; got != want {
		t.Fatalf("Outcome = %q, want %q", got, want)
	}
	if got, want := activity.Timestamp, "2026-03-23T16:00:01Z"; got != want {
		t.Fatalf("Timestamp = %q, want %q", got, want)
	}
	if !strings.Contains(activity.Reason, "storage usage remains above target") {
		t.Fatalf("Reason = %q, want failure detail", activity.Reason)
	}
}

func TestReadLastStorageActivityReturnsMostRecentStorageLine(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "daemon.log")
	content := strings.Join([]string{
		"[2026-03-23T16:00:00Z] daemon running",
		"[2026-03-23T16:00:01Z] storage guard: reason=usage 101 > max 100 usage_bytes=101 free_bytes=200 pruned_records=7 safe_prune_offset=55",
		"[2026-03-23T16:00:02Z] storage guard failed (1/10): storage usage remains above target after pruning",
	}, "\n")
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	activity, err := readLastStorageActivity(logPath)
	if err != nil {
		t.Fatalf("readLastStorageActivity() error = %v", err)
	}
	if activity == nil {
		t.Fatal("readLastStorageActivity() = nil, want activity")
	}
	if got, want := activity.Outcome, "failed"; got != want {
		t.Fatalf("Outcome = %q, want %q", got, want)
	}
}

func TestBuildDaemonStatusViewProducesStructuredPathsAndRuntime(t *testing.T) {
	base := t.TempDir()
	cfg := config.Config{
		MachineID:            "example-machine",
		StatePath:            filepath.Join(base, "state.db"),
		LedgerPath:           filepath.Join(base, "ledger.db"),
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
	}
	paths := daemonPathsFor(cfg)
	storage := daemonStorageStatus{
		Configured:        true,
		UsageBytes:        123,
		ManagedPathCount:  2,
		ManagedPaths:      []string{filepath.Join(base, "state.db"), filepath.Join(base, "ledger.db")},
		MaxStorageBytes:   1000,
		PruneTargetBytes:  800,
		DesiredUsageBytes: 800,
		MinFreeBytes:      50,
		LastActivity: &daemonStorageActivity{
			Timestamp:     "2026-03-23T17:00:00Z",
			Outcome:       "enforced",
			Reason:        "usage 101 > max 100",
			PrunedRecords: 7,
		},
	}

	view := buildDaemonStatusView("/tmp/oas.json", cfg, daemonMetadata{
		ConfigPath:           "/tmp/oas.json",
		StartedAt:            "2026-03-23T16:59:00Z",
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
	}, paths, 42, true, storage, nil)

	if got, want := view.State, "running"; got != want {
		t.Fatalf("State = %q, want %q", got, want)
	}
	if !view.Running {
		t.Fatal("Running = false, want true")
	}
	if got, want := view.Runtime.ConfigPath, "/tmp/oas.json"; got != want {
		t.Fatalf("Runtime.ConfigPath = %q, want %q", got, want)
	}
	for label, value := range map[string]string{
		"state_path":  view.Paths.StatePath,
		"ledger_path": view.Paths.LedgerPath,
		"pid_path":    view.Paths.PIDPath,
		"log_path":    view.Paths.LogPath,
		"meta_path":   view.Paths.MetaPath,
	} {
		if !filepath.IsAbs(value) {
			t.Fatalf("%s = %q, want absolute path", label, value)
		}
	}
	if view.Storage == nil {
		t.Fatal("Storage = nil, want populated storage status")
	}
	if got, want := view.Storage.ManagedPathCount, 2; got != want {
		t.Fatalf("ManagedPathCount = %d, want %d", got, want)
	}
}

func TestWriteDaemonStatusReportIncludesOperationalSections(t *testing.T) {
	view := daemonStatusView{
		State:     "running",
		Running:   true,
		PID:       42,
		StartedAt: "2026-03-23T16:59:00Z",
		Runtime: daemonRuntimeStatus{
			ConfigPath:           "/tmp/oas.json",
			PollInterval:         "3s",
			ErrorBackoff:         "10s",
			MaxConsecutiveErrors: 10,
		},
		Paths: daemonResolvedPaths{
			StatePath:  "/tmp/state.db",
			LedgerPath: "/tmp/ledger.db",
			PIDPath:    "/tmp/oas.pid",
			LogPath:    "/tmp/oas.log",
			MetaPath:   "/tmp/oas.meta.json",
		},
		Storage: &daemonStorageStatus{
			Configured:        true,
			UsageBytes:        123,
			ManagedPathCount:  2,
			MaxStorageBytes:   1000,
			PruneTargetBytes:  800,
			DesiredUsageBytes: 800,
			MinFreeBytes:      50,
			LastActivity: &daemonStorageActivity{
				Outcome:       "enforced",
				Timestamp:     "2026-03-23T17:00:00Z",
				Reason:        "usage 101 > max 100",
				PrunedRecords: 7,
			},
		},
	}

	var out bytes.Buffer
	if err := writeDaemonStatusReport(&out, view); err != nil {
		t.Fatalf("writeDaemonStatusReport() error = %v", err)
	}

	text := out.String()
	for _, want := range []string{
		"Daemon:",
		"Runtime:",
		"Paths:",
		"Storage:",
		"Storage activity:",
		"State",
		"Ledger DB",
		"Managed path count",
		"Pruned records",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
	}
}

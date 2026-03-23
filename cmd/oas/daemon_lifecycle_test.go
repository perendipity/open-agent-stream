package main

import (
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

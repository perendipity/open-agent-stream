package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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

	for label, values := range map[string][2]string{
		"pid":        {pathsA.pidPath, pathsB.pidPath},
		"log":        {pathsA.logPath, pathsB.logPath},
		"status":     {pathsA.statusPath, pathsB.statusPath},
		"lock":       {pathsA.lockPath, pathsB.lockPath},
		"controlDir": {pathsA.controlDirPath, pathsB.controlDirPath},
	} {
		if values[0] == values[1] {
			t.Fatalf("%s path collision: %q", label, values[0])
		}
	}
}

func TestDaemonStateKeyIncludesReadableBaseName(t *testing.T) {
	key := daemonStateKey("/tmp/oas/state-a.db")
	if !strings.HasPrefix(key, "state-a-") {
		t.Fatalf("daemonStateKey = %q, want readable state basename prefix", key)
	}
}

func TestProcessSignalIndicatesExistsTreatsPermissionDeniedAsAlive(t *testing.T) {
	if !processSignalIndicatesExists(syscall.EPERM) {
		t.Fatal("processSignalIndicatesExists(EPERM) = false, want true")
	}
}

func TestDeriveDaemonStatusSnapshotFreshRunningLocked(t *testing.T) {
	now := time.Now().UTC()
	snapshot := daemonStatusSnapshot{
		LockHeld: true,
		Record: daemonStatusRecord{
			State:       daemonStateRunning,
			HeartbeatAt: now.Format(time.RFC3339Nano),
			ReadyAt:     now.Format(time.RFC3339Nano),
		},
		HasRecord: true,
	}

	deriveDaemonStatusSnapshot(&snapshot, now)

	if !snapshot.Live {
		t.Fatal("Live = false, want true")
	}
	if !snapshot.Ready {
		t.Fatal("Ready = false, want true")
	}
	if snapshot.Stale {
		t.Fatal("Stale = true, want false")
	}
	if !snapshot.HeartbeatFresh {
		t.Fatal("HeartbeatFresh = false, want true")
	}
}

func TestDeriveDaemonStatusSnapshotStaleWithFreeLock(t *testing.T) {
	now := time.Now().UTC()
	snapshot := daemonStatusSnapshot{
		LockHeld: false,
		Record: daemonStatusRecord{
			State:       daemonStateRunning,
			HeartbeatAt: now.Add(-daemonStatusStaleThreshold - time.Second).Format(time.RFC3339Nano),
		},
		HasRecord: true,
	}

	deriveDaemonStatusSnapshot(&snapshot, now)

	if snapshot.Live {
		t.Fatal("Live = true, want false")
	}
	if !snapshot.Stale {
		t.Fatal("Stale = false, want true")
	}
	if !strings.Contains(snapshot.StatusReason, "lock is free") {
		t.Fatalf("StatusReason = %q, want free-lock stale reason", snapshot.StatusReason)
	}
}

func TestDeriveDaemonStatusSnapshotStaleWithHeldLock(t *testing.T) {
	now := time.Now().UTC()
	snapshot := daemonStatusSnapshot{
		LockHeld: true,
		Record: daemonStatusRecord{
			State:       daemonStateRunning,
			HeartbeatAt: now.Add(-daemonStatusStaleThreshold - time.Second).Format(time.RFC3339Nano),
		},
		HasRecord: true,
	}

	deriveDaemonStatusSnapshot(&snapshot, now)

	if !snapshot.Live {
		t.Fatal("Live = false, want true")
	}
	if !snapshot.Stale {
		t.Fatal("Stale = false, want true")
	}
	if !strings.Contains(snapshot.StatusReason, "still held") {
		t.Fatalf("StatusReason = %q, want held-lock stale reason", snapshot.StatusReason)
	}
}

func TestDeriveDaemonStatusSnapshotUnreadableStatusButLockHeld(t *testing.T) {
	snapshot := daemonStatusSnapshot{
		LockHeld:  true,
		RecordErr: errors.New("bad json"),
	}

	deriveDaemonStatusSnapshot(&snapshot, time.Now().UTC())

	if !snapshot.Live {
		t.Fatal("Live = false, want true")
	}
	if !strings.Contains(snapshot.StatusReason, "unreadable") {
		t.Fatalf("StatusReason = %q, want unreadable-status reason", snapshot.StatusReason)
	}
}

func TestInspectDaemonSnapshotUsesLockWhenProcessProbesDisabled(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	lock, err := acquireDaemonLock(paths.lockPath)
	if err != nil {
		t.Fatalf("acquireDaemonLock() error = %v", err)
	}
	defer lock.Release()

	now := time.Now().UTC()
	record := daemonStatusRecord{
		SchemaVersion:    daemonStatusSchemaVersion,
		InstanceID:       "inst-test",
		PID:              123,
		State:            daemonStateRunning,
		StartedAt:        now.Format(time.RFC3339Nano),
		ReadyAt:          now.Format(time.RFC3339Nano),
		HeartbeatAt:      now.Format(time.RFC3339Nano),
		LastTransitionAt: now.Format(time.RFC3339Nano),
	}
	if err := writeAtomicJSON(paths.statusPath, record); err != nil {
		t.Fatalf("writeAtomicJSON() error = %v", err)
	}

	daemonDisableProcessTest = true
	defer func() { daemonDisableProcessTest = false }()

	snapshot, err := inspectDaemonSnapshot(paths)
	if err != nil {
		t.Fatalf("inspectDaemonSnapshot() error = %v", err)
	}
	if !snapshot.Live {
		t.Fatal("Live = false, want true")
	}
	if !snapshot.LockHeld {
		t.Fatal("LockHeld = false, want true")
	}
}

func TestDaemonLockHeldReportsTrueWhenLockIsHeld(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	lock, err := acquireDaemonLock(paths.lockPath)
	if err != nil {
		t.Fatalf("acquireDaemonLock() error = %v", err)
	}
	defer lock.Release()

	held, err := daemonLockHeld(paths.lockPath)
	if err != nil {
		t.Fatalf("daemonLockHeld() error = %v", err)
	}
	if !held {
		t.Fatal("held = false, want true")
	}
}

func TestProcessDaemonControlRequestsMarksStoppingOnce(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	manager := testDaemonStatusManager(t, paths, daemonStatusRecord{
		SchemaVersion: daemonStatusSchemaVersion,
		InstanceID:    "inst-a",
		State:         daemonStateRunning,
		HeartbeatAt:   time.Now().UTC().Format(time.RFC3339Nano),
	})
	request := daemonControlRequest{
		SchemaVersion:    daemonControlSchemaVersion,
		RequestID:        "req-1",
		TargetInstanceID: "inst-a",
		Action:           "stop",
		RequestedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeDaemonControlRequest(paths.controlDirPath, request); err != nil {
		t.Fatalf("writeDaemonControlRequest() error = %v", err)
	}

	stopRequested, err := processDaemonControlRequests(paths, manager, "inst-a")
	if err != nil {
		t.Fatalf("processDaemonControlRequests() error = %v", err)
	}
	if !stopRequested {
		t.Fatal("stopRequested = false, want true")
	}

	record := manager.snapshot()
	if got, want := record.State, daemonStateStopping; got != want {
		t.Fatalf("State = %q, want %q", got, want)
	}
	if got, want := record.LastProcessedRequestID, "req-1"; got != want {
		t.Fatalf("LastProcessedRequestID = %q, want %q", got, want)
	}

	stopRequested, err = processDaemonControlRequests(paths, manager, "inst-a")
	if err != nil {
		t.Fatalf("second processDaemonControlRequests() error = %v", err)
	}
	if stopRequested {
		t.Fatal("second stopRequested = true, want false")
	}
}

func TestProcessDaemonControlRequestsIgnoresOldInstanceAndMalformedFiles(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	manager := testDaemonStatusManager(t, paths, daemonStatusRecord{
		SchemaVersion: daemonStatusSchemaVersion,
		InstanceID:    "inst-current",
		State:         daemonStateRunning,
		HeartbeatAt:   time.Now().UTC().Format(time.RFC3339Nano),
	})

	if err := os.MkdirAll(paths.controlDirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.controlDirPath, "bad.json"), []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("WriteFile(bad.json) error = %v", err)
	}
	oldRequest := daemonControlRequest{
		SchemaVersion:    daemonControlSchemaVersion,
		RequestID:        "req-old",
		TargetInstanceID: "inst-old",
		Action:           "stop",
		RequestedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeDaemonControlRequest(paths.controlDirPath, oldRequest); err != nil {
		t.Fatalf("writeDaemonControlRequest() error = %v", err)
	}

	stopRequested, err := processDaemonControlRequests(paths, manager, "inst-current")
	if err != nil {
		t.Fatalf("processDaemonControlRequests() error = %v", err)
	}
	if stopRequested {
		t.Fatal("stopRequested = true, want false")
	}

	record := manager.snapshot()
	if got := record.LastProcessedRequestID; got != "" {
		t.Fatalf("LastProcessedRequestID = %q, want empty", got)
	}
	entries, err := os.ReadDir(paths.controlDirPath)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("control dir entries = %d, want 0", len(entries))
	}
}

func TestCleanupDaemonTransientFilesRemovesPIDAndControlFilesKeepsStatus(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	if err := os.MkdirAll(paths.controlDirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(paths.pidPath, []byte("42"), 0o644); err != nil {
		t.Fatalf("WriteFile(pid) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.controlDirPath, "req.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("WriteFile(req.json) error = %v", err)
	}
	if err := os.WriteFile(paths.statusPath, []byte("{}"), 0o644); err != nil {
		t.Fatalf("WriteFile(status) error = %v", err)
	}

	if err := cleanupDaemonTransientFiles(paths); err != nil {
		t.Fatalf("cleanupDaemonTransientFiles() error = %v", err)
	}
	if _, err := os.Stat(paths.statusPath); err != nil {
		t.Fatalf("status path missing after cleanup: %v", err)
	}
	if _, err := os.Stat(paths.pidPath); !os.IsNotExist(err) {
		t.Fatalf("pid path err = %v, want IsNotExist", err)
	}
	entries, err := os.ReadDir(paths.controlDirPath)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("control dir entries = %d, want 0", len(entries))
	}
}

func TestWaitForDaemonReadyTimesOut(t *testing.T) {
	base := t.TempDir()
	paths := testDaemonPaths(base)
	exitCh := make(chan error)
	defer close(exitCh)

	_, err := waitForDaemonReady(paths, "inst-timeout", 50*time.Millisecond, exitCh)
	if err == nil {
		t.Fatal("waitForDaemonReady() error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("waitForDaemonReady() error = %v, want timeout", err)
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
	now := time.Now().UTC()
	snapshot := daemonStatusSnapshot{
		Record: daemonStatusRecord{
			SchemaVersion:          daemonStatusSchemaVersion,
			InstanceID:             "inst-123",
			PID:                    42,
			ConfigPath:             "/tmp/oas.json",
			StartedAt:              now.Format(time.RFC3339Nano),
			ReadyAt:                now.Format(time.RFC3339Nano),
			HeartbeatAt:            now.Format(time.RFC3339Nano),
			LastTransitionAt:       now.Format(time.RFC3339Nano),
			State:                  daemonStateRunning,
			LastProcessedRequestID: "req-1",
			LastControlAction:      "stop",
			PollInterval:           "3s",
			ErrorBackoff:           "10s",
			MaxConsecutiveErrors:   10,
		},
		HasRecord:      true,
		LockHeld:       true,
		Live:           true,
		Ready:          true,
		HeartbeatFresh: true,
		PID:            42,
	}
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

	view := buildDaemonStatusView("/tmp/oas.json", cfg, paths, snapshot, storage, nil)

	if got, want := view.State, daemonStateRunning; got != want {
		t.Fatalf("State = %q, want %q", got, want)
	}
	if !view.Live || !view.Ready || !view.Running {
		t.Fatalf("live/ready/running = %v/%v/%v, want true/true/true", view.Live, view.Ready, view.Running)
	}
	if got, want := view.Runtime.ConfigPath, "/tmp/oas.json"; got != want {
		t.Fatalf("Runtime.ConfigPath = %q, want %q", got, want)
	}
	for label, value := range map[string]string{
		"state_path":   view.Paths.StatePath,
		"ledger_path":  view.Paths.LedgerPath,
		"pid_path":     view.Paths.PIDPath,
		"log_path":     view.Paths.LogPath,
		"status_path":  view.Paths.StatusPath,
		"meta_path":    view.Paths.MetaPath,
		"lock_path":    view.Paths.LockPath,
		"control_path": view.Paths.ControlDirPath,
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
		State:          daemonStateRunning,
		Running:        true,
		Live:           true,
		Ready:          true,
		HeartbeatFresh: true,
		PID:            42,
		StartedAt:      "2026-03-23T16:59:00Z",
		Runtime: daemonRuntimeStatus{
			ConfigPath:           "/tmp/oas.json",
			PollInterval:         "3s",
			ErrorBackoff:         "10s",
			MaxConsecutiveErrors: 10,
		},
		Paths: daemonResolvedPaths{
			StatePath:      "/tmp/state.db",
			LedgerPath:     "/tmp/ledger.db",
			PIDPath:        "/tmp/oas.pid",
			LogPath:        "/tmp/oas.log",
			StatusPath:     "/tmp/oas.status.json",
			MetaPath:       "/tmp/oas.status.json",
			LockPath:       "/tmp/oas.lock",
			ControlDirPath: "/tmp/oas.control.d",
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
		"Control:",
		"Paths:",
		"Storage:",
		"Storage activity:",
		"Heartbeat fresh",
		"Status file",
		"Control dir",
		"Pruned records",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
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

func testDaemonPaths(base string) daemonPaths {
	statusPath := filepath.Join(base, "daemon.json")
	return daemonPaths{
		pidPath:        filepath.Join(base, "daemon.pid"),
		logPath:        filepath.Join(base, "daemon.log"),
		statusPath:     statusPath,
		metaPath:       statusPath,
		lockPath:       filepath.Join(base, "daemon.lock"),
		controlDirPath: filepath.Join(base, "daemon.control.d"),
	}
}

func testDaemonStatusManager(t *testing.T, paths daemonPaths, record daemonStatusRecord) *daemonStatusManager {
	t.Helper()
	manager := &daemonStatusManager{paths: paths, record: record}
	if err := manager.persist(); err != nil {
		t.Fatalf("manager.persist() error = %v", err)
	}
	return manager
}

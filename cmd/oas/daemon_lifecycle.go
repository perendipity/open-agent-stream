package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/storageguard"
	"github.com/open-agent-stream/open-agent-stream/internal/supervisor"
)

const (
	daemonStatusSchemaVersion   = 1
	daemonControlSchemaVersion  = 1
	daemonHeartbeatInterval     = 5 * time.Second
	daemonControlPollInterval   = 1 * time.Second
	daemonStatusStaleThreshold  = daemonHeartbeatInterval * 4
	daemonLifecyclePollInterval = 100 * time.Millisecond
	daemonStartReadyTimeout     = 10 * time.Second
)

const (
	daemonStateStarting = "starting"
	daemonStateRunning  = "running"
	daemonStateStopping = "stopping"
	daemonStateStopped  = "stopped"
	daemonStateFailed   = "failed"
)

const daemonInstanceIDEnv = "OAS_DAEMON_INSTANCE_ID"

var (
	errDaemonLockHeld        = errors.New("daemon lock already held")
	daemonDisableProcessTest bool
)

type daemonPaths struct {
	pidPath        string
	logPath        string
	statusPath     string
	metaPath       string
	lockPath       string
	controlDirPath string
}

type daemonStatusRecord struct {
	SchemaVersion          int    `json:"schema_version,omitempty"`
	InstanceID             string `json:"instance_id,omitempty"`
	PID                    int    `json:"pid,omitempty"`
	ConfigPath             string `json:"config_path,omitempty"`
	LogPath                string `json:"log_path,omitempty"`
	StartedAt              string `json:"started_at,omitempty"`
	ReadyAt                string `json:"ready_at,omitempty"`
	HeartbeatAt            string `json:"heartbeat_at,omitempty"`
	LastTransitionAt       string `json:"last_transition_at,omitempty"`
	State                  string `json:"state,omitempty"`
	LastError              string `json:"last_error,omitempty"`
	LastProcessedRequestID string `json:"last_processed_request_id,omitempty"`
	LastControlAction      string `json:"last_control_action,omitempty"`
	PollInterval           string `json:"poll_interval,omitempty"`
	ErrorBackoff           string `json:"error_backoff,omitempty"`
	MaxConsecutiveErrors   int    `json:"max_consecutive_errors,omitempty"`
}

type daemonControlRequest struct {
	SchemaVersion    int    `json:"schema_version,omitempty"`
	RequestID        string `json:"request_id,omitempty"`
	TargetInstanceID string `json:"target_instance_id,omitempty"`
	Action           string `json:"action,omitempty"`
	RequestedAt      string `json:"requested_at,omitempty"`
}

type daemonStorageStatus struct {
	Configured        bool                   `json:"configured"`
	UsageBytes        int64                  `json:"usage_bytes"`
	FreeBytes         uint64                 `json:"free_bytes,omitempty"`
	ManagedPathCount  int                    `json:"managed_path_count,omitempty"`
	ManagedPaths      []string               `json:"managed_paths,omitempty"`
	MaxStorageBytes   int64                  `json:"max_storage_bytes,omitempty"`
	PruneTargetBytes  int64                  `json:"prune_target_bytes,omitempty"`
	DesiredUsageBytes int64                  `json:"desired_usage_bytes,omitempty"`
	MinFreeBytes      int64                  `json:"min_free_bytes,omitempty"`
	NeedsEnforcement  bool                   `json:"needs_enforcement,omitempty"`
	Reason            string                 `json:"reason,omitempty"`
	LastActivity      *daemonStorageActivity `json:"last_activity,omitempty"`
}

type daemonStorageActivity struct {
	Timestamp       string `json:"timestamp,omitempty"`
	Outcome         string `json:"outcome,omitempty"`
	Reason          string `json:"reason,omitempty"`
	UsageBytes      int64  `json:"usage_bytes,omitempty"`
	FreeBytes       uint64 `json:"free_bytes,omitempty"`
	PrunedRecords   int64  `json:"pruned_records,omitempty"`
	SafePruneOffset int64  `json:"safe_prune_offset,omitempty"`
	Raw             string `json:"raw,omitempty"`
}

type daemonStatusView struct {
	SchemaVersion          int                  `json:"schema_version,omitempty"`
	InstanceID             string               `json:"instance_id,omitempty"`
	State                  string               `json:"state"`
	Running                bool                 `json:"running"`
	Live                   bool                 `json:"live"`
	Ready                  bool                 `json:"ready"`
	Stale                  bool                 `json:"stale,omitempty"`
	HeartbeatFresh         bool                 `json:"heartbeat_fresh,omitempty"`
	StatusReason           string               `json:"status_reason,omitempty"`
	StalePID               bool                 `json:"stale_pid,omitempty"`
	PID                    int                  `json:"pid,omitempty"`
	StartedAt              string               `json:"started_at,omitempty"`
	ReadyAt                string               `json:"ready_at,omitempty"`
	HeartbeatAt            string               `json:"heartbeat_at,omitempty"`
	LastTransitionAt       string               `json:"last_transition_at,omitempty"`
	LastError              string               `json:"last_error,omitempty"`
	LastProcessedRequestID string               `json:"last_processed_request_id,omitempty"`
	LastControlAction      string               `json:"last_control_action,omitempty"`
	Runtime                daemonRuntimeStatus  `json:"runtime"`
	Paths                  daemonResolvedPaths  `json:"paths"`
	Storage                *daemonStorageStatus `json:"storage,omitempty"`
	StorageError           string               `json:"storage_error,omitempty"`
}

type daemonRuntimeStatus struct {
	ConfigPath           string `json:"config_path,omitempty"`
	PollInterval         string `json:"poll_interval,omitempty"`
	ErrorBackoff         string `json:"error_backoff,omitempty"`
	MaxConsecutiveErrors int    `json:"max_consecutive_errors,omitempty"`
}

type daemonResolvedPaths struct {
	StatePath      string `json:"state_path"`
	LedgerPath     string `json:"ledger_path"`
	PIDPath        string `json:"pid_path"`
	LogPath        string `json:"log_path"`
	StatusPath     string `json:"status_path"`
	MetaPath       string `json:"meta_path"`
	LockPath       string `json:"lock_path"`
	ControlDirPath string `json:"control_dir_path"`
}

type daemonStatusSnapshot struct {
	Record         daemonStatusRecord
	HasRecord      bool
	RecordErr      error
	LockHeld       bool
	HeartbeatFresh bool
	Live           bool
	Ready          bool
	Stale          bool
	StatusReason   string
	PID            int
}

type daemonStopResult struct {
	PID        int
	InstanceID string
	WasLive    bool
}

type daemonLock struct {
	file *os.File
}

type daemonStatusManager struct {
	mu     sync.Mutex
	paths  daemonPaths
	record daemonStatusRecord
}

type daemonConfigHelp struct {
	Description string
	Notes       []string
	Examples    []string
}

func daemonStartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon start", daemonConfigHelp{
		Description: "Start a detached daemon process and return after it reports ready.",
		Notes: []string{
			"Safe to rerun: if the daemon lock is already held for this config, OAS will not start a second copy.",
			"Detached mode writes stdout/stderr to the resolved daemon log path.",
			"Daemon lifecycle state must live on a local filesystem; network-mounted state directories are not supported.",
		},
		Examples: []string{
			"oas daemon start -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	snapshot, err := inspectDaemonSnapshot(paths)
	if err != nil {
		fatal(err)
	}
	if snapshot.LockHeld {
		if snapshot.Stale || snapshot.RecordErr != nil || !snapshot.HasRecord {
			fatal(fmt.Errorf("daemon lock is held for this config but the active owner is unhealthy; refusing to start a duplicate"))
		}
		if snapshot.PID > 0 {
			fmt.Printf("Daemon already running (pid=%d)\n", snapshot.PID)
		} else {
			fmt.Println("Daemon already running.")
		}
		return
	}
	exePath, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	instanceID := newDaemonID("inst")
	pid, exitCh, err := startDetachedDaemon(exePath, configPath, paths.logPath, instanceID)
	if err != nil {
		fatal(err)
	}
	record, err := waitForDaemonReady(paths, instanceID, daemonStartReadyTimeout, exitCh)
	if err != nil {
		fatal(err)
	}
	if record.PID > 0 {
		pid = record.PID
	}
	fmt.Printf("Started daemon (pid=%d, log=%s)\n", pid, paths.logPath)
}

func daemonStopCommand(args []string) {
	cfg, _ := mustDaemonConfig(args, "daemon stop", daemonConfigHelp{
		Description: "Stop the detached daemon for this config if it is running.",
		Notes: []string{
			"Safe to run when the daemon is already stopped.",
			"OAS stops the daemon through its on-disk control contract and only uses OS signals as a best-effort fallback.",
			"Daemon lifecycle state must live on a local filesystem; network-mounted state directories are not supported.",
		},
		Examples: []string{
			"oas daemon stop -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	result, err := stopDaemon(paths, 15*time.Second)
	if err != nil {
		fatal(err)
	}
	if !result.WasLive {
		fmt.Println("Daemon not running.")
		return
	}
	if result.PID > 0 {
		fmt.Printf("Stopped daemon (pid=%d)\n", result.PID)
		return
	}
	fmt.Println("Stopped daemon.")
}

func daemonStatusCommand(args []string) {
	fs := flag.NewFlagSet("daemon status", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	asJSON := fs.Bool("json", false, "print status as JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas daemon status -config <path> [flags]

Show daemon/runtime status, resolved paths, and storage activity.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas daemon status -config ./examples/config.example.json",
			"oas daemon status -config ./examples/config.example.json -json",
		)
	}
	_ = fs.Parse(args)
	if strings.TrimSpace(*configPath) == "" {
		fatal(errors.New("config path is required"))
	}
	absConfigPath, err := filepath.Abs(*configPath)
	if err != nil {
		fatal(err)
	}
	cfg, err := config.Load(absConfigPath)
	if err != nil {
		fatal(err)
	}
	paths := daemonPathsFor(cfg)
	snapshot, err := inspectDaemonSnapshot(paths)
	if err != nil {
		fatal(err)
	}
	storageStatus, storageErr := daemonStorageStatusFor(cfg, paths.logPath)
	view := buildDaemonStatusView(absConfigPath, cfg, paths, snapshot, storageStatus, storageErr)
	if *asJSON {
		if err := writeJSON(os.Stdout, view); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeDaemonStatusReport(os.Stdout, view); err != nil {
		fatal(err)
	}
}

func daemonRestartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon restart", daemonConfigHelp{
		Description: "Stop the detached daemon for this config, then wait for a new detached daemon to report ready.",
		Notes: []string{
			"Restart is strict stop-then-start. If the old daemon does not release its lock, restart fails instead of spawning a duplicate.",
			"Daemon lifecycle state must live on a local filesystem; network-mounted state directories are not supported.",
		},
		Examples: []string{
			"oas daemon restart -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	if result, err := stopDaemon(paths, 15*time.Second); err != nil {
		fatal(err)
	} else if result.WasLive && result.PID > 0 {
		fmt.Printf("Stopped daemon (pid=%d)\n", result.PID)
	}
	snapshot, err := inspectDaemonSnapshot(paths)
	if err != nil {
		fatal(err)
	}
	if snapshot.LockHeld {
		fatal(errors.New("daemon lock is still held after stop; refusing to start a duplicate"))
	}
	exePath, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	instanceID := newDaemonID("inst")
	pid, exitCh, err := startDetachedDaemon(exePath, configPath, paths.logPath, instanceID)
	if err != nil {
		fatal(err)
	}
	record, err := waitForDaemonReady(paths, instanceID, daemonStartReadyTimeout, exitCh)
	if err != nil {
		fatal(err)
	}
	if record.PID > 0 {
		pid = record.PID
	}
	fmt.Printf("Started daemon (pid=%d, log=%s)\n", pid, paths.logPath)
}

func mustDaemonConfig(args []string, name string, help daemonConfigHelp) (config.Config, string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: oas %s -config <path>\n\n%s\n\n", name, help.Description)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		if len(help.Notes) > 0 {
			fmt.Fprintln(os.Stderr, "Notes:")
			for _, note := range help.Notes {
				fmt.Fprintf(os.Stderr, "  %s\n", note)
			}
			fmt.Fprintln(os.Stderr)
		}
		printExamples(os.Stderr, help.Examples...)
	}
	_ = fs.Parse(args)
	if strings.TrimSpace(*configPath) == "" {
		fatal(errors.New("config path is required"))
	}
	absConfig, err := filepath.Abs(*configPath)
	if err != nil {
		fatal(err)
	}
	cfg, err := config.Load(absConfig)
	if err != nil {
		fatal(err)
	}
	return cfg, absConfig
}

func daemonPathsFor(cfg config.Config) daemonPaths {
	baseDir := filepath.Dir(cfg.StatePath)
	instance := sanitizeDaemonComponent(cfg.MachineID)
	if instance == "" {
		instance = "default"
	}
	stateID := daemonStateKey(cfg.StatePath)
	prefix := "oas-daemon-" + instance + "-" + stateID
	statusPath := filepath.Join(baseDir, prefix+".json")
	return daemonPaths{
		pidPath:        filepath.Join(baseDir, prefix+".pid"),
		logPath:        filepath.Join(baseDir, prefix+".log"),
		statusPath:     statusPath,
		metaPath:       statusPath,
		lockPath:       filepath.Join(baseDir, prefix+".lock"),
		controlDirPath: filepath.Join(baseDir, prefix+".control.d"),
	}
}

func daemonStateKey(statePath string) string {
	base := sanitizeDaemonComponent(strings.TrimSuffix(filepath.Base(statePath), filepath.Ext(statePath)))
	if base == "" {
		base = "state"
	}
	sum := sha256.Sum256([]byte(statePath))
	return fmt.Sprintf("%s-%x", base, sum[:4])
}

func sanitizeDaemonComponent(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func runDaemonForeground(ctx context.Context, cfg config.Config, configPath string) error {
	paths := daemonPathsFor(cfg)
	if err := os.MkdirAll(filepath.Dir(paths.pidPath), 0o755); err != nil {
		return err
	}
	lock, err := acquireDaemonLock(paths.lockPath)
	if err != nil {
		if errors.Is(err, errDaemonLockHeld) {
			return errors.New("daemon already running for this config")
		}
		return err
	}
	defer lock.Release()

	if err := cleanupDaemonTransientFiles(paths); err != nil {
		return err
	}

	instanceID := strings.TrimSpace(os.Getenv(daemonInstanceIDEnv))
	if instanceID == "" {
		instanceID = newDaemonID("inst")
	}
	pid := os.Getpid()
	startedAt := time.Now().UTC()
	record := daemonStatusRecord{
		SchemaVersion:        daemonStatusSchemaVersion,
		InstanceID:           instanceID,
		PID:                  pid,
		ConfigPath:           configPath,
		LogPath:              paths.logPath,
		StartedAt:            startedAt.Format(time.RFC3339Nano),
		HeartbeatAt:          startedAt.Format(time.RFC3339Nano),
		LastTransitionAt:     startedAt.Format(time.RFC3339Nano),
		State:                daemonStateStarting,
		PollInterval:         cfg.PollInterval,
		ErrorBackoff:         cfg.ErrorBackoff,
		MaxConsecutiveErrors: cfg.MaxConsecutiveErrors,
	}
	manager := &daemonStatusManager{paths: paths, record: record}
	if err := writePIDFile(paths.pidPath, pid); err != nil {
		return err
	}
	if err := manager.persist(); err != nil {
		_ = os.Remove(paths.pidPath)
		return err
	}

	runtime, err := supervisor.New(cfg)
	if err != nil {
		_ = manager.markFailed(err)
		_ = os.Remove(paths.pidPath)
		return err
	}
	defer runtime.Close(ctx)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	monitorDone := make(chan struct{})
	go daemonLifecycleLoop(runCtx, manager, paths, instanceID, cancel, monitorDone)

	if err := manager.markRunning(); err != nil {
		cancel()
		<-monitorDone
		_ = manager.markFailed(err)
		_ = os.Remove(paths.pidPath)
		return err
	}

	fmt.Fprintf(os.Stderr, "[%s] daemon running (pid=%d, config=%s)\n", time.Now().UTC().Format(time.RFC3339), pid, configPath)

	runErr := runtime.RunContinuously(runCtx)
	cancel()
	<-monitorDone
	_ = os.Remove(paths.pidPath)

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		_ = manager.markFailed(runErr)
		return runErr
	}
	if err := manager.markStopped(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[%s] daemon stopped\n", time.Now().UTC().Format(time.RFC3339))
	return nil
}

func daemonLifecycleLoop(ctx context.Context, manager *daemonStatusManager, paths daemonPaths, instanceID string, cancel context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	heartbeatTicker := time.NewTicker(daemonHeartbeatInterval)
	controlTicker := time.NewTicker(daemonControlPollInterval)
	defer heartbeatTicker.Stop()
	defer controlTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			if err := manager.heartbeat(); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] daemon heartbeat update failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
			}
		case <-controlTicker.C:
			stopRequested, err := processDaemonControlRequests(paths, manager, instanceID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] daemon control processing failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
				continue
			}
			if stopRequested {
				cancel()
			}
		}
	}
}

func startDetachedDaemon(exePath, configPath, logPath, instanceID string) (int, <-chan error, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return 0, nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, nil, err
	}
	defer logFile.Close()

	cmd := exec.Command(exePath, "daemon", "run", "-config", configPath)
	cmd.Env = append(os.Environ(), daemonInstanceIDEnv+"="+instanceID)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, nil, err
	}
	exitCh := make(chan error, 1)
	go func() {
		exitCh <- cmd.Wait()
		close(exitCh)
	}()
	return cmd.Process.Pid, exitCh, nil
}

func waitForDaemonReady(paths daemonPaths, instanceID string, timeout time.Duration, exitCh <-chan error) (daemonStatusRecord, error) {
	deadline := time.Now().Add(timeout)
	var lastMatch daemonStatusRecord
	for {
		snapshot, err := inspectDaemonSnapshot(paths)
		if err == nil && snapshot.HasRecord && snapshot.RecordErr == nil && snapshot.Record.InstanceID == instanceID {
			lastMatch = snapshot.Record
			switch snapshot.Record.State {
			case daemonStateRunning:
				if snapshot.Live && snapshot.Ready {
					return snapshot.Record, nil
				}
			case daemonStateFailed:
				return snapshot.Record, errors.New(firstNonEmpty(snapshot.Record.LastError, "detached daemon failed before readiness"))
			case daemonStateStopped:
				return snapshot.Record, errors.New("detached daemon stopped before readiness")
			}
		}

		select {
		case err, ok := <-exitCh:
			if ok {
				if err != nil {
					return daemonStatusRecord{}, fmt.Errorf("detached daemon exited before readiness: %w", err)
				}
				return daemonStatusRecord{}, errors.New("detached daemon exited before readiness")
			}
		default:
		}

		if time.Now().After(deadline) {
			if lastMatch.InstanceID != "" {
				return daemonStatusRecord{}, fmt.Errorf("timed out waiting for daemon %s to report ready (last state=%s)", instanceID, lastMatch.State)
			}
			return daemonStatusRecord{}, fmt.Errorf("timed out waiting for daemon %s to report ready", instanceID)
		}
		time.Sleep(daemonLifecyclePollInterval)
	}
}

func stopDaemon(paths daemonPaths, timeout time.Duration) (daemonStopResult, error) {
	snapshot, err := inspectDaemonSnapshot(paths)
	if err != nil {
		return daemonStopResult{}, err
	}
	result := daemonStopResult{
		PID:        snapshot.PID,
		InstanceID: snapshot.Record.InstanceID,
		WasLive:    snapshot.Live || snapshot.LockHeld,
	}
	if !result.WasLive {
		if err := cleanupDaemonTransientFiles(paths); err != nil {
			return daemonStopResult{}, err
		}
		return result, nil
	}
	if snapshot.RecordErr != nil || !snapshot.HasRecord || strings.TrimSpace(snapshot.Record.InstanceID) == "" {
		if result.PID > 0 && bestEffortStopProcess(result.PID, 2*time.Second) {
			_ = cleanupDaemonTransientFiles(paths)
			return result, nil
		}
		return daemonStopResult{}, errors.New("daemon appears live but no readable status record with instance_id is available; cannot issue a safe targeted stop request")
	}

	request := daemonControlRequest{
		SchemaVersion:    daemonControlSchemaVersion,
		RequestID:        newDaemonID("req"),
		TargetInstanceID: snapshot.Record.InstanceID,
		Action:           "stop",
		RequestedAt:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeDaemonControlRequest(paths.controlDirPath, request); err != nil {
		return daemonStopResult{}, err
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current, err := inspectDaemonSnapshot(paths)
		if err == nil && !current.LockHeld {
			if err := cleanupDaemonTransientFiles(paths); err != nil {
				return daemonStopResult{}, err
			}
			return result, nil
		}
		time.Sleep(daemonLifecyclePollInterval)
	}

	if result.PID > 0 && bestEffortStopProcess(result.PID, 2*time.Second) {
		if err := cleanupDaemonTransientFiles(paths); err != nil {
			return daemonStopResult{}, err
		}
		return result, nil
	}

	return daemonStopResult{}, fmt.Errorf("timed out waiting for daemon %s to release ownership", snapshot.Record.InstanceID)
}

func inspectDaemonSnapshot(paths daemonPaths) (daemonStatusSnapshot, error) {
	lockHeld, err := daemonLockHeld(paths.lockPath)
	if err != nil {
		return daemonStatusSnapshot{}, err
	}
	record, hasRecord, recordErr := readDaemonStatusRecord(paths.statusPath)
	pid, _ := readPIDFile(paths.pidPath)
	if pid == 0 && hasRecord {
		pid = record.PID
	}
	snapshot := daemonStatusSnapshot{
		Record:    record,
		HasRecord: hasRecord,
		RecordErr: recordErr,
		LockHeld:  lockHeld,
		PID:       pid,
	}
	deriveDaemonStatusSnapshot(&snapshot, time.Now().UTC())
	return snapshot, nil
}

func deriveDaemonStatusSnapshot(snapshot *daemonStatusSnapshot, now time.Time) {
	if snapshot.RecordErr != nil {
		if snapshot.LockHeld {
			snapshot.Live = true
			snapshot.StatusReason = "daemon lock is held but the status record is unreadable"
		}
		return
	}
	if !snapshot.HasRecord {
		if snapshot.LockHeld {
			snapshot.Live = true
			snapshot.StatusReason = "daemon lock is held but no status record is available"
		}
		return
	}

	if daemonStateKnown(snapshot.Record.State) {
		snapshot.HeartbeatFresh = daemonTimestampFresh(snapshot.Record.HeartbeatAt, now)
	}

	if daemonStateIsActive(snapshot.Record.State) {
		snapshot.Live = snapshot.LockHeld || snapshot.HeartbeatFresh
		snapshot.Stale = !snapshot.HeartbeatFresh
	}
	if snapshot.Record.ReadyAt != "" && snapshot.Record.State == daemonStateRunning && snapshot.HeartbeatFresh {
		snapshot.Ready = true
	}

	switch {
	case !daemonStateKnown(snapshot.Record.State):
		if snapshot.LockHeld {
			snapshot.Live = true
		}
		snapshot.StatusReason = fmt.Sprintf("status record contains unknown state %q", snapshot.Record.State)
	case snapshot.Record.HeartbeatAt == "" && daemonStateIsActive(snapshot.Record.State):
		if snapshot.LockHeld {
			snapshot.Live = true
		}
		snapshot.StatusReason = "status record is missing heartbeat_at"
	case snapshot.Stale && snapshot.LockHeld:
		snapshot.StatusReason = "heartbeat expired while daemon lock is still held"
	case snapshot.Stale:
		snapshot.StatusReason = "heartbeat expired and daemon lock is free"
	case snapshot.LockHeld && !daemonStateIsActive(snapshot.Record.State):
		snapshot.Live = true
		snapshot.StatusReason = "daemon lock is held but the status record is not in an active lifecycle state"
	}
}

func buildDaemonStatusView(configPath string, cfg config.Config, paths daemonPaths, snapshot daemonStatusSnapshot, storageStatus daemonStorageStatus, storageErr error) daemonStatusView {
	record := snapshot.Record
	state := daemonStateStopped
	if snapshot.HasRecord && snapshot.RecordErr == nil && daemonStateKnown(record.State) {
		state = record.State
	} else if snapshot.LockHeld {
		state = daemonStateStarting
	}
	view := daemonStatusView{
		SchemaVersion:          firstNonZero(record.SchemaVersion, daemonStatusSchemaVersion),
		InstanceID:             record.InstanceID,
		State:                  state,
		Running:                snapshot.Live,
		Live:                   snapshot.Live,
		Ready:                  snapshot.Ready,
		Stale:                  snapshot.Stale,
		HeartbeatFresh:         snapshot.HeartbeatFresh,
		StatusReason:           snapshot.StatusReason,
		StalePID:               snapshot.Stale && snapshot.PID > 0 && !snapshot.Live,
		PID:                    snapshot.PID,
		StartedAt:              record.StartedAt,
		ReadyAt:                record.ReadyAt,
		HeartbeatAt:            record.HeartbeatAt,
		LastTransitionAt:       record.LastTransitionAt,
		LastError:              record.LastError,
		LastProcessedRequestID: record.LastProcessedRequestID,
		LastControlAction:      record.LastControlAction,
		Runtime: daemonRuntimeStatus{
			ConfigPath:           firstNonEmpty(record.ConfigPath, configPath),
			PollInterval:         firstNonEmpty(record.PollInterval, cfg.PollInterval),
			ErrorBackoff:         firstNonEmpty(record.ErrorBackoff, cfg.ErrorBackoff),
			MaxConsecutiveErrors: firstNonZero(record.MaxConsecutiveErrors, cfg.MaxConsecutiveErrors),
		},
		Paths: daemonResolvedPaths{
			StatePath:      absolutePathOrValue(cfg.StatePath),
			LedgerPath:     absolutePathOrValue(cfg.LedgerPath),
			PIDPath:        absolutePathOrValue(paths.pidPath),
			LogPath:        absolutePathOrValue(paths.logPath),
			StatusPath:     absolutePathOrValue(paths.statusPath),
			MetaPath:       absolutePathOrValue(paths.metaPath),
			LockPath:       absolutePathOrValue(paths.lockPath),
			ControlDirPath: absolutePathOrValue(paths.controlDirPath),
		},
	}
	if storageErr != nil {
		view.StorageError = storageErr.Error()
	} else {
		copy := storageStatus
		view.Storage = &copy
	}
	return view
}

func writeDaemonStatusReport(writer io.Writer, view daemonStatusView) error {
	var b strings.Builder
	appendKeyValueSection(&b, "Daemon",
		[2]string{"State", view.State},
		[2]string{"Live", yesNo(view.Live)},
		[2]string{"Ready", yesNo(view.Ready)},
		[2]string{"Stale", yesNo(view.Stale)},
		[2]string{"Heartbeat fresh", yesNo(view.HeartbeatFresh)},
		[2]string{"PID", intString(view.PID)},
		[2]string{"Instance ID", view.InstanceID},
		[2]string{"Started", view.StartedAt},
		[2]string{"Ready at", view.ReadyAt},
		[2]string{"Heartbeat", view.HeartbeatAt},
		[2]string{"Last transition", view.LastTransitionAt},
		[2]string{"Config", view.Runtime.ConfigPath},
		[2]string{"Status reason", view.StatusReason},
		[2]string{"Last error", view.LastError},
	)
	appendKeyValueSection(&b, "Runtime",
		[2]string{"Poll interval", view.Runtime.PollInterval},
		[2]string{"Error backoff", view.Runtime.ErrorBackoff},
		[2]string{"Max consecutive errors", intString(view.Runtime.MaxConsecutiveErrors)},
	)
	appendKeyValueSection(&b, "Control",
		[2]string{"Last request ID", emptyStatusValue(view.LastProcessedRequestID)},
		[2]string{"Last action", emptyStatusValue(view.LastControlAction)},
	)
	appendKeyValueSection(&b, "Paths",
		[2]string{"State DB", view.Paths.StatePath},
		[2]string{"Ledger DB", view.Paths.LedgerPath},
		[2]string{"PID file", view.Paths.PIDPath},
		[2]string{"Log file", view.Paths.LogPath},
		[2]string{"Status file", view.Paths.StatusPath},
		[2]string{"Metadata file", view.Paths.MetaPath},
		[2]string{"Lock file", view.Paths.LockPath},
		[2]string{"Control dir", view.Paths.ControlDirPath},
	)
	if view.StorageError != "" {
		appendKeyValueSection(&b, "Storage",
			[2]string{"Error", view.StorageError},
		)
	} else if view.Storage != nil {
		appendKeyValueSection(&b, "Storage",
			[2]string{"Configured", yesNo(view.Storage.Configured)},
			[2]string{"Usage bytes", int64String(view.Storage.UsageBytes)},
			[2]string{"Managed path count", intString(view.Storage.ManagedPathCount)},
			[2]string{"Max storage bytes", int64String(view.Storage.MaxStorageBytes)},
			[2]string{"Prune target bytes", int64String(view.Storage.PruneTargetBytes)},
			[2]string{"Desired usage bytes", int64String(view.Storage.DesiredUsageBytes)},
			[2]string{"Free bytes", uint64String(view.Storage.FreeBytes)},
			[2]string{"Min free bytes", int64String(view.Storage.MinFreeBytes)},
			[2]string{"Needs enforcement", needsEnforcementText(view.Storage)},
		)
		if view.Storage.LastActivity != nil {
			appendKeyValueSection(&b, "Storage activity",
				[2]string{"Outcome", view.Storage.LastActivity.Outcome},
				[2]string{"Timestamp", view.Storage.LastActivity.Timestamp},
				[2]string{"Reason", view.Storage.LastActivity.Reason},
				[2]string{"Usage bytes", int64String(view.Storage.LastActivity.UsageBytes)},
				[2]string{"Free bytes", uint64String(view.Storage.LastActivity.FreeBytes)},
				[2]string{"Pruned records", int64String(view.Storage.LastActivity.PrunedRecords)},
				[2]string{"Safe prune offset", int64String(view.Storage.LastActivity.SafePruneOffset)},
			)
		}
	}
	_, err := io.WriteString(writer, strings.TrimRight(b.String(), "\n")+"\n")
	return err
}

func needsEnforcementText(status *daemonStorageStatus) string {
	if status == nil {
		return ""
	}
	if status.NeedsEnforcement {
		if status.Reason != "" {
			return "yes (" + status.Reason + ")"
		}
		return "yes"
	}
	return "no"
}

func firstNonZero(primary, fallback int) int {
	if primary != 0 {
		return primary
	}
	return fallback
}

func intString(value int) string {
	if value == 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func int64String(value int64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

func uint64String(value uint64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatUint(value, 10)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func emptyStatusValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "none"
	}
	return value
}

func daemonStorageStatusFor(cfg config.Config, logPath string) (daemonStorageStatus, error) {
	guard := storageguard.New(cfg, nil, nil)
	report, err := guard.Inspect()
	if err != nil {
		return daemonStorageStatus{}, err
	}
	status := daemonStorageStatus{
		Configured:        report.MaxStorageBytes > 0 || report.MinFreeBytes > 0 || report.PruneTargetBytes > 0,
		UsageBytes:        report.UsageBytes,
		FreeBytes:         report.FreeBytes,
		ManagedPathCount:  len(report.ManagedPaths),
		MaxStorageBytes:   report.MaxStorageBytes,
		PruneTargetBytes:  report.PruneTargetBytes,
		DesiredUsageBytes: report.DesiredUsageBytes,
		MinFreeBytes:      report.MinFreeBytes,
		NeedsEnforcement:  report.NeedsEnforcement,
		Reason:            report.Reason,
	}
	for _, managedPath := range report.ManagedPaths {
		status.ManagedPaths = append(status.ManagedPaths, absolutePathOrValue(managedPath))
	}
	activity, err := readLastStorageActivity(logPath)
	if err != nil {
		return status, err
	}
	status.LastActivity = activity
	return status, nil
}

func readLastStorageActivity(logPath string) (*daemonStorageActivity, error) {
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var last string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.Contains(line, "storage guard") {
			last = line
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if last == "" {
		return nil, nil
	}
	activity, ok := parseDaemonStorageActivity(last)
	if !ok {
		return nil, nil
	}
	return &activity, nil
}

func parseDaemonStorageActivity(line string) (daemonStorageActivity, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return daemonStorageActivity{}, false
	}
	activity := daemonStorageActivity{Raw: line}
	if strings.HasPrefix(line, "[") {
		if end := strings.Index(line, "] "); end > 1 {
			activity.Timestamp = line[1:end]
			line = line[end+2:]
		}
	}
	switch {
	case strings.HasPrefix(line, "storage guard: "):
		activity.Outcome = "enforced"
		body := strings.TrimPrefix(line, "storage guard: ")
		var ok bool
		var value string
		body, value, ok = cutLastField(body, " safe_prune_offset=")
		if ok {
			activity.SafePruneOffset = parseInt64Default(value)
		}
		body, value, ok = cutLastField(body, " pruned_records=")
		if ok {
			activity.PrunedRecords = parseInt64Default(value)
		}
		body, value, ok = cutLastField(body, " free_bytes=")
		if ok {
			activity.FreeBytes = uint64(parseInt64Default(value))
		}
		body, value, ok = cutLastField(body, " usage_bytes=")
		if ok {
			activity.UsageBytes = parseInt64Default(value)
		}
		body = strings.TrimSpace(strings.TrimPrefix(body, "reason="))
		activity.Reason = body
		return activity, true
	case strings.HasPrefix(line, "storage guard failed"):
		activity.Outcome = "failed"
		if idx := strings.Index(line, ": "); idx >= 0 {
			activity.Reason = strings.TrimSpace(line[idx+2:])
		} else {
			activity.Reason = line
		}
		return activity, true
	default:
		return daemonStorageActivity{}, false
	}
}

func cutLastField(value, separator string) (string, string, bool) {
	idx := strings.LastIndex(value, separator)
	if idx < 0 {
		return value, "", false
	}
	return value[:idx], value[idx+len(separator):], true
}

func parseInt64Default(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func readDaemonStatusRecord(path string) (daemonStatusRecord, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return daemonStatusRecord{}, false, nil
		}
		return daemonStatusRecord{}, false, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return daemonStatusRecord{}, false, errors.New("status record is empty")
	}
	var record daemonStatusRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return daemonStatusRecord{}, false, err
	}
	return record, true, nil
}

func daemonStateKnown(state string) bool {
	switch strings.TrimSpace(state) {
	case daemonStateStarting, daemonStateRunning, daemonStateStopping, daemonStateStopped, daemonStateFailed:
		return true
	default:
		return false
	}
}

func daemonStateIsActive(state string) bool {
	switch strings.TrimSpace(state) {
	case daemonStateStarting, daemonStateRunning, daemonStateStopping:
		return true
	default:
		return false
	}
}

func daemonTimestampFresh(raw string, now time.Time) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return false
	}
	return now.Sub(timestamp) <= daemonStatusStaleThreshold
}

func acquireDaemonLock(path string) (*daemonLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errDaemonLockHeld
		}
		return nil, wrapDaemonLockError(err)
	}
	return &daemonLock{file: file}, nil
}

func daemonLockHeld(path string) (bool, error) {
	lock, err := acquireDaemonLock(path)
	if err != nil {
		if errors.Is(err, errDaemonLockHeld) {
			return true, nil
		}
		return false, err
	}
	lock.Release()
	return false, nil
}

func (l *daemonLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}

func wrapDaemonLockError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS) {
		return fmt.Errorf("%w: daemon lifecycle state must live on a local filesystem; network-mounted state directories are unsupported", err)
	}
	return err
}

func cleanupDaemonTransientFiles(paths daemonPaths) error {
	if err := os.MkdirAll(paths.controlDirPath, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(paths.controlDirPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(paths.controlDirPath, entry.Name())); err != nil {
			return err
		}
	}
	if err := os.Remove(paths.pidPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func writePIDFile(path string, pid int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.Itoa(pid)), 0o644)
}

func readPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func writeDaemonControlRequest(controlDir string, request daemonControlRequest) error {
	filename := request.RequestID + ".json"
	return writeAtomicJSON(filepath.Join(controlDir, filename), request)
}

func processDaemonControlRequests(paths daemonPaths, manager *daemonStatusManager, instanceID string) (bool, error) {
	if err := os.MkdirAll(paths.controlDirPath, 0o755); err != nil {
		return false, err
	}
	entries, err := os.ReadDir(paths.controlDirPath)
	if err != nil {
		return false, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	current := manager.snapshot()
	for _, entry := range entries {
		path := filepath.Join(paths.controlDirPath, entry.Name())
		if entry.IsDir() {
			if err := os.RemoveAll(path); err != nil {
				return false, err
			}
			continue
		}
		request, err := readDaemonControlRequest(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] ignoring malformed daemon control request %s: %v\n", time.Now().UTC().Format(time.RFC3339), path, err)
			_ = os.Remove(path)
			continue
		}
		if strings.TrimSpace(request.TargetInstanceID) != instanceID {
			_ = os.Remove(path)
			continue
		}
		if request.RequestID == "" || request.RequestID == current.LastProcessedRequestID {
			_ = os.Remove(path)
			continue
		}
		if request.Action != "stop" {
			if err := manager.ackRequest(request, "unsupported:"+request.Action, ""); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] daemon control acknowledgement failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
			}
			_ = os.Remove(path)
			current = manager.snapshot()
			continue
		}
		if err := manager.markStopping(request); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] daemon stopping transition failed: %v\n", time.Now().UTC().Format(time.RFC3339), err)
		}
		_ = os.Remove(path)
		return true, nil
	}
	return false, nil
}

func readDaemonControlRequest(path string) (daemonControlRequest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return daemonControlRequest{}, err
	}
	var request daemonControlRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return daemonControlRequest{}, err
	}
	return request, nil
}

func writeAtomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeAtomicFile(path, data)
}

func writeAtomicFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp-" + newDaemonID("write")
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func newDaemonID(prefix string) string {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UTC().UnixNano(), hex.EncodeToString(suffix[:]))
}

func (m *daemonStatusManager) snapshot() daemonStatusRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.record
}

func (m *daemonStatusManager) persist() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistLocked()
}

func (m *daemonStatusManager) persistLocked() error {
	return writeAtomicJSON(m.paths.statusPath, m.record)
}

func (m *daemonStatusManager) update(update func(record *daemonStatusRecord, now time.Time)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	update(&m.record, now)
	return m.persistLocked()
}

func (m *daemonStatusManager) heartbeat() error {
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
	})
}

func (m *daemonStatusManager) markRunning() error {
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.State = daemonStateRunning
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
		record.LastTransitionAt = now.Format(time.RFC3339Nano)
		record.LastError = ""
		if record.ReadyAt == "" {
			record.ReadyAt = now.Format(time.RFC3339Nano)
		}
	})
}

func (m *daemonStatusManager) markStopping(request daemonControlRequest) error {
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.State = daemonStateStopping
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
		record.LastTransitionAt = now.Format(time.RFC3339Nano)
		record.LastProcessedRequestID = request.RequestID
		record.LastControlAction = request.Action
		record.LastError = ""
	})
}

func (m *daemonStatusManager) ackRequest(request daemonControlRequest, action, lastError string) error {
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
		record.LastProcessedRequestID = request.RequestID
		record.LastControlAction = action
		record.LastError = lastError
	})
}

func (m *daemonStatusManager) markStopped() error {
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.State = daemonStateStopped
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
		record.LastTransitionAt = now.Format(time.RFC3339Nano)
		record.LastError = ""
	})
}

func (m *daemonStatusManager) markFailed(runErr error) error {
	trimmed := strings.TrimSpace(runErr.Error())
	return m.update(func(record *daemonStatusRecord, now time.Time) {
		record.State = daemonStateFailed
		record.HeartbeatAt = now.Format(time.RFC3339Nano)
		record.LastTransitionAt = now.Format(time.RFC3339Nano)
		record.LastError = trimmed
	})
}

func bestEffortStopProcess(pid int, timeout time.Duration) bool {
	if pid <= 0 || daemonDisableProcessTest {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			return true
		}
		time.Sleep(daemonLifecyclePollInterval)
	}
	if err := proc.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return false
	}
	time.Sleep(200 * time.Millisecond)
	return !processExists(pid)
}

func processExists(pid int) bool {
	if pid <= 0 || daemonDisableProcessTest {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err == nil && processSignalIndicatesExists(proc.Signal(syscall.Signal(0))) {
		return true
	}
	output, psErr := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "pid=").Output()
	if psErr != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

func processSignalIndicatesExists(err error) bool {
	return err == nil || errors.Is(err, syscall.EPERM) || errors.Is(err, os.ErrPermission)
}

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/storageguard"
	"github.com/open-agent-stream/open-agent-stream/internal/supervisor"
)

type daemonPaths struct {
	pidPath  string
	logPath  string
	metaPath string
}

type daemonMetadata struct {
	PID                  int    `json:"pid"`
	ConfigPath           string `json:"config_path"`
	LogPath              string `json:"log_path"`
	StartedAt            string `json:"started_at"`
	PollInterval         string `json:"poll_interval"`
	ErrorBackoff         string `json:"error_backoff"`
	MaxConsecutiveErrors int    `json:"max_consecutive_errors"`
}

type daemonStorageStatus struct {
	Configured        bool                   `json:"configured"`
	UsageBytes        int64                  `json:"usage_bytes"`
	FreeBytes         uint64                 `json:"free_bytes,omitempty"`
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

func daemonStartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon start", daemonConfigHelp{
		Description: "Start a detached daemon process and return immediately.",
		Notes: []string{
			"Safe to rerun: if the daemon already appears to be running for this config, OAS prints the existing PID instead of starting a second copy.",
			"Detached mode writes stdout/stderr to the resolved daemon log path.",
		},
		Examples: []string{
			"oas daemon start -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	metadata, _ := readDaemonMetadata(paths.metaPath)
	if pid, running := readPID(paths.pidPath, metadata); running {
		fmt.Printf("Daemon already running (pid=%d)\n", pid)
		return
	}
	clearStaleDaemonFiles(paths)
	exePath, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	pid, err := startDetachedDaemon(exePath, configPath, paths.logPath)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("Started daemon (pid=%d, log=%s)\n", pid, paths.logPath)
}

func daemonStopCommand(args []string) {
	cfg, _ := mustDaemonConfig(args, "daemon stop", daemonConfigHelp{
		Description: "Stop the detached daemon for this config if it is running.",
		Notes: []string{
			"Safe to run when the daemon is already stopped.",
			"OAS clears stale daemon control files when it can prove the recorded PID is no longer the right daemon.",
		},
		Examples: []string{
			"oas daemon stop -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	pid, err := stopDaemon(paths, 15*time.Second)
	if err != nil {
		fatal(err)
	}
	if pid == 0 {
		fmt.Println("Daemon not running.")
		return
	}
	fmt.Printf("Stopped daemon (pid=%d)\n", pid)
}

func daemonStatusCommand(args []string) {
	fs := flag.NewFlagSet("daemon status", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	asJSON := fs.Bool("json", false, "print status as JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas daemon status -config <path> [flags]

Show daemon status, storage visibility, and resolved control-file paths.

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
	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(err)
	}
	paths := daemonPathsFor(cfg)
	metadata, _ := readDaemonMetadata(paths.metaPath)
	pid, running := readPID(paths.pidPath, metadata)
	storageStatus, storageErr := daemonStorageStatusFor(cfg, paths.logPath)

	payload := map[string]any{
		"running":                running,
		"pid":                    pid,
		"started_at":             metadata.StartedAt,
		"config_path":            metadata.ConfigPath,
		"log_path":               paths.logPath,
		"pid_path":               paths.pidPath,
		"meta_path":              paths.metaPath,
		"poll_interval":          metadata.PollInterval,
		"error_backoff":          metadata.ErrorBackoff,
		"max_consecutive_errors": metadata.MaxConsecutiveErrors,
	}
	if storageErr != nil {
		payload["storage_error"] = storageErr.Error()
	} else {
		payload["storage"] = storageStatus
	}
	if *asJSON {
		if err := writeJSON(os.Stdout, payload); err != nil {
			fatal(err)
		}
		return
	}

	switch {
	case running:
		fmt.Printf("Daemon running (pid=%d)\n", pid)
	case pid > 0:
		fmt.Printf("Daemon not running (stale pid=%d)\n", pid)
	default:
		fmt.Println("Daemon not running.")
	}
	if metadata.StartedAt != "" {
		fmt.Printf("Started: %s\n", metadata.StartedAt)
	}
	if metadata.ConfigPath != "" {
		fmt.Printf("Config: %s\n", metadata.ConfigPath)
	}
	fmt.Printf("PID file: %s\n", paths.pidPath)
	fmt.Printf("Log file: %s\n", paths.logPath)
	if metadata.PollInterval != "" {
		fmt.Printf("Poll interval: %s\n", metadata.PollInterval)
	}
	if metadata.ErrorBackoff != "" {
		fmt.Printf("Error backoff: %s\n", metadata.ErrorBackoff)
	}
	if metadata.MaxConsecutiveErrors > 0 {
		fmt.Printf("Max consecutive errors: %d\n", metadata.MaxConsecutiveErrors)
	}
	if storageErr != nil {
		fmt.Printf("Storage: error: %v\n", storageErr)
		return
	}
	fmt.Println("Storage:")
	fmt.Printf("  Usage bytes: %d\n", storageStatus.UsageBytes)
	fmt.Printf("  Managed paths: %d\n", len(storageStatus.ManagedPaths))
	if storageStatus.MaxStorageBytes > 0 {
		fmt.Printf("  Max storage bytes: %d\n", storageStatus.MaxStorageBytes)
	}
	if storageStatus.DesiredUsageBytes > 0 {
		fmt.Printf("  Desired usage bytes: %d\n", storageStatus.DesiredUsageBytes)
	}
	if storageStatus.FreeBytes > 0 {
		fmt.Printf("  Free bytes: %d\n", storageStatus.FreeBytes)
	}
	if storageStatus.MinFreeBytes > 0 {
		fmt.Printf("  Min free bytes: %d\n", storageStatus.MinFreeBytes)
	}
	if storageStatus.NeedsEnforcement {
		fmt.Printf("  Needs enforcement: yes (%s)\n", storageStatus.Reason)
	} else {
		fmt.Println("  Needs enforcement: no")
	}
	if storageStatus.LastActivity != nil {
		fmt.Printf("  Last storage event: %s at %s\n", storageStatus.LastActivity.Outcome, storageStatus.LastActivity.Timestamp)
		if storageStatus.LastActivity.PrunedRecords > 0 {
			fmt.Printf("  Last pruned records: %d\n", storageStatus.LastActivity.PrunedRecords)
		}
		if storageStatus.LastActivity.Reason != "" {
			fmt.Printf("  Last event detail: %s\n", storageStatus.LastActivity.Reason)
		}
	}
}

func daemonRestartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon restart", daemonConfigHelp{
		Description: "Stop the detached daemon for this config, then start a new detached process.",
		Notes: []string{
			"Useful after changing config values that affect continuous mode or storage behavior.",
		},
		Examples: []string{
			"oas daemon restart -config ./examples/config.example.json",
		},
	})
	paths := daemonPathsFor(cfg)
	if oldPID, err := stopDaemon(paths, 15*time.Second); err != nil {
		fatal(err)
	} else if oldPID > 0 {
		fmt.Printf("Stopped daemon (pid=%d)\n", oldPID)
	}
	clearStaleDaemonFiles(paths)
	exePath, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	pid, err := startDetachedDaemon(exePath, configPath, paths.logPath)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("Started daemon (pid=%d, log=%s)\n", pid, paths.logPath)
}

type daemonConfigHelp struct {
	Description string
	Notes       []string
	Examples    []string
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
	return daemonPaths{
		pidPath:  filepath.Join(baseDir, prefix+".pid"),
		logPath:  filepath.Join(baseDir, prefix+".log"),
		metaPath: filepath.Join(baseDir, prefix+".json"),
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
	if existingPID := readExistingPID(paths); existingPID > 0 {
		return fmt.Errorf("daemon already running (pid=%d)", existingPID)
	}
	clearStaleDaemonFiles(paths)
	pid := os.Getpid()
	metadata := daemonMetadata{
		PID:                  pid,
		ConfigPath:           configPath,
		LogPath:              paths.logPath,
		StartedAt:            time.Now().UTC().Format(time.RFC3339),
		PollInterval:         cfg.PollInterval,
		ErrorBackoff:         cfg.ErrorBackoff,
		MaxConsecutiveErrors: cfg.MaxConsecutiveErrors,
	}
	if err := os.WriteFile(paths.pidPath, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return err
	}
	if err := writeDaemonMetadata(paths.metaPath, metadata); err != nil {
		_ = os.Remove(paths.pidPath)
		return err
	}
	defer clearDaemonFiles(paths)

	runtime, err := supervisor.New(cfg)
	if err != nil {
		return err
	}
	defer runtime.Close(ctx)

	fmt.Fprintf(os.Stderr, "[%s] daemon running (pid=%d, config=%s)\n", time.Now().UTC().Format(time.RFC3339), pid, configPath)
	if err := runtime.RunContinuously(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	fmt.Fprintf(os.Stderr, "[%s] daemon stopped\n", time.Now().UTC().Format(time.RFC3339))
	return nil
}

func startDetachedDaemon(exePath, configPath, logPath string) (int, error) {
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return 0, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()

	cmd := exec.Command(exePath, "daemon", "run", "-config", configPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}

func stopDaemon(paths daemonPaths, timeout time.Duration) (int, error) {
	metadata, _ := readDaemonMetadata(paths.metaPath)
	pid, running := readPID(paths.pidPath, metadata)
	if !running {
		clearStaleDaemonFiles(paths)
		return 0, nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		clearStaleDaemonFiles(paths)
		return 0, nil
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return pid, err
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			clearDaemonFiles(paths)
			return pid, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := proc.Signal(syscall.SIGKILL); err != nil {
		return pid, err
	}
	time.Sleep(200 * time.Millisecond)
	clearDaemonFiles(paths)
	return pid, nil
}

func readExistingPID(paths daemonPaths) int {
	metadata, _ := readDaemonMetadata(paths.metaPath)
	pid, running := readPID(paths.pidPath, metadata)
	if running {
		return pid
	}
	return 0
}

func readPID(pidPath string, metadata daemonMetadata) (int, bool) {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false
	}
	return pid, daemonProcessMatches(pid, metadata)
}

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func writeDaemonMetadata(path string, metadata daemonMetadata) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func readDaemonMetadata(path string) (daemonMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return daemonMetadata{}, err
	}
	var metadata daemonMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return daemonMetadata{}, err
	}
	return metadata, nil
}

func clearDaemonFiles(paths daemonPaths) {
	_ = os.Remove(paths.pidPath)
	_ = os.Remove(paths.metaPath)
}

func clearStaleDaemonFiles(paths daemonPaths) {
	metadata, _ := readDaemonMetadata(paths.metaPath)
	if _, running := readPID(paths.pidPath, metadata); !running {
		clearDaemonFiles(paths)
	}
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
		ManagedPaths:      append([]string(nil), report.ManagedPaths...),
		MaxStorageBytes:   report.MaxStorageBytes,
		PruneTargetBytes:  report.PruneTargetBytes,
		DesiredUsageBytes: report.DesiredUsageBytes,
		MinFreeBytes:      report.MinFreeBytes,
		NeedsEnforcement:  report.NeedsEnforcement,
		Reason:            report.Reason,
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

func daemonProcessMatches(pid int, metadata daemonMetadata) bool {
	if !processExists(pid) {
		return false
	}
	if metadata.PID != pid || strings.TrimSpace(metadata.ConfigPath) == "" {
		return false
	}
	cmdline, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return false
	}
	parts := strings.Split(string(cmdline), "\x00")
	wantConfig := strings.TrimSpace(metadata.ConfigPath)
	hasDaemon := false
	hasRun := false
	hasConfig := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		switch part {
		case "daemon":
			hasDaemon = true
		case "run":
			hasRun = true
		case wantConfig:
			hasConfig = true
		}
	}
	return hasDaemon && hasRun && hasConfig
}

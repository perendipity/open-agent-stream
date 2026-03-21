package main

import (
	"context"
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

func daemonStartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon start")
	paths := daemonPathsFor(cfg)
	if pid, running := readPID(paths.pidPath); running {
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
	cfg, _ := mustDaemonConfig(args, "daemon stop")
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
	cfg, _ := mustDaemonConfig(args, "daemon status")
	paths := daemonPathsFor(cfg)
	pid, running := readPID(paths.pidPath)
	metadata, _ := readDaemonMetadata(paths.metaPath)

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
}

func daemonRestartCommand(args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon restart")
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

func mustDaemonConfig(args []string, name string) (config.Config, string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
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
	prefix := "oas-daemon-" + instance
	return daemonPaths{
		pidPath:  filepath.Join(baseDir, prefix+".pid"),
		logPath:  filepath.Join(baseDir, prefix+".log"),
		metaPath: filepath.Join(baseDir, prefix+".json"),
	}
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
	if existingPID := readExistingPID(paths.pidPath); existingPID > 0 {
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
	pid, running := readPID(paths.pidPath)
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

func readExistingPID(pidPath string) int {
	pid, running := readPID(pidPath)
	if running {
		return pid
	}
	return 0
}

func readPID(pidPath string) (int, bool) {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false
	}
	return pid, processExists(pid)
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
	if _, running := readPID(paths.pidPath); !running {
		clearDaemonFiles(paths)
	}
}

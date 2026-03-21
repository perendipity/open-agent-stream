package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/supervisor"
	"github.com/open-agent-stream/open-agent-stream/pkg/conformance"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "--help", "-h", "help":
		usage()
		return
	case "run":
		runCommand(ctx, os.Args[2:])
	case "daemon":
		daemonCommand(ctx, os.Args[2:])
	case "config":
		configCommand(os.Args[2:])
	case "replay":
		replayCommand(ctx, os.Args[2:])
	case "export":
		exportCommand(ctx, os.Args[2:])
	case "doctor":
		doctorCommand(ctx, os.Args[2:])
	case "validate":
		validateCommand(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas run -config <path>

Run one ingestion + normalization + delivery cycle.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas run -config ./examples/config.example.json
`)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)
	if err := runtime.Run(ctx); err != nil {
		fatal(err)
	}
}

func daemonCommand(ctx context.Context, args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		daemonUsage()
		if len(args) == 0 {
			os.Exit(2)
		}
		return
	}
	switch args[0] {
	case "run":
		daemonRunCommand(ctx, args[1:])
	case "start":
		daemonStartCommand(args[1:])
	case "stop":
		daemonStopCommand(args[1:])
	case "status":
		daemonStatusCommand(args[1:])
	case "restart":
		daemonRestartCommand(args[1:])
	default:
		daemonUsage()
		os.Exit(2)
	}
}

func daemonRunCommand(ctx context.Context, args []string) {
	cfg, configPath := mustDaemonConfig(args, "daemon run")
	if err := runDaemonForeground(ctx, cfg, configPath); err != nil && err != context.Canceled {
		fatal(err)
	}
}

func configCommand(args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		configUsage()
		if len(args) == 0 {
			os.Exit(2)
		}
		return
	}
	switch args[0] {
	case "init":
		configInitCommand(args[1:])
	case "print":
		configPrintCommand(args[1:])
	case "validate":
		configValidateCommand(args[1:])
	default:
		configUsage()
		os.Exit(2)
	}
}

func configInitCommand(args []string) {
	fs := flag.NewFlagSet("config init", flag.ExitOnError)
	outputPath := fs.String("output", "-", "output path or - for stdout")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas config init [flags]

Write a starter config with documented defaults.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas config init
  oas config init -output ./oas.json
`)
	}
	_ = fs.Parse(args)

	cfg := config.Config{
		Version:              "0.1",
		MachineID:            "local-dev",
		StatePath:            filepath.Join(config.DefaultDataDir(), "state.db"),
		LedgerPath:           filepath.Join(config.DefaultDataDir(), "ledger.db"),
		MaxStorageBytes:      100 * 1024 * 1024,
		PruneTargetBytes:     80 * 1024 * 1024,
		MinFreeBytes:         50 * 1024 * 1024,
		PollInterval:         "3s",
		ErrorBackoff:         "10s",
		MaxConsecutiveErrors: 10,
		Sources: []sourceapi.Config{
			{InstanceID: "codex-local", Type: "codex_local", Root: "./fixtures/sources/codex"},
			{InstanceID: "claude-local", Type: "claude_local", Root: "./fixtures/sources/claude"},
		},
		Sinks: []sinkapi.Config{
			{ID: "stdout", Type: "stdout", Options: map[string]string{}},
		},
	}
	writeConfigOutput(*outputPath, cfg)
}

func configPrintCommand(args []string) {
	fs := flag.NewFlagSet("config print", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas config print -config <path>

Print the effective config after defaults are applied.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas config print -config ./examples/config.example.json
`)
	}
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(err)
	}
	paths := daemonPathsFor(cfg)
	payload := map[string]any{
		"config": cfg,
		"resolved": map[string]any{
			"daemon_pid_path":  paths.pidPath,
			"daemon_log_path":  paths.logPath,
			"daemon_meta_path": paths.metaPath,
			"default_data_dir": config.DefaultDataDir(),
		},
	}
	if err := writeJSON(os.Stdout, payload); err != nil {
		fatal(err)
	}
}

func configValidateCommand(args []string) {
	fs := flag.NewFlagSet("config validate", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	rootPath := fs.String("root", ".", "repository root")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas config validate -config <path> [flags]

Validate the config file and fixture/conformance inputs.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas config validate -config ./examples/config.example.json
`)
	}
	_ = fs.Parse(args)

	if *configPath != "" {
		if _, err := config.Load(*configPath); err != nil {
			fatal(err)
		}
	}
	absRoot, err := filepath.Abs(*rootPath)
	if err != nil {
		fatal(err)
	}
	if err := conformance.ValidateFixtures(absRoot); err != nil {
		fatal(err)
	}
	fmt.Println("config and fixtures validated")
}

func replayCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	sinkIDs := fs.String("sinks", "", "comma-separated sink IDs to replay to")
	includeNonIdempotent := fs.Bool("include-non-idempotent", false, "include append-only and side-effecting sinks during replay")
	dryRun := fs.Bool("dry-run", false, "print the replay sink selection plan and exit")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas replay -config <path> [flags]

Replay previously ingested ledger entries to selected sinks.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas replay -config ./examples/config.example.json
  oas replay -config ./examples/config.example.json -dry-run
  oas replay -config ./examples/config.example.json -include-non-idempotent -sinks jsonl-local
`)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)
	options := supervisor.ReplayOptions{
		SinkIDs:              splitCSV(*sinkIDs),
		IncludeNonIdempotent: *includeNonIdempotent,
	}
	if *dryRun {
		plan, err := runtime.ReplayPlan(options)
		if err != nil {
			fatal(err)
		}
		if err := writeJSON(os.Stdout, plan); err != nil {
			fatal(err)
		}
		return
	}
	if err := runtime.ReplayWithOptions(ctx, options); err != nil {
		fatal(err)
	}
}

func exportCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	outputPath := fs.String("output", "-", "output file path or - for stdout")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas export -config <path> [flags]

Export a deterministic JSONL snapshot from the ledger.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas export -config ./examples/config.example.json -output ./exports/events.jsonl
`)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	writer, err := supervisor.EnsureOutputWriter(*outputPath)
	if err != nil {
		fatal(err)
	}
	defer writer.Close()

	if err := runtime.Export(ctx, writer); err != nil {
		fatal(err)
	}
}

func doctorCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas doctor -config <path>

Run operational checks and print results as JSON.

Flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  oas doctor -config ./examples/config.example.json
`)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	checks, err := runtime.Doctor(ctx)
	if err != nil {
		fatal(err)
	}
	if err := supervisor.WriteChecks(os.Stdout, checks); err != nil {
		fatal(err)
	}
}

func validateCommand(args []string) {
	configValidateCommand(args)
}

func mustRuntime(configPath string) *supervisor.Runtime {
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(err)
	}
	runtime, err := supervisor.New(cfg)
	if err != nil {
		fatal(err)
	}
	return runtime
}

func closeRuntime(ctx context.Context, runtime *supervisor.Runtime) {
	if err := runtime.Close(ctx); err != nil {
		fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `oas - open-agent-stream CLI

Usage:
  oas <command> [flags]

Core commands:
  run       Run one ingestion/normalization/delivery cycle
  daemon    Manage continuous daemon mode
  config    Initialize, print, or validate config
  replay    Replay ledger entries to sinks
  export    Export canonical events as JSONL
  doctor    Run operational checks
  validate  Validate config and fixtures (alias of "config validate")

Use:
  oas <command> --help
  oas daemon --help
  oas config --help
`)
}

func daemonUsage() {
	fmt.Fprintf(os.Stderr, `usage: oas daemon <subcommand> -config <path>

Subcommands:
  run       Run the daemon in the foreground
  start     Start a detached daemon
  stop      Stop a detached daemon
  status    Show daemon status and resolved file paths
  restart   Restart a detached daemon

Examples:
  oas daemon start -config ./examples/config.example.json
  oas daemon status -config ./examples/config.example.json
  oas daemon stop -config ./examples/config.example.json
`)
}

func configUsage() {
	fmt.Fprintf(os.Stderr, `usage: oas config <subcommand> [flags]

Subcommands:
  init      Write a starter config
  print     Print the effective config after defaults
  validate  Validate config and fixtures

Examples:
  oas config init
  oas config print -config ./examples/config.example.json
  oas config validate -config ./examples/config.example.json
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func writeJSON(target *os.File, value any) error {
	encoder := json.NewEncoder(target)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeConfigOutput(path string, value any) {
	writer, err := supervisor.EnsureOutputWriter(path)
	if err != nil {
		fatal(err)
	}
	defer writer.Close()
	if err := writeJSONToWriter(writer, value); err != nil {
		fatal(err)
	}
}

func writeJSONToWriter(target io.Writer, value any) error {
	encoder := json.NewEncoder(target)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

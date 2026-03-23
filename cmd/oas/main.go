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
	case "summary":
		summaryCommand(ctx, os.Args[2:])
	case "inspect":
		inspectCommand(ctx, os.Args[2:])
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

This is the one-shot path. For continuous collection, use:
  oas daemon run
  oas daemon start

`)
		printFlagSection(os.Stderr, fs, "Common flags", usageFlag{Name: "config", Placeholder: "<path>"})
		printExamples(os.Stderr,
			"oas run -config ./examples/config.example.json",
		)
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
	cfg, configPath := mustDaemonConfig(args, "daemon run", daemonConfigHelp{
		Description: "Run the daemon in the foreground until interrupted or until continuous mode exits loudly after repeated failures.",
		Notes: []string{
			"Foreground mode is useful under tmux, systemd, or another process supervisor.",
			"Use `oas daemon start` if you want OAS to detach and return immediately.",
		},
		Examples: []string{
			"oas daemon run -config ./examples/config.example.json",
		},
	})
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

`)
		printFlagSection(os.Stderr, fs, "Common flags", usageFlag{Name: "output", Placeholder: "<path|->"})
		printExamples(os.Stderr,
			"oas config init",
			"oas config init -output ./oas.json",
		)
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
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a readable summary")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas config print -config <path>

Print the effective config after defaults are applied, plus the resolved paths OAS will use.

`)
		printFlagSection(os.Stderr, fs, "Common flags", usageFlag{Name: "config", Placeholder: "<path>"})
		printFlagSection(os.Stderr, fs, "Advanced flags", usageFlag{Name: "json"})
		printExamples(os.Stderr,
			"oas config print -config ./examples/config.example.json",
			"oas config print -config ./examples/config.example.json -json",
		)
	}
	_ = fs.Parse(args)

	absConfigPath, err := filepath.Abs(*configPath)
	if err != nil {
		fatal(err)
	}
	cfg, err := config.Load(absConfigPath)
	if err != nil {
		fatal(err)
	}
	view := buildConfigInspectView(absConfigPath, cfg)
	if *jsonOutput {
		if err := writeJSON(os.Stdout, view); err != nil {
			fatal(err)
		}
		return
	}
	if err := writeConfigInspectReport(os.Stdout, view); err != nil {
		fatal(err)
	}
}

func configValidateCommand(args []string) {
	configValidateCommandWithName(args, "config validate")
}

func configValidateCommandWithName(args []string, commandName string) {
	fs := flag.NewFlagSet(commandName, flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	rootPath := fs.String("root", ".", "repository root")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas %s -config <path> [flags]

Validate the config file and fixture/conformance inputs.

`, commandName)
		printFlagSection(os.Stderr, fs, "Common flags", usageFlag{Name: "config", Placeholder: "<path>"})
		printFlagSection(os.Stderr, fs, "Advanced flags", usageFlag{Name: "root", Placeholder: "<path>"})
		if commandName == "validate" {
			printExamples(os.Stderr,
				"oas validate -config ./examples/config.example.json",
				"oas config validate -config ./examples/config.example.json",
			)
			return
		}
		printExamples(os.Stderr,
			"oas config validate -config ./examples/config.example.json",
		)
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

By default, replay includes only replay-safe sinks. Use the advanced flags
below to inspect or override that plan.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "dry-run"},
			usageFlag{Name: "sinks", Placeholder: "<id[,id]>"},
			usageFlag{Name: "include-non-idempotent"},
		)
		printExamples(os.Stderr,
			"oas replay -config ./examples/config.example.json",
			"oas replay -config ./examples/config.example.json -dry-run",
			"oas replay -config ./examples/config.example.json -include-non-idempotent -sinks jsonl-local",
		)
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

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
			usageFlag{Name: "output", Placeholder: "<path|->"},
		)
		printExamples(os.Stderr,
			"oas export -config ./examples/config.example.json -output ./exports/events.jsonl",
		)
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
	jsonOutput := fs.Bool("json", false, "print structured JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oas doctor -config <path>

Run operational checks and print a readable table by default.

`)
		printFlagSection(os.Stderr, fs, "Common flags",
			usageFlag{Name: "config", Placeholder: "<path>"},
		)
		printFlagSection(os.Stderr, fs, "Advanced flags",
			usageFlag{Name: "json"},
		)
		printExamples(os.Stderr,
			"oas doctor -config ./examples/config.example.json",
			"oas doctor -config ./examples/config.example.json -json",
		)
	}
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)

	checks, err := runtime.Doctor(ctx)
	if err != nil {
		fatal(err)
	}
	if *jsonOutput {
		if err := supervisor.WriteChecksJSON(os.Stdout, checks); err != nil {
			fatal(err)
		}
		return
	}
	if err := supervisor.WriteChecksTable(os.Stdout, checks); err != nil {
		fatal(err)
	}
}

func validateCommand(args []string) {
	configValidateCommandWithName(args, "validate")
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
  config    Initialize, inspect, or validate config
  replay    Replay ledger entries to sinks
  export    Export canonical events as JSONL
  summary   Summarize canonical events by session
  inspect   Inspect one session in reviewer-friendly detail
  doctor    Run operational checks
  validate  Validate config and fixtures (alias of "config validate")

Use:
  oas <command> --help
  oas daemon --help
  oas config --help

Common starts:
  oas config init -output ./oas.json
  oas run -config ./oas.json
  oas daemon start -config ./oas.json
  oas export -config ./oas.json -output ./exports/events.jsonl
  oas summary -input ./exports/events.jsonl
`)
}

func daemonUsage() {
	fmt.Fprintf(os.Stderr, `usage: oas daemon <subcommand> -config <path>

Subcommands:
  run       Run the daemon in the foreground
  start     Start a detached daemon
  stop      Stop a detached daemon
  status    Show daemon status, storage visibility, and resolved file paths
  restart   Restart a detached daemon

Examples:
  oas daemon run -config ./examples/config.example.json
  oas daemon start -config ./examples/config.example.json
  oas daemon status -config ./examples/config.example.json
  oas daemon restart -config ./examples/config.example.json
  oas daemon stop -config ./examples/config.example.json

Use:
  oas daemon <subcommand> --help
`)
}

func configUsage() {
	fmt.Fprintf(os.Stderr, `usage: oas config <subcommand> [flags]

Subcommands:
  init      Write a starter config
  print     Print the effective config and resolved paths
  validate  Validate config and fixtures

Examples:
  oas config init
  oas config print -config ./examples/config.example.json
  oas config validate -config ./examples/config.example.json

Use:
  oas config <subcommand> --help
`)
}

type usageFlag struct {
	Name        string
	Placeholder string
}

func printFlagSection(writer io.Writer, fs *flag.FlagSet, title string, flags ...usageFlag) {
	rows := make([][2]string, 0, len(flags))
	width := 0
	for _, spec := range flags {
		flagInfo := fs.Lookup(spec.Name)
		if flagInfo == nil {
			continue
		}
		label := "-" + flagInfo.Name
		if spec.Placeholder != "" {
			label += " " + spec.Placeholder
		}
		detail := flagInfo.Usage + helpDefaultSuffix(flagInfo.DefValue)
		rows = append(rows, [2]string{label, detail})
		if len(label) > width {
			width = len(label)
		}
	}
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(writer, "%s:\n", title)
	for _, row := range rows {
		fmt.Fprintf(writer, "  %-*s  %s\n", width, row[0], row[1])
	}
	fmt.Fprintln(writer)
}

func printExamples(writer io.Writer, examples ...string) {
	if len(examples) == 0 {
		return
	}
	fmt.Fprintln(writer, "Examples:")
	for _, example := range examples {
		fmt.Fprintf(writer, "  %s\n", example)
	}
}

func helpDefaultSuffix(value string) string {
	switch value {
	case "", "false":
		return ""
	default:
		if strings.ContainsAny(value, " \t") {
			return fmt.Sprintf(" (default %q)", value)
		}
		return fmt.Sprintf(" (default %s)", value)
	}
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

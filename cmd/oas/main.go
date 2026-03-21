package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/internal/supervisor"
	"github.com/open-agent-stream/open-agent-stream/pkg/conformance"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "run":
		runCommand(ctx, os.Args[2:])
	case "daemon":
		daemonCommand(ctx, os.Args[2:])
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
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)
	if err := runtime.Run(ctx); err != nil {
		fatal(err)
	}
}

func daemonCommand(ctx context.Context, args []string) {
	if len(args) == 0 {
		daemonUsage()
		os.Exit(2)
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

func replayCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	sinkIDs := fs.String("sinks", "", "comma-separated sink IDs to replay to")
	includeNonIdempotent := fs.Bool("include-non-idempotent", false, "include append-only and side-effecting sinks during replay")
	dryRun := fs.Bool("dry-run", false, "print the replay sink selection plan and exit")
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
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	rootPath := fs.String("root", ".", "repository root")
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
	fmt.Fprintln(os.Stderr, "usage: oas <run|daemon|replay|export|doctor|validate> [flags]")
}

func daemonUsage() {
	fmt.Fprintln(os.Stderr, "usage: oas daemon <run|start|stop|status|restart> -config <path>")
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

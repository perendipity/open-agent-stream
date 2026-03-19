package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
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

func replayCommand(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	configPath := fs.String("config", "", "path to config JSON")
	_ = fs.Parse(args)

	runtime := mustRuntime(*configPath)
	defer closeRuntime(ctx, runtime)
	if err := runtime.Replay(ctx); err != nil {
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
	fmt.Fprintln(os.Stderr, "usage: oas <run|replay|export|doctor|validate> [flags]")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

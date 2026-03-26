package main

import (
	"bytes"
	"testing"
)

func TestWriteUsageSnapshot(t *testing.T) {
	var out bytes.Buffer
	writeUsage(&out)

	const want = `oas - open-agent-stream CLI

Usage:
  oas <command> [flags]

Core commands:
  version   Print the installed CLI version
  run       Run one ingestion/normalization/delivery cycle
  daemon    Manage continuous daemon mode
  delivery  Inspect and retry sealed delivery batches
  config    Initialize, inspect, or validate config
  replay    Replay ledger entries to sinks
  export    Export canonical events as JSONL
  analytics Build, query, export, and inspect DuckDB analytics
  summary   Summarize canonical events by session
  inspect   Inspect one session in reviewer-friendly detail
  doctor    Run operational checks
  validate  Validate config and fixtures (alias of "config validate")

Use:
  oas <command> --help
  oas daemon --help
  oas delivery --help
  oas config --help
  oas analytics --help

Common starts:
  oas version
  oas config init -output ./oas.json
  oas run -config ./oas.json
  oas daemon start -config ./oas.json
  oas delivery status -config ./oas.json
  oas export -config ./oas.json -output ./exports/events.jsonl
  oas analytics build -config ./oas.json
  oas summary -input ./exports/events.jsonl
`
	if got := out.String(); got != want {
		t.Fatalf("writeUsage() mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestWriteAnalyticsUsageSnapshot(t *testing.T) {
	var out bytes.Buffer
	writeAnalyticsUsage(&out)

	const want = `usage: oas analytics <subcommand> -config <path>

Subcommands:
  build     Build or refresh the local DuckDB analytics cache
  query     Run presets or custom SQL against analytics views
  export    Export a Parquet analytics snapshot plus manifest.json
  status    Show analytics cache lineage and completeness

Examples:
  oas analytics build -config ./oas.json
  oas analytics query -config ./oas.json -preset attention
  oas analytics export -config ./oas.json -output ./exports/analytics
  oas analytics status -config ./oas.json

Use:
  oas analytics <subcommand> --help
`
	if got := out.String(); got != want {
		t.Fatalf("writeAnalyticsUsage() mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestWriteDaemonUsageSnapshot(t *testing.T) {
	var out bytes.Buffer
	writeDaemonUsage(&out)

	const want = `usage: oas daemon <subcommand> -config <path>

Subcommands:
  run       Run the daemon in the foreground
  start     Start a detached daemon
  stop      Stop a detached daemon
  status    Show daemon/runtime status, resolved paths, and storage activity
  restart   Restart a detached daemon

Examples:
  oas daemon run -config ./examples/config.example.json
  oas daemon start -config ./examples/config.example.json
  oas daemon status -config ./examples/config.example.json
  oas daemon restart -config ./examples/config.example.json
  oas daemon stop -config ./examples/config.example.json

Use:
  oas daemon <subcommand> --help
`
	if got := out.String(); got != want {
		t.Fatalf("writeDaemonUsage() mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestWriteDeliveryUsageSnapshot(t *testing.T) {
	var out bytes.Buffer
	writeDeliveryUsage(&out)

	const want = `usage: oas delivery <subcommand> -config <path>

Subcommands:
  status    Show per-sink delivery queue and watermark state
  list      List pending, retrying, or quarantined delivery batches
  inspect   Inspect one sealed delivery batch
  retry     Requeue one quarantined delivery batch

Examples:
  oas delivery status -config ./examples/config.example.json
  oas delivery list -config ./examples/config.example.json -status quarantined
  oas delivery inspect -config ./examples/config.example.json -batch batch_123
  oas delivery retry -config ./examples/config.example.json -batch batch_123

Use:
  oas delivery <subcommand> --help
`
	if got := out.String(); got != want {
		t.Fatalf("writeDeliveryUsage() mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestWriteConfigUsageSnapshot(t *testing.T) {
	var out bytes.Buffer
	writeConfigUsage(&out)

	const want = `usage: oas config <subcommand> [flags]

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
`
	if got := out.String(); got != want {
		t.Fatalf("writeConfigUsage() mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

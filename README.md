# open-agent-stream

`open-agent-stream` is a local-first event pipeline and open standard for agentic work.

It is designed to capture source-native coding-agent artifacts, append them to an immutable local ledger, normalize them into a portable canonical event stream, and route those events to one or more sinks without binding the core to any single product's semantics.

## Thesis

The project optimizes for:

- source volatility
- replayability
- privacy
- portability
- community extensibility

It explicitly does **not** optimize for tightly coupled product semantics.

## Current Status

This repository is intentionally spec-first. The initial implementation includes:

- versioned normative specs under `/spec`
- fixtures plus executable conformance-style tests
- a Go reference collector with:
  - source adapters for Codex-style and Claude-style local artifacts
  - a SQLite-backed raw ledger and state store
  - a canonical normalizer
  - neutral sinks for `jsonl`, `sqlite`, `stdout`, `http`, `command`, and `s3`, plus a `webhook` compatibility alias to `http`
  - CLI commands for `run`, `daemon`, `replay`, `export`, `analytics`, `doctor`, and `validate`

## Non-Goals

- product-specific sink semantics in the core repo
- LLM-powered summarization in the ingestion path
- vendor-defined canonical schemas
- a distributed ingestion system in v0

## Repo Map

- `/docs`: project intent, architecture, governance, and roadmap
- `/spec`: normative versioned specs and schemas
- `/fixtures`: captured examples and expected canonical outputs
- `/conformance`: contract tests over fixtures and runtime behavior
- `/cmd/oas`: CLI entrypoint
- `/pkg`: stable public contracts
- `/internal`: runtime implementation details
- `/sources`: built-in adapters
- `/sinks`: built-in neutral sinks

## Install

For early adopters today, the canonical install path is:

```bash
go install github.com/open-agent-stream/open-agent-stream/cmd/oas@latest
oas version
```

If you are evaluating a local checkout instead:

```bash
go install ./cmd/oas
oas version
```

For the current distribution reality and PATH troubleshooting, see
[`docs/adoption/install.md`](docs/adoption/install.md).

If you want to run the fixture-backed demo below, use a local checkout of this
repo so the starter config can point at `./fixtures/...`.

## Quickstart

The demo flow below uses repository fixtures, so run these commands from the
repo root.

### 1. Generate a starter config

```bash
oas config init -output ./oas.json
```

This creates a starter config with current defaults, fixture-backed Codex and
Claude sources for a local demo, and a `stdout` sink. Treat it as a safe demo
config, not a serious multi-machine starting point.

For real use:

- keep the live config outside the repo checkout, usually under
  `~/.config/open-agent-stream/` or `/etc/open-agent-stream/`
- replace the fixture roots with real local session roots before you run OAS
- use a private ops repo or an existing infrastructure repo for committed
  machine configs and shared sink templates

If you want fuller shape references, see
[`examples/config.example.json`](examples/config.example.json) and the examples
index in [`examples/README.md`](examples/README.md).

### 2. Inspect and validate what OAS will actually use

```bash
oas config print -config ./oas.json
oas config validate -config ./oas.json
```

- `oas config print` shows a readable effective-config summary by default,
  including resolved state, ledger, daemon, and source-root paths.
- add `-json` if you want structured output for automation.
- `oas config validate` checks both the config and the repo's bundled fixtures
  when you run it from an `open-agent-stream` checkout. If you're using an
  installed binary elsewhere, either pass `-root /path/to/open-agent-stream` or
  skip fixture validation.

### 3. Run one ingestion cycle end to end

```bash
oas run -config ./oas.json
```

With the default starter config, this ingests the fixture sources in
`./fixtures/sources/...`, appends them to the local ledger, normalizes them,
and writes canonical events to stdout.

For serious local use, avoid pointing OAS at top-level agent home directories
such as `~/.codex` or `~/.claude`; those trees usually contain unrelated JSON.
Prefer session subtrees such as:

- Codex: `~/.codex/sessions`, optionally `~/.codex/archived_sessions`
- Claude: `~/.claude/projects`

### 4. Export a deterministic review file

```bash
mkdir -p ./exports
oas export -config ./oas.json -output ./exports/events.jsonl
wc -l ./exports/events.jsonl
```

After the fixture-backed run above, the export should contain six canonical
events.

### 5. Review exported session data

For a built-in reviewer-oriented session summary:

```bash
oas summary -input ./exports/events.jsonl -sort recent -limit 20
oas summary -input ./exports/events.jsonl -failed
```

To drill into one interesting session from that summary:

```bash
oas inspect -input ./exports/events.jsonl -session <session_key>
oas inspect -input ./exports/events.jsonl -session <session_key> -command-status attention
oas inspect -input ./exports/events.jsonl -session <session_key> -command-limit 0
```

For a quick event-level skim:

```bash
jq -r '[.timestamp, .session_key, .kind] | @tsv' ./exports/events.jsonl
```

For a reviewer-oriented session summary:

```bash
jq -s 'group_by(.session_key) | map({session_key: .[0].session_key, events: length, kinds: (map(.kind) | unique)})' ./exports/events.jsonl
```

What to look for:

- `session_key` groups events that belong to the same captured session
- `kind` shows the interaction shape (`session.started`, `message.user`,
  `command.finished`, `tool.failed`, ...)
- `export` gives reviewers a deterministic JSONL snapshot without re-delivering
  to sinks
- `summary` turns exported JSONL into a stable per-session review table with
  project paths plus optional recent/biggest/failed triage knobs, without
  requiring `jq`
- `inspect` turns one `session_key` from that summary into a session-level view
  with project path, duration, collapsed command summaries, failure signals,
  tool failures, an `ATTENTION EVENTS` section with short failure excerpts for
  non-zero command exits and tool failures when output is available, plus
  backfilled command context when the raw failure event omitted the command
  text, and a compact timeline; `-command-status attention` isolates failed or
  incomplete command rows, and `-command-limit 0` restores the full command
  list when needed

### 6. Build local analytics

```bash
oas analytics build -config ./oas.json
oas analytics query -config ./oas.json
oas analytics query -config ./oas.json -preset failures
oas analytics export -config ./oas.json -output ./exports/analytics
oas analytics status -config ./oas.json
```

`oas analytics` builds a local DuckDB cache on demand from retained ledger
history. It is not part of the ingest hot path. The stable portable contract is
the exported Parquet snapshot plus `manifest.json`; the local DuckDB layout is
an implementation detail except for the public query views.

### 7. Move from one-shot runs to continuous collection

```bash
oas daemon start -config ./oas.json
oas daemon status -config ./oas.json
oas daemon status -config ./oas.json -json
oas daemon stop -config ./oas.json
```

`oas daemon status` now surfaces current storage usage, configured limits, the
most recent storage-guard event it can recover from the daemon log, and a more
structured JSON view of runtime settings plus resolved status, lock, and
control paths for automation.

Daemon lifecycle is now daemon-owned rather than PID-probe-owned:

- `<prefix>.json` is the live status record for the current daemon instance
- `<prefix>.lock` is the exclusivity primitive that prevents duplicate starts
- `<prefix>.control.d/` holds stop requests the daemon acknowledges on its next
  control poll

The JSON status output keeps `running` as a compatibility alias for `live`, and
also exposes explicit `live`, `ready`, heartbeat freshness, lifecycle `state`,
and the current `instance_id`. `start` waits for the matching instance to
become ready before returning, and `restart` will fail rather than spawning a
duplicate if the previous owner does not release the lock.

Store daemon state on a local filesystem. Network-mounted state directories are
not supported for daemon lifecycle control because lock semantics are not
portable there.

### 8. Configure remote destinations

Built-in remote sinks use two config blocks:

- `settings`: sink-specific destination fields such as URL, headers, bucket,
  key templates, argv, and env-var references for credentials
- `delivery`: timing and retry policy such as batch size, batch age, fixed
  release windows, retry backoff, and poison-batch thresholds

Common built-in choices:

- `http`: send neutral OAS batches to a proprietary API or generic HTTP
  collector
- `command`: stage a sealed payload file and invoke your own transport such as
  `rsync`, `scp`, or a wrapper script
- `s3`: upload sealed batch objects to a bucket with a deterministic object key

For serious multi-machine use, do not keep the live destination config inside
the public OAS repo checkout. Keep committed configs in a private ops repo and
render or copy the live machine config into `~/.config/open-agent-stream/`.
See [`docs/adoption/config-management.md`](docs/adoption/config-management.md),
[`docs/adoption/remote-destinations.md`](docs/adoption/remote-destinations.md),
and [`docs/adoption/multi-machine.md`](docs/adoption/multi-machine.md).

A minimal `http` sink looks like:

```json
{
  "id": "remote-http",
  "type": "http",
  "settings": {
    "url": "https://collector.example.invalid/ingest",
    "method": "POST",
    "format": "oas_batch_json",
    "bearer_token_env": "OAS_REMOTE_TOKEN"
  },
  "delivery": {
    "max_batch_events": 100,
    "max_batch_age": "5s",
    "retry_initial_backoff": "2s",
    "retry_max_backoff": "1m",
    "poison_after_failures": 5
  }
}
```

For a full guide with `http`, `command`, and `s3` examples plus a safe local
validation flow, see
[`docs/adoption/remote-destinations.md`](docs/adoption/remote-destinations.md).

If you are validating a local checkout in daemon mode, prefer a real built
binary rather than `go run` so `oas daemon start` re-execs the exact build you
tested:

```bash
go build -o ./bin/oas ./cmd/oas
./bin/oas config print -config ./oas.json
./bin/oas daemon start -config ./oas.json
./bin/oas daemon status -config ./oas.json -json
./bin/oas doctor -config ./oas.json
./bin/oas daemon stop -config ./oas.json
```

### 9. Run operational checks

```bash
oas doctor -config ./oas.json
oas doctor -config ./oas.json -json
```

`oas doctor` prints a readable table by default and supports `-json` for
automation.

### 10. Replay when you want to re-deliver ledger history

```bash
oas replay -config ./oas.json
oas replay -config ./oas.json -dry-run
```

Notes:

- `go install github.com/open-agent-stream/open-agent-stream/cmd/oas@latest` is the primary install path today.
- The demo starter config points at repository fixtures, so run the Quickstart from the repo root or change the generated source roots to your real local artifacts.
- `oas validate` is repo-checkout-aware because it also validates bundled fixtures; for installed-binary use outside the repo, pass `-root /path/to/open-agent-stream` or skip that step.
- Default persistent storage should live in a durable app state directory. If you omit
  `state_path`/`ledger_path`, OAS defaults to `XDG_STATE_HOME/open-agent-stream` or
  `~/.local/state/open-agent-stream`.
- `oas <command> --help` is intended to be the primary discovery surface for common and advanced usage.
- `sqlite` is replay-safe by default because it converges by `event_id`.
- `jsonl` is an append-only delivery sink, so replay will duplicate lines if you explicitly include it.
- `s3` is also append-only by default; use deterministic keys and expect replay to stay disabled unless you explicitly opt in.
- `http`, `command`, `external`, and `webhook` are side-effecting delivery sinks and are skipped by default during replay.
- `export` is the deterministic way to produce a JSONL snapshot from the ledger for review or downstream tooling.
- `max_storage_bytes` lets the daemon enforce a storage budget for its managed files.
  When usage exceeds the budget, OAS prunes safely delivered ledger rows, compacts the
  SQLite stores, and exits loudly if it still cannot get back under the configured limit.
- Before handing OAS to an early adopter, read [`docs/adoption/early-adopters.md`](docs/adoption/early-adopters.md) for current guardrails and evaluation expectations.
- For remote sink setup and validation, read [`docs/adoption/remote-destinations.md`](docs/adoption/remote-destinations.md).
- For multi-machine rollout and service management, read [`docs/adoption/multi-machine.md`](docs/adoption/multi-machine.md).

## Extending OAS

If you want to build a third-party adapter or sink, start with:

- [`docs/integrations/README.md`](docs/integrations/README.md)
- [`docs/governance/compatibility-matrix.md`](docs/governance/compatibility-matrix.md)

Today the public contracts are ready for authoring against `/pkg`, and the
stock CLI supports built-in sinks plus out-of-process `external` sinks. In
practice, that means:

- upstream an adapter or built-in sink here if you want it available in the
  stock `oas` CLI for everyone,
- ship an external sink executable if you want a proprietary or unbundled
  destination without forking the runtime, or
- maintain a custom CLI overlay when you need a custom source adapter or a
  deeper runtime change than the published sink/runtime contracts cover

See [`docs/integrations/README.md`](docs/integrations/README.md) and
[`rfcs/0002-external-plugin-runtime.md`](rfcs/0002-external-plugin-runtime.md)
for the current boundary.

## How To Read This Repo

1. Start with [`docs/README.md`](docs/README.md).
2. Read the architecture docs before the runtime packages.
3. Treat `/spec` as the normative source of truth.
4. Use `/fixtures` and `/conformance` to understand expected behavior.
5. Read `/internal` only after the contracts make sense.

## Licensing

- Code: Apache-2.0
- Specs, docs, RFCs, and fixtures: CC BY 4.0

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
  - neutral sinks for `jsonl`, `sqlite`, `stdout`, and `webhook`
  - CLI commands for `run`, `daemon`, `replay`, `export`, `doctor`, and `validate`

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
Claude sources for a local demo, and a `stdout` sink. If you want a fuller
reference config, see [`examples/config.example.json`](examples/config.example.json).

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

### 6. Move from one-shot runs to continuous collection

```bash
oas daemon start -config ./oas.json
oas daemon status -config ./oas.json
oas daemon status -config ./oas.json -json
oas daemon stop -config ./oas.json
```

`oas daemon status` now surfaces current storage usage, configured limits, the
most recent storage-guard event it can recover from the daemon log, and a more
structured JSON view of runtime settings and resolved paths for automation.

### 7. Run operational checks

```bash
oas doctor -config ./oas.json
oas doctor -config ./oas.json -json
```

`oas doctor` prints a readable table by default and supports `-json` for
automation.

### 8. Replay when you want to re-deliver ledger history

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
- `export` is the deterministic way to produce a JSONL snapshot from the ledger for review or downstream tooling.
- `max_storage_bytes` lets the daemon enforce a storage budget for its managed files.
  When usage exceeds the budget, OAS prunes safely delivered ledger rows, compacts the
  SQLite stores, and exits loudly if it still cannot get back under the configured limit.
- Before handing OAS to an early adopter, read [`docs/adoption/early-adopters.md`](docs/adoption/early-adopters.md) for current guardrails and evaluation expectations.

## Extending OAS

If you want to build a third-party adapter or sink, start with:

- [`docs/integrations/README.md`](docs/integrations/README.md)
- [`docs/governance/compatibility-matrix.md`](docs/governance/compatibility-matrix.md)

Today the public contracts are ready for authoring against `/pkg`, but the
stock CLI still wires only the built-in types. In practice, that means:

- upstream an adapter or sink here if you want it available in the stock `oas`
  CLI, or
- maintain a small custom CLI overlay if you want to move faster against the
  published contracts before plugin loading exists in the stock runtime

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

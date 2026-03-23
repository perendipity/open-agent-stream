# Early Adopter Guide

This guide is the practical "what should I expect?" document for the first
people trying OAS.

## What is ready today

OAS is in good shape for:

- local-first collection from the built-in Codex and Claude fixture-backed
  source families
- deterministic export to JSONL for review and downstream tooling
- reviewer-oriented session triage with `oas summary` and `oas inspect`
- continuous collection with daemon mode and storage-budget visibility

## What is still intentionally early

Please share OAS as an early-adopter tool, not as a finished general-purpose
platform:

- source and sink support is still intentionally small and built-in
- external plugin loading is not part of the stock runtime yet
- distribution is currently source-install-first rather than package-manager-first
- the CLI is now much more stable, but output and workflows may still sharpen
  as more real usage comes in

## Good first evaluation flow

The smoothest first run is:

```bash
oas config init -output ./oas.json
oas config print -config ./oas.json
oas validate -config ./oas.json
oas run -config ./oas.json
oas export -config ./oas.json -output ./exports/events.jsonl
oas summary -input ./exports/events.jsonl
```

That path exercises config generation, config inspectability, validation,
ingestion, export, and reviewer UX without requiring a daemon first.

## What OAS stores locally

By default, OAS keeps its durable state under:

- `XDG_STATE_HOME/open-agent-stream`, or
- `~/.local/state/open-agent-stream` when `XDG_STATE_HOME` is unset

The two main SQLite files are:

- `state.db` for checkpoints, sink delivery metadata, and dead letters
- `ledger.db` for the raw immutable envelope ledger

If you run daemon mode, OAS also creates daemon PID, log, and metadata files in
the same state directory.

## Guardrails for real evaluations

- start with local test artifacts or a small non-sensitive workspace first
- keep off-machine sinks disabled until you review the configured privacy policy
- remember that replay-safe sinks are included by default, but append-only sinks
  like JSONL will duplicate output if you explicitly replay to them
- set a `max_storage_bytes` budget if you want the daemon to enforce a hard cap
  on managed files

## How to reset a local evaluation

If you want to start clean, remove the state directory referenced by your
resolved config output, including:

- `state.db*`
- `ledger.db*`
- any `oas-daemon-*.pid`, `oas-daemon-*.log`, and `oas-daemon-*.json` files

`oas config print -config ./oas.json` is the fastest way to confirm the exact
paths a given config will use before you delete anything.

## What feedback is most valuable

The highest-value early-adopter reports include:

- the exact command they ran
- whether they were using built-in sources or a custom integration path
- the expected behavior versus the actual behavior
- a redacted sample export, log excerpt, or failing fixture when possible

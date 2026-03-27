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
- remote delivery through the built-in `http`, `command`, and `s3` sinks when
  you want near-real-time forwarding or scheduled batch release

## What is still intentionally early

Please share OAS as an early-adopter tool, not as a finished general-purpose
platform:

- source and sink support is still intentionally small and built-in
- external plugin loading is not part of the stock runtime yet
- distribution is currently source-install-first rather than package-manager-first
- the CLI is now much more stable, but output and workflows may still sharpen
  as more real usage comes in

## Good first evaluation flow

There are two good first-run paths, depending on how you're evaluating OAS.

### A. Repo-backed demo flow

If you have a local checkout of `open-agent-stream` and want deterministic demo
data from the bundled fixtures, run this from the repo root:

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

### B. Real local evaluation flow

If you installed `oas` outside a repo checkout and want to point it at real
local artifacts:

1. run `oas config init -output ./oas.json`
2. edit the generated source roots away from `./fixtures/...` and point them at
   your real local Codex/Claude artifact directories
3. run `oas config print -config ./oas.json`
4. optionally run `oas doctor -config ./oas.json`
5. run `oas run -config ./oas.json`
6. export and review with `oas export`, `oas summary`, and `oas inspect`

If you also want fixture/conformance validation, run:

```bash
oas validate -config ./oas.json -root /path/to/open-agent-stream
```

without `-root`, `oas validate` will fail outside an `open-agent-stream` repo
checkout because it also validates the bundled fixtures.

Prefer session roots, not top-level agent home directories:

- Codex: `~/.codex/sessions`, optionally `~/.codex/archived_sessions`
- Claude: `~/.claude/projects`

If you are evaluating a new remote destination, start with a smaller recent
subtree first and widen it after the first successful delivery.

## What OAS stores locally

By default, OAS keeps its durable state under:

- `XDG_STATE_HOME/open-agent-stream`, or
- `~/.local/state/open-agent-stream` when `XDG_STATE_HOME` is unset

The two main SQLite files are:

- `state.db` for checkpoints, sink delivery metadata, and dead letters
- `ledger.db` for the raw immutable envelope ledger

If you run daemon mode, OAS also creates daemon lifecycle files in the same
state directory:

- `oas-daemon-*.json` as the live daemon status record
- `oas-daemon-*.lock` as the exclusivity lock that prevents duplicate starts
- `oas-daemon-*.control.d/` for queued stop requests
- `oas-daemon-*.pid` and `oas-daemon-*.log` as diagnostic artifacts

Keep that state directory on a local filesystem. Network-mounted locations are
not supported for daemon lifecycle control because lock behavior is not
portable there.

## Guardrails for real evaluations

- start with local test artifacts or a small non-sensitive workspace first
- keep off-machine sinks disabled until you review the configured privacy policy
- use [`remote-destinations.md`](remote-destinations.md) for first-run remote
  sink setup and local validation before pointing OAS at a real external system
- keep live configs outside the public repo checkout and manage multi-machine
  configs through a private ops repo or template-render step
- remember that replay-safe sinks are included by default, but append-only sinks
  like JSONL will duplicate output if you explicitly replay to them
- treat `oas validate` warnings about missing local state on shared sinks as a
  real bootstrap risk; the first run will ingest whatever history is visible
  under the configured roots
- set a `max_storage_bytes` budget if you want the daemon to enforce a hard cap
  on managed files
- use `oas daemon status` to distinguish lifecycle `state` from `live` and
  `ready`; a stale heartbeat with a held lock means the old owner may be wedged,
  so do not try to start a second daemon for the same config

## How to reset a local evaluation

If you want to start clean, remove the state directory referenced by your
resolved config output, including:

- `state.db*`
- `ledger.db*`
- any `oas-daemon-*.pid`, `oas-daemon-*.log`, `oas-daemon-*.json`,
  `oas-daemon-*.lock`, and `oas-daemon-*.control.d/` artifacts

If daemon mode was enabled, stop it first. If `oas daemon status` still shows a
held lock with a stale heartbeat, treat that as a wedged owner and clear the
state directory only after you have confirmed no real daemon process should
still be running for that config.

`oas config print -config ./oas.json` is the fastest way to confirm the exact
paths a given config will use before you delete anything.

## What feedback is most valuable

The highest-value early-adopter reports include:

- the exact command they ran
- whether they were using built-in sources or a custom integration path
- the expected behavior versus the actual behavior
- a redacted sample export, log excerpt, or failing fixture when possible

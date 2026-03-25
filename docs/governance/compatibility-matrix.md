# Compatibility Matrix

This is the quick entry point for what the reference implementation ships today
and what third-party authors can reasonably target.

## Built-in source adapters

| Type | Contract | Capabilities | Coverage | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| `codex_local` | `source-adapter/v1` | `messages`, `commands`, `tool_calls`, `file_ops`, `usage` | fixtures + runtime conformance | reference implementation | Reads local Codex-style JSON artifacts. |
| `claude_local` | `source-adapter/v1` | `messages`, `commands`, `tool_calls`, `usage` | fixtures + runtime conformance | reference implementation | Reads local Claude-style JSON artifacts. |

## Built-in sinks

| Type | Contract | Replay class | Required options | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| `stdout` | `sink/v1` | `append_only` | none | reference implementation | Good for demos and local debugging. |
| `jsonl` | `sink/v1` | `append_only` | `options.path` | reference implementation | Appends canonical events to a file; use `oas export` for deterministic snapshots. |
| `sqlite` | `sink/v1` | `idempotent` | `options.path` | reference implementation | Stores canonical events in a queryable local SQLite table. |
| `http` | `sink/v1` | `side_effecting` | `settings.url` | reference implementation | Sends sealed batches to an HTTP endpoint with retry/permanent classification rules. |
| `webhook` | `sink/v1` | `side_effecting` | `settings.url` | compatibility alias | Alias to `http` with `POST` and OAS batch JSON defaults. |
| `command` | `sink/v1` | `side_effecting` | `settings.argv` | reference implementation | Hands sealed payload files to a local executable such as `rsync`, `scp`, or a wrapper script. |
| `s3` | `sink/v1` | `append_only` | `settings.bucket` | reference implementation | Uploads sealed batches to S3 with deterministic keys. |
| `external` | `sink/v1` + `sink-runtime/v1` | `side_effecting` | `settings.plugin_type`, `settings.argv` | reference implementation | Invokes an out-of-process sink executable at the sealed-batch boundary. |

## Third-party author status

| Surface | Current state | Stable target today | Not shipped yet |
| --- | --- | --- | --- |
| Adapter authoring | supported at the contract level | `spec/source-adapter/v1`, `pkg/sourceapi`, `pkg/schema`, fixtures + conformance expectations | dynamic registration in the stock `oas` CLI |
| Sink authoring | supported in the stock CLI for built-ins and out-of-process plugins | `spec/sink/v1`, `spec/sink-runtime/v1`, `pkg/sinkapi`, `pkg/externalapi`, `pkg/schema`, replay-class documentation model | dynamic registration for custom source adapters |

## How to interpret status

- **reference implementation** means the type ships in this repo today and is
  covered by the current fixture/runtime test story.
- **supported at the contract level** means third parties can author against the
  published specs and `/pkg` contracts without importing `/internal`.
- Runtime registration is a separate concern from compatibility. An extension
  can be structurally compatible before the stock CLI knows how to instantiate
  it automatically.

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
  - CLI commands for `run`, `replay`, `export`, `doctor`, and `validate`

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

## Quickstart

Create a config file:

```json
{
  "version": "0.1",
  "machine_id": "local-dev",
  "state_path": ".open-agent-stream/state.db",
  "ledger_path": ".open-agent-stream/ledger.db",
  "sources": [
    {
      "instance_id": "codex-local",
      "type": "codex_local",
      "root": "./fixtures/sources/codex"
    },
    {
      "instance_id": "claude-local",
      "type": "claude_local",
      "root": "./fixtures/sources/claude"
    }
  ],
  "sinks": [
    {
      "id": "stdout",
      "type": "stdout"
    }
  ]
}
```

Then run:

```bash
go run ./cmd/oas run -config ./examples/config.example.json
```

## How To Read This Repo

1. Start with [`docs/README.md`](docs/README.md).
2. Read the architecture docs before the runtime packages.
3. Treat `/spec` as the normative source of truth.
4. Use `/fixtures` and `/conformance` to understand expected behavior.
5. Read `/internal` only after the contracts make sense.

## Licensing

- Code: Apache-2.0
- Specs, docs, RFCs, and fixtures: CC BY 4.0


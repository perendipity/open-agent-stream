# Replay Model

Replay is a first-class behavior.

- Raw envelopes are the authoritative ingestion record.
- Canonical events can be recomputed from the ledger.
- Sink delivery is at-least-once, but sink replay safety is sink-specific.
- Spec changes must preserve the ability to replay older raw ledgers under version-aware normalizers.

Replay and export are intentionally different operations:

- `replay` resends events through configured sinks using each sink's delivery semantics.
- `export` materializes a deterministic snapshot from the ledger and does not imply delivery retries or side effects.

The reference implementation treats sink replay classes as:

- `idempotent`: included by default during replay
- `append_only`: skipped by default during replay because duplicate deliveries are expected
- `side_effecting`: skipped by default during replay because replay may trigger external effects

For built-in sinks this means:

- `sqlite` is replay-safe by default because rows converge by `event_id`
- `jsonl` is append-only and records delivery history, so replay duplicates lines unless explicitly enabled
- `stdout` is append-only for the same reason
- `s3` is append-only and will be skipped by default unless the operator explicitly includes it
- `http` is side-effecting and must be explicitly included
- `command` is side-effecting and must be explicitly included
- `webhook` inherits `http` behavior as a compatibility alias and must be explicitly included

If an operator wants a clean JSONL snapshot of the ledger, they should use `oas export`, not rely on replay against the live `jsonl` sink.

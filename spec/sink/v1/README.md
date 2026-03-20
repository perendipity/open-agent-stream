# Sink Contract v1

A sink receives canonical events and may optionally receive raw envelopes.

Required lifecycle:

- init
- send batch
- flush
- health
- close

Delivery is at-least-once. Implementations should use stable event IDs and checkpoints for idempotency.

Replay behavior is sink-specific. A sink implementation should declare one of these replay classes in its documentation:

- `idempotent`: replay is safe by default because duplicate deliveries converge by stable event ID or equivalent semantics.
- `append_only`: replay preserves delivery history and will append duplicate deliveries unless the operator explicitly chooses a snapshot/export workflow instead.
- `side_effecting`: replay may trigger external effects and must require explicit operator intent.

Built-in sink classes in the reference implementation:

- `sqlite`: `idempotent`
- `jsonl`: `append_only`
- `stdout`: `append_only`
- `webhook`: `side_effecting`

`Replay` and `export` are distinct behaviors:

- replay resends canonical events through sink delivery semantics
- export materializes a deterministic snapshot from the ledger without reusing sink delivery behavior

# Replay Model

Replay is a first-class behavior.

- Raw envelopes are the authoritative ingestion record.
- Canonical events can be recomputed from the ledger.
- Sink delivery is at-least-once and idempotent by stable event IDs.
- Spec changes must preserve the ability to replay older raw ledgers under version-aware normalizers.


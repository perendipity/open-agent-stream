# Pipeline

The reference architecture is a six-layer pipeline:

1. Source adapters
2. Raw envelope capture
3. Immutable local ledger
4. Canonical normalizer
5. Per-sink prepared-item persistence
6. Sealed dispatch batches and sink delivery

Optional derivers and indexers sit downstream and must never block raw capture.

## Runtime Components

- Supervisor: lifecycle and wiring
- Source watchers: source discovery and incremental read
- Ledger writer: append-only durability boundary
- Normalizer workers: canonical mapping and parse status
- Router: per-sink fan-out after normalization and redaction
- Delivery manager: prepared-item persistence, batch sealing, retry scheduling,
  quarantine, and per-sink progress tracking
- Derivers: non-blocking downstream enrichment

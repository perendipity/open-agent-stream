# Pipeline

The reference architecture is a five-layer pipeline:

1. Source adapters
2. Raw envelope capture
3. Immutable local ledger
4. Canonical normalizer
5. Router and sink delivery

Optional derivers and indexers sit downstream and must never block raw capture.

## Runtime Components

- Supervisor: lifecycle and wiring
- Source watchers: source discovery and incremental read
- Ledger writer: append-only durability boundary
- Normalizer workers: canonical mapping and parse status
- Router: fan-out, retries, per-sink isolation
- Derivers: non-blocking downstream enrichment


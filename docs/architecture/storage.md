# Storage Model

The reference collector uses three storage layers:

- state SQLite database for checkpoints, health, delivery queues, and dead letters
- ledger SQLite database for append-only raw envelopes
- optional derived stores handled by sinks or future indexers

SQLite is the v0 default because it is transactional, cross-platform, and easy to replay from. A segment-log backend can be added later without changing the standard.


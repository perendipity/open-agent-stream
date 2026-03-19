# Sink Contract v1

A sink receives canonical events and may optionally receive raw envelopes.

Required lifecycle:

- init
- send batch
- flush
- health
- close

Delivery is at-least-once. Implementations should use stable event IDs and checkpoints for idempotency.


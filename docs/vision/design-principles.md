# Design Principles

- User-owned first: the developer's retained session history is the product, not
  a side effect of a vendor UI.
- Raw first: preserve source-native records before interpretation.
- Canonical second: keep the shared schema narrow and stable.
- Replay from local truth: normalize and redeliver from the local ledger.
- Inspect and export cleanly: stable local review and deterministic snapshots
  are first-class workflows.
- Local-first trust: users decide what leaves the machine.
- Product and infrastructure neutrality: the core protocol is not defined by any
  sink or hosted control plane.
- Extensibility by contract: adapters and sinks depend on published interfaces plus conformance.

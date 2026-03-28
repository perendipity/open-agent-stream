# Problem Statement

Developers using coding agents already generate rich session history on their
own machines, but that history is fragmented across incompatible, drifting,
source-native formats. Tooling built directly against those artifacts inherits
vendor-specific assumptions, breaks when formats change, and often ends up
depending on vendor-hosted storage or dashboards to reconstruct past sessions.

`open-agent-stream` exists to separate:

- source-native capture
- durable local retention
- portable event normalization
- local inspection and deterministic export
- sink delivery
- higher-order derivation

The standard should let any developer gather, retain, replay, inspect, and
export their own coding-agent session data across one or more machines without
making an upstream product the center of the architecture.

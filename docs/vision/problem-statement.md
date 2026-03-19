# Problem Statement

Coding agents already emit rich local traces, but they do so in incompatible, drifting, source-native formats. Consumers that build directly against those formats inherit every vendor-specific assumption and break when artifacts change shape.

`open-agent-stream` exists to separate:

- source-native capture
- durable local retention
- portable event normalization
- sink delivery
- higher-order derivation

The standard should let any tool collect local agent traces without making an upstream product the center of the architecture.


# RFC 0002: External Plugin Runtime

- Status: Proposed
- Authors: open-agent-stream maintainers
- Spec version impact: None yet
- Implementation impact: Future

## Summary

Defer external plugin execution until adapter and sink contracts stabilize.

## Motivation

The standard should stabilize before the runtime commits to an isolation boundary such as subprocess, gRPC, or WASI.

## Proposal

The initial runtime ships built-in adapters and sinks only. External plugins remain out of tree until a future RFC chooses a runtime boundary.

## Compatibility

No effect on current spec consumers.

## Migration

Future plugin authors should target the published contracts and fixture/conformance model first.


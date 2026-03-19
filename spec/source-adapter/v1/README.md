# Source Adapter Contract v1

A compliant source adapter must provide:

- artifact discovery
- incremental read with checkpoints
- parse hints for session, project, and timestamp
- capability declaration

Adapters must not:

- enforce sink semantics
- call LLMs
- perform non-local side effects as part of ingestion
- define the canonical schema


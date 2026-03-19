# Failure Model

The collector assumes failures in:

- source reads
- normalization
- sink delivery
- local disk and permissions

Operational rules:

- preserve raw capture before anything else
- record malformed or unknown records in the ledger
- quarantine normalization and sink failures into inspectable local tables
- isolate sink failure from other sinks


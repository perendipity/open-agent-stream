# Glossary

- Source artifact: a source-native file, directory, stream, or socket discovered by an adapter.
- Raw envelope: the first stable internal record captured from a source artifact.
- Immutable local ledger: append-only durable storage for raw envelopes.
- Canonical event: normalized event for portable downstream use.
- Deriver: optional downstream component that produces richer semantics from canonical events.
- Sink: a destination that receives raw envelopes or canonical events.


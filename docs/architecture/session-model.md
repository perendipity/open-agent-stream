# Session Model

Session identity is source-aware but internally stable.

The collector maps:

- `source_instance_id`
- `source_session_key`

into an internal `session_key`.

Normalizers must apply deterministic rules for:

- started
- resumed
- ended
- orphaned

The canonical event stream is ordered by local ledger offset plus per-session sequence.


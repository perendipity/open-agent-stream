# Session Model v1

`SessionIdentity` captures the stable internal mapping from source-native session keys to portable session keys.

The reference implementation derives `session_key` from:

- `source_instance_id`
- `source_session_key`

Normalizers are responsible for maintaining deterministic per-session ordering.


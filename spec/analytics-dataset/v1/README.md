# Analytics Dataset v1

`analytics-dataset/v1` is the portable snapshot contract produced by:

- `oas analytics export`

It is intentionally separate from the local DuckDB cache implementation.

## Snapshot Model

Exports are replaceable snapshots, not append logs.

Downstream systems should merge them by table primary key with upsert or dedupe
semantics, never by blind append.

## Files

An export directory contains:

- `manifest.json`
- `envelope_facts.parquet`
- `event_facts.parquet`
- `session_rollups.parquet`
- `command_rollups.parquet`

## Stable Tables

### `envelope_facts`

One row per retained raw envelope, excluding `raw_payload`.

Primary key:

- `global_envelope_key`

### `event_facts`

One row per canonical event, excluding free-text payload content.

Primary key:

- `global_event_key`

### `session_rollups`

Versioned rollups derived from `event_facts` and `command_rollups`.

Primary key:

- `global_session_key`

### `command_rollups`

Versioned command lifecycle rollups derived from `event_facts`.

Primary key:

- `global_command_rollup_key`

## Identity Rules

The export contract uses merge-safe analytics keys:

- `global_envelope_key = stable(machine_id, envelope_id)`
- `global_event_key = stable(machine_id, event_id)`
- `global_session_key = stable(machine_id, source_instance_id, source_session_key)`
- `global_command_rollup_key = stable(machine_id, derivation_version, start_global_envelope_key, finish_global_envelope_key)`

## Completeness

`manifest.json` carries one of these `coverage_mode` values:

- `complete_history`
- `cache_preserved_history`
- `retained_window_only`

## Privacy

V1 is envelope-only and excludes free-text content:

- no message bodies
- no command strings
- no raw payload JSON
- no stdout or stderr payloads

Only structured metadata, timestamps, identifiers, counts, and selected numeric
fields such as `exit_code` and `duration_ms` are part of the stable contract.

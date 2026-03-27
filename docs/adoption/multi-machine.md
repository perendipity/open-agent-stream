# Multi-Machine Deployment Guide

This guide covers the minimal production shape for running OAS on multiple machines and delivering into one shared destination.

## Deployment model

- run one OAS instance per machine
- give each machine its own config, `state.db`, and `ledger.db`
- assign a unique opaque `machine_id` per machine
- use `event_spec_version: "v2"` on shared-destination sinks so portable exports and remote payloads keep host identity

Each OAS instance owns only its local durable state. Shared destinations are where streams converge.

## Current config model

OAS does not support config includes or overlays today.

That means the practical multi-machine model is:

- one full config file per machine
- one shared sink definition copied or rendered into each machine config
- machine-local values changed per host

Machine-local fields:

- `machine_id`
- `data_dir` or explicit `state_path` and `ledger_path`
- source roots
- any machine-local credential environment

Shared fields:

- sink `id`, `type`, `event_spec_version`, `settings`, and `delivery`
- shared privacy overrides for that sink

For serious use, keep those committed configs in a private ops repo or your
existing infrastructure repo, not in the public OAS source checkout. See
[`config-management.md`](config-management.md).

## Shared destination patterns

### HTTP

- use one `http` sink per machine pointed at the same endpoint
- prefer `event_spec_version: "v2"`
- set `probe_url` so `oas doctor` can verify auth and reachability without sending delivery traffic

### Command

- use `command` when the destination is ultimately another system you control, such as `rsync`, `scp`, or a wrapper script
- keep the command idempotent if possible
- use a dedicated staging directory on each machine

### S3

- use a deterministic `key_template`
- prefer bucket-level default encryption
- keep each machine on its own local ledger/state even if all machines share one bucket prefix
- keep `event_spec_version: "v2"` on the shared sink so payloads retain host identity

## Recommended source roots

Avoid top-level agent home directories such as `~/.codex` or `~/.claude`.
Those trees usually contain unrelated JSON that is not session history.

Prefer:

- Codex: `~/.codex/sessions`, optionally `~/.codex/archived_sessions`
- Claude: `~/.claude/projects`

When you validate a new shared destination, start with smaller recent
subtrees first and widen them later.

## Service management

- Linux `systemd`: start from [`/packaging/systemd/oas.service`](../../packaging/systemd/oas.service)
- macOS `launchd`: start from [`/packaging/launchd/dev.open-agent-stream.oas.plist`](../../packaging/launchd/dev.open-agent-stream.oas.plist)
- other environments: run `oas daemon run` under your existing supervisor

## Validation before enabling continuous mode

On each machine:

```bash
oas version
oas validate -config /path/to/oas.json
oas doctor -config /path/to/oas.json
oas run -config /path/to/oas.json
oas delivery status -config /path/to/oas.json
```

For shared destinations, validate one machine first, confirm the payload shape, then bring up the others.

Also run those checks serially against the same config path. Parallel status,
doctor, and manual inspection commands against the same live SQLite files can
produce transient `SQLITE_BUSY` responses during local validation.

## Backup and restore

- back up both `state.db` and `ledger.db` together
- stop the service before taking a filesystem-level snapshot or file copy
- restore both files together onto the same machine identity
- if you intentionally migrate a machine to a new host, keep the same `machine_id`

Restoring only `state.db` or only `ledger.db` can break replay, sequence, and delivery accounting.

## Upgrade workflow

1. Stop one machine’s service.
2. Install the new verified binary.
3. Run `oas validate`, `oas doctor`, and `oas delivery status`.
4. Start the service again and confirm the shared destination sees the expected payload shape.
5. Repeat on the remaining machines.

This keeps rollback small and makes it obvious which host introduced a regression.

## First rollout advice

The first catch-up can be much larger than the steady-state stream, especially
on laptops with a long Codex or Claude history already present.

For the first rollout:

1. Start with a modest recent subtree and validate the remote payloads.
2. Increase `max_storage_bytes` to a value that fits the expected local ledger
   growth for that host.
3. Widen the source roots only after you are comfortable with the storage
   footprint and delivery behavior.

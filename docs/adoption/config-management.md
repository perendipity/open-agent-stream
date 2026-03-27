# Config Management For Real Deployments

This guide covers the practical config-management pattern for serious OAS use
across multiple machines and repositories.

## Recommended stance

Do not keep live OAS configs in the public `open-agent-stream` repo checkout.

Instead:

- keep live configs on each machine under `~/.config/open-agent-stream/` or
  `/etc/open-agent-stream/`
- commit the real config sources to a private ops repo or an existing
  infrastructure repo
- treat the example configs in this repo as shape references, not as the place
  where your live destination settings should live

## Why this is the recommended pattern

For serious use, configs usually contain environment-specific details even when
they do not contain secrets:

- bucket names and prefixes
- internal hostnames or API URLs
- local source roots
- machine IDs
- service-manager paths
- staging directories

Those details belong with your private operations material, not in the public
OAS source tree.

## What should vary per machine

Each machine should have its own values for:

- `machine_id`
- `data_dir` or explicit `state_path` and `ledger_path`
- source roots
- any machine-local credential environment

Each machine should keep the same values for the shared destination:

- sink `id`
- sink `type`
- sink `event_spec_version`
- sink `settings`
- sink `delivery`
- matching per-sink privacy overrides

## Current OAS config reality

OAS does not support config includes or overlays today.

That means your operational choices are:

- duplicate the shared sink stanza into one full config per machine
- or keep templates/snippets in a private repo and render the final machine
  config before deployment

Both are reasonable. The second approach scales better.

## Recommended repo layouts

### Option A: simple private repo

Use one complete config file per machine:

```text
oas-config-private/
  README.md
  machines/
    macbook-air.json
    studio.json
    linux-box.json
```

This is the simplest path and works well when you only have a few machines.

### Option B: template-plus-render

Keep one shared destination snippet, one shared privacy block, and one
machine-local values file per host:

```text
oas-config-private/
  shared/
    s3-destination.json
    privacy.json
  machines/
    macbook-air.values.json
    studio.values.json
    linux-box.values.json
  scripts/
    render-oas-config.sh
```

Render the final config onto each machine at a local path such as:

- `~/.config/open-agent-stream/oas.json`
- `/etc/open-agent-stream/oas.json`

This is the recommended long-term pattern for teams and serious personal use.

## Secrets

Do not commit secrets into OAS config JSON.

Use:

- secret references for HTTP sinks, such as
  `bearer_token_ref: "env://OAS_REMOTE_TOKEN"` or
  `bearer_token_ref: "op://vault/oas/collector-token"`
- `s3` auth in this order:
  - implicit AWS SDK default chain when `settings.auth` is absent
  - `settings.auth.mode: "profile"` with a named AWS profile
  - `settings.auth.mode: "secret_refs"` only when the environment cannot use
    the first two options
- machine environment or a secret manager for `command` and `external` sinks

When you use `file://` secret references, point them at machine-local files on
a local POSIX filesystem with restrictive permissions such as `0600`. Avoid
repo-relative secret files, shared drives, and cross-mounted paths such as WSL
`/mnt/c/...` or a symlinked `~/.aws` backed by that mount.

What is usually safe to commit in a private ops repo:

- bucket names
- regions
- prefixes
- key templates
- delivery timing and retry policy
- machine IDs
- source-root patterns
- AWS profile names
- secret references such as `env://...`, `op://...`, or `file://...`

What should stay out of committed config:

- bearer tokens
- AWS access keys
- AWS session tokens
- resolved secret values from any provider
- ad hoc smoke-test configs
- temporary local-only overrides

Stable secret-reference schemes in v1:

- `env://VAR_NAME`
- `file:///absolute/path`
- `op://vault/item/field`

Experimental secret-reference schemes in v1:

- `keychain://service/account`
- `pass://entry/path`

`file://` is intentionally constrained. Use it for externally managed secret
files or mounted ephemeral secrets, not as your default secret store. OAS
rejects world-readable and world-writable secret files, warns on group access
outside runtime secret directories, and warns when the secret file resolves
inside the current repo worktree.

## What to commit to this public repo

Commit only:

- examples
- templates
- documentation
- service template references

Do not commit:

- your personal live machine configs
- organization-specific destinations
- private infrastructure details that do not belong in an open-source example

## Recommended first serious workflow

1. Copy a template from [`/examples`](../../examples/README.md) into a private
   ops repo.
2. Render or copy the final config onto one machine at
   `~/.config/open-agent-stream/oas.json`.
3. Validate one machine first with `oas config print`, `oas validate`,
   `oas doctor`, and `oas run` or `oas daemon run`.
4. Confirm the remote payload shape.
5. Roll the same shared sink definition out to the remaining machines.

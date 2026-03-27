# Remote Destinations

This guide is the practical setup path for sending OAS batches off-machine.

The stock runtime includes these built-in remote-capable sinks:

- `http`: send neutral OAS batches to a proprietary API or generic HTTP endpoint
- `command`: stage a sealed payload file and invoke your own transport such as
  `rsync`, `scp`, or a wrapper script
- `s3`: upload sealed batch objects to a bucket with a deterministic object key
- `external`: hand the sealed batch to an out-of-process sink executable over a
  versioned stdio protocol
- `webhook`: compatibility alias to `http`

## Config model

Remote sinks use two config blocks:

- `settings`: destination-specific data such as URLs, headers, bucket names,
  key templates, argv arrays, and secret references for credentials
- `delivery`: batching and retry policy such as batch size, batch age, fixed
  release windows, backoff, max attempts, and poison-batch thresholds

The stock runtime persists per-sink prepared items after normalization and
redaction, then seals immutable dispatch batches before sending them. Retries
reuse the same sealed payload bytes and destination identity.

Practical implications:

- inject secrets through secret references such as `bearer_token_ref:
  "env://OAS_REMOTE_TOKEN"` rather than hard-coding them into the config
- use deterministic S3 keys so retries reuse the same object key
- expect `http`, `command`, and `webhook` replay to stay disabled by default
- expect `external` replay to stay disabled by default because the plugin may
  trigger side effects
- expect `s3` replay to stay disabled by default because it is append-only
- expect auth or secret-provider outages to block delivery and retry in place
  rather than quarantining good payloads

For serious use, keep the live config outside the `open-agent-stream` repo
checkout and manage it through a private ops repo or template-render workflow.
See [`config-management.md`](config-management.md).

## Choose the right sink

Use `http` when:

- you want to POST OAS-native batches to a service you control
- you need header-based auth or token injection from a secret reference
- you want near-real-time delivery with small batches

Use `command` when:

- you want to delegate transport to `rsync`, `scp`, a VPN-only wrapper, or your
  own binary
- you want OAS to prepare the payload, but you want full control over the final
  hop
- you need an escape hatch without adding a product-specific sink to the repo

Use `s3` when:

- you want append-only archival or downstream fan-out from a bucket
- you want sealed batch objects in `canonical_jsonl.gz`
- you can provide a deterministic key template

For shared destinations across multiple machines:

- set `event_spec_version: "v2"`
- give each machine its own local `machine_id`, `data_dir`, and source roots
- keep the sink stanza identical across machines

Use `external` when:

- you need a proprietary or unbundled destination without forking the stock CLI
- you want OAS to keep ownership of batching, retry, and quarantine
- your destination is better implemented as a standalone executable than as an HTTP wrapper

## HTTP example

```json
{
  "id": "remote-http",
  "type": "http",
  "settings": {
    "url": "https://collector.example.invalid/ingest",
    "method": "POST",
    "format": "oas_batch_json",
    "bearer_token_ref": "env://OAS_REMOTE_TOKEN",
    "headers": {
      "X-OAS-Source": "dev-laptop"
    }
  },
  "delivery": {
    "max_batch_events": 100,
    "max_batch_age": "5s",
    "retry_initial_backoff": "2s",
    "retry_max_backoff": "1m",
    "poison_after_failures": 5
  }
}
```

Useful `http` settings:

- `url`
- `method`
- `format`: `oas_batch_json` or `canonical_jsonl`
- `headers`
- `bearer_token_ref`
- `bearer_token_env`: legacy compatibility alias for `env://...`
- `timeout`
- `success_statuses`, `retry_on_statuses`, `permanent_on_statuses`

Supported secret-reference schemes for `http` and `webhook`:

- stable: `env://VAR_NAME`, `file:///absolute/path`, `op://vault/item/field`
- experimental: `keychain://service/account`, `pass://entry/path`

Use `file://` only for externally managed secret files or mounted ephemeral
secrets. OAS requires absolute paths, resolves symlinks before use, rejects
world-accessible files, warns on group-readable files outside runtime secret
directories, and warns when the secret file resolves inside the current repo
worktree.

## Command example

For a real remote copy path, use a transport binary you already trust:

```json
{
  "id": "remote-rsync",
  "type": "command",
  "settings": {
    "argv": [
      "rsync",
      "-az",
      "{payload_path}",
      "collector@example:/var/lib/oas/inbox/"
    ],
    "format": "canonical_jsonl.gz",
    "staging_dir": "/var/tmp/oas-command",
    "timeout": "30s",
    "max_output_bytes": 4096
  },
  "delivery": {
    "max_batch_events": 500,
    "max_batch_age": "30s",
    "window_every": "5m",
    "retry_initial_backoff": "10s",
    "retry_max_backoff": "5m",
    "poison_after_failures": 5
  }
}
```

Supported placeholders in `argv`:

- `{payload_path}`
- `{manifest_path}`
- `{sink_id}`
- `{batch_id}`
- `{ledger_min_offset}`
- `{ledger_max_offset}`

The command sink does not use a shell by default. OAS executes the argv array
directly.

## S3 example

```json
{
  "id": "archive-s3",
  "type": "s3",
  "settings": {
    "bucket": "oas-example-bucket",
    "region": "us-west-2",
    "prefix": "collector/",
    "key_template": "{prefix}{sink_id}/{batch_id}-{payload_sha256}.jsonl.gz",
    "storage_class": "STANDARD",
    "server_side_encryption": "AES256",
    "auth": {
      "mode": "profile",
      "profile": "oas-shared"
    }
  },
  "delivery": {
    "max_batch_events": 1000,
    "max_batch_age": "1m",
    "retry_initial_backoff": "5s",
    "retry_max_backoff": "2m",
    "poison_after_failures": 5
  }
}
```

`key_template` must include at least one uniqueness token such as:

- `{batch_id}`
- `{payload_sha256}`
- `{ledger_min_offset}`
- `{ledger_max_offset}`

That keeps retries on the same object key instead of creating accidental
duplicates.

For a shared bucket across several machines, prefer:

- `event_spec_version: "v2"` on the sink
- a bucket-level encryption policy
- a key template that includes both `{batch_id}` and `{payload_sha256}`

That combination preserves host identity in the payload and keeps retries
deterministic at the object-key level.

## S3 auth modes

If `settings.auth` is absent, OAS preserves the current AWS SDK behavior and
uses the default credential chain.

If `settings.auth` is present, `mode` is required and OAS does not fall back
implicitly. Supported modes are:

- `default_chain`: explicit opt-in to the AWS SDK default chain
- `profile`: use a named AWS profile, with optional `credentials_file_ref` and
  `config_file_ref`
- `secret_refs`: resolve `access_key_id_ref`, `secret_access_key_ref`, and
  optional `session_token_ref`

Recommended preference order for `s3`:

1. implicit default chain or `settings.auth.mode: "default_chain"`
2. `settings.auth.mode: "profile"`
3. `settings.auth.mode: "secret_refs"` only when the environment cannot use
   the first two options

Example `secret_refs` block:

```json
{
  "auth": {
    "mode": "secret_refs",
    "access_key_id_ref": "env://OAS_S3_ACCESS_KEY_ID",
    "secret_access_key_ref": "env://OAS_S3_SECRET_ACCESS_KEY",
    "session_token_ref": "env://OAS_S3_SESSION_TOKEN"
  }
}
```

If you are moving an existing machine off implicit `aws login` or other default
chain behavior, the shortest migration is to pin the sink to a named profile:

```json
{
  "auth": {
    "mode": "profile",
    "profile": "oas-shared-archive-s3"
  }
}
```

That keeps OAS off the ambient default profile and makes the credential source
explicit without putting keys into OAS JSON.

For Linux or other service-managed installs, prefer pinning the AWS shared
config files too instead of relying on ambient `AWS_PROFILE` or `HOME`:

```json
{
  "auth": {
    "mode": "profile",
    "profile": "oas-shared-archive-s3",
    "credentials_file_ref": "file:///home/USER/.aws/credentials",
    "config_file_ref": "file:///home/USER/.aws/config"
  }
}
```

That keeps `oas doctor`, `oas daemon run`, and service-manager launches on the
same named profile even when the process environment is minimal or the working
directory changes. Run OAS as the same user that owns those AWS files.

For a new machine-local OAS profile, a dedicated directory is often less
fragile than reusing ambient `~/.aws`:

```json
{
  "auth": {
    "mode": "profile",
    "profile": "oas-shared-archive-s3-linux-box",
    "credentials_file_ref": "file:///home/USER/.config/open-agent-stream/aws/credentials",
    "config_file_ref": "file:///home/USER/.config/open-agent-stream/aws/config"
  }
}
```

Create those files with mode `0600` on a local POSIX filesystem.

On WSL or any environment where `~/.aws` points into `/mnt/c/...` or another
cross-mounted location, do not point file refs at that path. OAS validates
file-backed secrets and may report `secret is missing` or `file-backed secret
has insecure permissions` when the referenced files do not exist locally or do
not satisfy the file-provider permission checks.

OAS treats expired AWS sessions, locked providers, and missing secret-manager
sessions as blocked delivery conditions. Those failures stay in retry/backoff
instead of poisoning batches.

## Provider capability matrix

| Provider | Platforms | Live session required | Common failure mode | Recommended for production |
| --- | --- | --- | --- | --- |
| `env://` | macOS, Linux | No | missing variable | Yes |
| `file://` | macOS, Linux | No | missing file or insecure permissions | Limited |
| `op://` | macOS, Linux | Usually | expired 1Password CLI sign-in or session | Yes |
| `keychain://` | macOS | Sometimes | locked or unauthorized keychain access | Experimental |
| `pass://` | Linux | Often | `gpg-agent` or password-store issues | Experimental |

Practical operator patterns:

- macOS: AWS profiles, 1Password CLI, launchd environment injection, optional
  experimental Keychain refs
- Linux: AWS profiles, 1Password CLI, systemd environment or credential
  injection, optional experimental `pass` refs

## Choose source roots carefully

OAS walks JSON and JSONL files recursively. For real local use, do not point
the built-in adapters at top-level agent home directories such as `~/.codex`
or `~/.claude`, because those trees usually contain unrelated settings, cache,
and todo JSON.

Prefer session trees:

- Codex: `~/.codex/sessions`, optionally `~/.codex/archived_sessions`
- Claude: `~/.claude/projects`

If you are validating a new remote destination, start even smaller:

- one recent Codex day directory such as `~/.codex/sessions/2026/03/25`
- one active Claude project directory under `~/.claude/projects/...`

Then widen the source roots only after you confirm the remote payload shape and
local storage budget.

## Shared S3 rollout

A practical first rollout to S3 should look like this:

1. Create a dedicated private bucket or private prefix.
2. Enable Block Public Access and default bucket encryption.
3. Validate one machine first with a small recent source subtree.
4. Confirm one uploaded object has:
   - `ContentEncoding: gzip`
   - `ContentType: application/x-ndjson`
   - `ServerSideEncryption` set as expected
5. Download one object and confirm the decompressed JSONL lines include
   `machine_id` in `event_spec_version: "v2"` payloads.
6. After the first machine is healthy, bring up the remaining machines.

Be aware that the first historical catch-up can be large. If you point OAS at
years of existing local sessions on the first run, normalization and local
ledger growth may take a while before delivery fully catches up.

If you want one dedicated machine credential per host, start with a
bucket-and-prefix-scoped policy like:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BucketHealth",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::YOUR-BUCKET"
    },
    {
      "Sid": "WriteArchiveObjects",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::YOUR-BUCKET/events/v2/shared-archive-s3/*"
    }
  ]
}
```

Adjust the object resource to match your real `key_template` prefix.

## External sink example

```json
{
  "id": "vendor-plugin",
  "type": "external",
  "event_spec_version": "v2",
  "settings": {
    "plugin_type": "vendor-api",
    "argv": ["/usr/local/bin/oas-vendor-plugin"],
    "format": "canonical_jsonl.gz",
    "timeout": "30s",
    "max_output_bytes": 4096
  },
  "delivery": {
    "max_batch_events": 100,
    "max_batch_age": "5s",
    "retry_initial_backoff": "5s",
    "retry_max_backoff": "1m",
    "poison_after_failures": 5
  }
}
```

The external executable receives `handshake`, `health`, and `send` requests over
stdin/stdout. See [`spec/sink-runtime/v1`](../../spec/sink-runtime/v1/README.md).

## Local validation before a real destination

Validate the sink shape first:

```bash
oas config print -config ./oas.json
oas validate -config ./oas.json
```

If you are validating a local checkout in daemon mode, prefer the exact binary
you built rather than `go run`:

```bash
go build -o ./bin/oas ./cmd/oas
./bin/oas version
```

For a safe local HTTP smoke test, start a loopback receiver in another shell:

```bash
python3 -u - <<'PY'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n)
        print("POST", self.path, "bytes=", len(body), flush=True)
        self.send_response(204)
        self.end_headers()
    def log_message(self, format, *args):
        pass
HTTPServer(("127.0.0.1", 8088), H).serve_forever()
PY
```

Then point an `http` sink at `http://127.0.0.1:8088/ingest` and run:

```bash
./bin/oas daemon start -config ./oas.json
./bin/oas daemon status -config ./oas.json -json
./bin/oas doctor -config ./oas.json
./bin/oas daemon stop -config ./oas.json
```

Run those checks serially against one config. Hammering the same SQLite state
files from multiple commands at once can produce transient `SQLITE_BUSY`
responses during local validation.

For a local command-sink smoke test on Unix-like systems, replace your transport
argv temporarily with a simple copy:

```json
{
  "argv": [
    "/bin/cp",
    "{payload_path}",
    "/tmp/oas-command/latest.payload"
  ]
}
```

For a real S3 smoke test, do the same thing conceptually: validate with a
small recent source subtree first, let `oas run` or `oas daemon run` create a
few batches, then inspect the uploaded objects before you widen the roots or
turn on continuous mode.

That lets you confirm batch sealing and file staging before you swap back to
`rsync` or another real transport.

## What to look for in status output

`oas daemon status -json` and `oas doctor` should show:

- `queue_depth=0` after delivery catches up
- `retrying_batch_count=0` during a healthy smoke test
- `blocked_batch_count=0` during healthy operation
- `quarantined_batch_count=0` unless you are intentionally testing poison-batch behavior
- `blocked_kind`, `last_blocked_error`, and `last_auth_error` only when a sink
  is blocked on auth or config
- `acked_contiguous_offset` moving forward as the destination acknowledges sends
- `terminal_contiguous_offset` moving forward even if a batch is terminally
  quarantined and retained locally for inspection

`oas doctor` now reports both static auth checks and best-effort live checks.
Static checks validate secret-ref syntax, provider binaries, path permissions,
and auth-mode completeness. Live checks still verify that the sink can resolve
credentials and reach the destination right now.

## Understanding acked versus terminal progress

The two delivery watermarks mean different things:

- `acked_contiguous_offset`: the highest contiguous ledger offset that the sink
  has successfully acknowledged to the destination
- `terminal_contiguous_offset`: the highest contiguous ledger offset that is no
  longer blocking local retention, including successful sends, empty-after-redaction
  skips, and terminally quarantined batches

If both numbers move together, delivery is healthy and retention can prune
normally.

If `terminal_contiguous_offset` is ahead of `acked_contiguous_offset`, OAS has
quarantined one or more batches after a permanent failure or poison-batch
threshold. That prevents local retention from stalling forever, but it also
means the destination never acknowledged some ledger range.

Use `oas delivery list`, `oas delivery inspect`, and `oas delivery retry` when
you need to inspect or requeue those quarantined batches intentionally.

`gaps` tells you how many ledger ranges are currently missing from the success
watermark. In a steady healthy state, `gaps` should be `0`.

`oas doctor` also surfaces sink-specific readiness hints such as the configured
HTTP URL or the command executable path.

## Replay and operator intent

Replay is intentionally conservative:

- `http`, `command`, and `webhook` are side-effecting and skipped by default
- `s3` is append-only and skipped by default
- `sqlite` is replay-safe by default

Use `oas replay -config ./oas.json -dry-run` to inspect the sink selection plan
before you explicitly include non-idempotent sinks.

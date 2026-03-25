# Remote Destinations

This guide is the practical setup path for sending OAS batches off-machine.

The stock runtime includes these built-in remote-capable sinks:

- `http`: send neutral OAS batches to a proprietary API or generic HTTP endpoint
- `command`: stage a sealed payload file and invoke your own transport such as
  `rsync`, `scp`, or a wrapper script
- `s3`: upload sealed batch objects to a bucket with a deterministic object key
- `webhook`: compatibility alias to `http`

## Config model

Remote sinks use two config blocks:

- `settings`: destination-specific data such as URLs, headers, bucket names,
  key templates, argv arrays, and env-var references for credentials
- `delivery`: batching and retry policy such as batch size, batch age, fixed
  release windows, backoff, max attempts, and poison-batch thresholds

The stock runtime persists per-sink prepared items after normalization and
redaction, then seals immutable dispatch batches before sending them. Retries
reuse the same sealed payload bytes and destination identity.

Practical implications:

- inject secrets through env-var references such as `bearer_token_env` rather
  than hard-coding them into the config
- use deterministic S3 keys so retries reuse the same object key
- expect `http`, `command`, and `webhook` replay to stay disabled by default
- expect `s3` replay to stay disabled by default because it is append-only

## Choose the right sink

Use `http` when:

- you want to POST OAS-native batches to a service you control
- you need header-based auth or token injection from an environment variable
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

## HTTP example

```json
{
  "id": "remote-http",
  "type": "http",
  "settings": {
    "url": "https://collector.example.invalid/ingest",
    "method": "POST",
    "format": "oas_batch_json",
    "bearer_token_env": "OAS_REMOTE_TOKEN",
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
- `bearer_token_env`
- `timeout`
- `success_statuses`, `retry_on_statuses`, `permanent_on_statuses`

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
    "key_template": "{prefix}{sink_id}/{batch_id}.jsonl.gz",
    "storage_class": "STANDARD",
    "server_side_encryption": "AES256"
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

That lets you confirm batch sealing and file staging before you swap back to
`rsync` or another real transport.

## What to look for in status output

`oas daemon status -json` and `oas doctor` should show:

- `queue_depth=0` after delivery catches up
- `retrying_batch_count=0` during a healthy smoke test
- `quarantined_batch_count=0` unless you are intentionally testing poison-batch behavior
- `acked_contiguous_offset` moving forward as the destination acknowledges sends
- `terminal_contiguous_offset` moving forward even if a batch is terminally
  quarantined and retained locally for inspection

`oas doctor` also surfaces sink-specific readiness hints such as the configured
HTTP URL or the command executable path.

## Replay and operator intent

Replay is intentionally conservative:

- `http`, `command`, and `webhook` are side-effecting and skipped by default
- `s3` is append-only and skipped by default
- `sqlite` is replay-safe by default

Use `oas replay -config ./oas.json -dry-run` to inspect the sink selection plan
before you explicitly include non-idempotent sinks.

# External Sink Runtime Protocol v1

The stock OAS runtime can invoke out-of-process sinks with `type: "external"`.

Protocol transport:

- request: one JSON object on stdin
- response: one JSON object on stdout
- stderr: diagnostic text for the operator; ignored on success and surfaced on failure

The runtime owns:

- ingest
- normalization
- redaction
- prepared-item persistence
- sealed-batch creation
- retry scheduling
- quarantine

The external sink process owns only destination-specific delivery behavior.

## Request

```json
{
  "protocol_version": "v1",
  "action": "handshake | health | send",
  "plugin_type": "vendor-name",
  "sink_config": {
    "id": "remote-vendor",
    "type": "external",
    "event_spec_version": "v2"
  },
  "prepared": {
    "sink_id": "remote-vendor",
    "batch_id": "batch_123",
    "payload": "<base64-bytes>",
    "payload_sha256": "sha256:...",
    "content_type": "application/x-ndjson",
    "headers": {
      "Content-Type": "application/x-ndjson"
    },
    "payload_format": "canonical_jsonl.gz",
    "ledger_min_offset": 10,
    "ledger_max_offset": 12,
    "event_count": 3,
    "created_at": "2026-03-25T22:00:00Z",
    "destination": {
      "plugin_type": "vendor-name"
    }
  }
}
```

`prepared` is present only for `action: "send"`.

## Response

```json
{
  "protocol_version": "v1",
  "status": "ok | retry | permanent",
  "message": "optional operator-facing detail",
  "plugin_type": "vendor-name",
  "supported_actions": ["handshake", "health", "send"]
}
```

## Runtime expectations

- `handshake` must return `status: "ok"` and the same protocol version
- `health` should be non-mutating
- `send` is batch-atomic
- `retry` means OAS should keep the sealed batch and retry it later
- `permanent` means OAS should quarantine the sealed batch

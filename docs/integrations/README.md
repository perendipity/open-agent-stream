# Third-Party Adapters and Sinks

This guide is the fastest way to understand how to extend OAS without depending
on `/internal`.

## Choose your path first

Today there are three realistic ways to extend OAS:

1. **Use the built-in types as-is** if the existing adapters and sinks already
   cover your workflow.
2. **Upstream a new adapter or sink here** if you want support in the stock
   `oas` CLI for everyone.
3. **Ship an external sink executable** if you want a proprietary or unbundled
   destination without forking the stock CLI.
4. **Maintain a custom CLI overlay** if you need a custom source adapter or a
   deeper runtime change than the external sink protocol supports.

The important distinction is that the **contracts are ready before the runtime
plugin boundary is finalized**.

## What is stable today

Build against these published surfaces:

- [`spec/source-adapter/v1`](../../spec/source-adapter/v1/README.md)
- [`spec/sink/v1`](../../spec/sink/v1/README.md)
- [`spec/raw-envelope/v1`](../../spec/raw-envelope/v1/README.md)
- [`spec/canonical-event/v1`](../../spec/canonical-event/v1/README.md)
- [`spec/canonical-event/v2/schema.json`](../../spec/canonical-event/v2/schema.json)
- [`spec/session-model/v1`](../../spec/session-model/v1/schema.json)
- [`spec/session-model/v2/schema.json`](../../spec/session-model/v2/schema.json)
- [`spec/privacy-policy/v1`](../../spec/privacy-policy/v1/README.md)
- [`spec/sink-runtime/v1`](../../spec/sink-runtime/v1/README.md)
- [`pkg/sourceapi`](../../pkg/sourceapi/sourceapi.go)
- [`pkg/externalapi`](../../pkg/externalapi/externalapi.go)
- [`pkg/sinkapi`](../../pkg/sinkapi/sinkapi.go)
- [`pkg/schema`](../../pkg/schema/schema.go)

Those contracts, plus fixtures and conformance expectations, are the intended
third-party extension surface.

## Decide what you are building

### Source adapter

Choose an adapter when you need to:

- discover source-native local artifacts
- read them incrementally with checkpoints
- preserve source-native payloads inside `RawEnvelope`
- provide parse hints without hard-coding sink semantics

Adapters should stay local-first and side-effect-free.

### Sink

Choose a sink when you need to:

- receive canonical events and optional raw envelopes
- control delivery semantics for storage or forwarding
- document replay safety clearly for operators

Sinks own delivery behavior. They should declare whether replay is
`idempotent`, `append_only`, or `side_effecting`.

## Minimal adapter shape

```go
package acmesource

import (
    "context"

    "github.com/open-agent-stream/open-agent-stream/pkg/schema"
    "github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

type Adapter struct{}

func (a *Adapter) Type() string { return "acme_local" }

func (a *Adapter) Capabilities() []sourceapi.Capability {
    return []sourceapi.Capability{sourceapi.CapabilityMessages}
}

func (a *Adapter) Discover(ctx context.Context, cfg sourceapi.Config) ([]sourceapi.Artifact, error) {
    // discover artifacts under cfg.Root and return stable artifact IDs
    return nil, nil
}

func (a *Adapter) Read(ctx context.Context, cfg sourceapi.Config, artifact sourceapi.Artifact, checkpoint sourceapi.Checkpoint) ([]schema.RawEnvelope, sourceapi.Checkpoint, error) {
    // emit source-native envelopes plus the next checkpoint
    return nil, sourceapi.Checkpoint{}, nil
}
```

An adapter should populate `RawEnvelope` fields that let the runtime preserve
fidelity and derive stable session identity later:

- `source_type`
- `source_instance_id`
- `artifact_id`
- `cursor`
- `observed_at`
- `raw_kind`
- `raw_payload`
- `content_hash`
- `parse_hints` when you can infer session, project, timestamp, or capabilities

## Minimal sink shape

```go
package acmesink

import (
    "context"
    "time"

    "github.com/open-agent-stream/open-agent-stream/pkg/schema"
    "github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
    cfg sinkapi.Config
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }
func (s *Sink) Init(context.Context) error { return nil }

func (s *Sink) SendBatch(context.Context, sinkapi.Batch) (sinkapi.Result, error) {
    return sinkapi.Result{
        Checkpoint: schema.SinkCheckpoint{
            SinkID:  s.cfg.ID,
            AckedAt: time.Now().UTC(),
        },
    }, nil
}

func (s *Sink) Flush(context.Context) error  { return nil }
func (s *Sink) Health(context.Context) error { return nil }
func (s *Sink) Close(context.Context) error  { return nil }
```

A sink should document:

- required `settings` keys and any legacy `options` compatibility
- supported `delivery` controls when the sink participates in the stock
  runtime's built-in delivery manager
- whether it consumes only canonical events or also raw envelopes
- replay class and duplicate-delivery expectations
- privacy/redaction assumptions before data leaves the machine

For the stock built-in runtime, operators should expect sink config to split
into:

- `settings` for destination-specific data
- `delivery` for batching, fixed-window release, retry backoff, and poison-batch handling

The public `sinkapi.Sink` contract remains intentionally small. Upstreamed
built-in remote sinks and external sink executables should freeze payload bytes
and destination identity before retrying.

## External sink runtime

If your destination should not be bundled into the stock repo, use the
versioned external sink runtime:

- config uses `type: "external"`
- OAS seals the batch locally, then invokes your executable over stdio
- the executable receives a versioned request and returns `ok`, `retry`, or `permanent`

Read [`spec/sink-runtime/v1`](../../spec/sink-runtime/v1/README.md) and use
[`examples/config.external.example.json`](../../examples/config.external.example.json) as the config shape reference.

## Compatibility checklist

A credible adapter or sink should ship with:

1. a declared contract target (`source-adapter/v1` or `sink/v1`)
2. a stable type string and documented config options
3. representative fixtures for the source or sink behavior you care about
4. executable coverage in `conformance/` or equivalent Go tests
5. operator-facing docs for replay, failure, and privacy behavior
6. migration notes if you need a new spec version

See [`CONTRIBUTING.md`](../../CONTRIBUTING.md) and the
[`compatibility matrix`](../governance/compatibility-matrix.md) for the current
reference bar.

## Current integration reality

The stock `oas` CLI now supports out-of-process external sinks, but it still
does **not** dynamically discover external source adapters. In practice:

- upstream an adapter here if you want it available in the stock `oas` CLI
- use `type: "external"` for proprietary or unbundled destinations
- maintain a custom CLI overlay only when you need a custom source adapter or a
  runtime change outside the published sink/runtime contracts

See [RFC 0002](../../rfcs/0002-external-plugin-runtime.md) for the current
reasoning behind that boundary.

## Recommended starting order

1. Read the relevant spec contract.
2. Check the [compatibility matrix](../governance/compatibility-matrix.md).
3. Copy the smallest built-in implementation that is structurally similar.
4. Add fixtures before broadening heuristics.
5. Add executable compatibility coverage before asking others to depend on it.

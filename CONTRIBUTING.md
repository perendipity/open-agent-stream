# Contributing

`open-agent-stream` treats the standard as a first-class deliverable.

## Change rules

- Any change to `/spec` must include matching fixture updates in `/fixtures`.
- Any change to `/spec` must include executable coverage in `/conformance` or Go tests that exercise the same contract.
- Any normative change must include a migration note, either in the affected spec version or an RFC.
- Breaking changes require a new versioned spec directory.
- Any CLI-facing change must meet the bar in [`docs/governance/cli-standards.md`](docs/governance/cli-standards.md).
- CLI-facing changes should update help/examples/docs in the same PR and add regression coverage for critical output when practical.

## Workflow

1. Open an RFC for normative changes.
2. Keep public contracts in `/pkg`.
3. Keep runtime-only machinery in `/internal`.
4. Prefer adding fixtures before broadening heuristics in adapters or normalizers.

## Testing

Run the same required checks GitHub requires before merge:

```bash
scripts/ci.sh
```

`scripts/ci.sh` runs the same commands as the required `build` and `cli-smoke` jobs. Its `go build -v ./...` and `go test -v ./...` step goes through `scripts/ci_go.sh`, which intentionally clears ambient `AWS_*` auth so local runs do not accidentally pass because of developer machine credentials.

If you only need the installed CLI smoke flow:

```bash
scripts/smoke_cli.sh
```

Validate local fixture integrity:

```bash
go run ./cmd/oas validate -config ./examples/config.example.json
```

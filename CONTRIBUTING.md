# Contributing

`open-agent-stream` treats the standard as a first-class deliverable.

## Change rules

- Any change to `/spec` must include matching fixture updates in `/fixtures`.
- Any change to `/spec` must include executable coverage in `/conformance` or Go tests that exercise the same contract.
- Any normative change must include a migration note, either in the affected spec version or an RFC.
- Breaking changes require a new versioned spec directory.

## Workflow

1. Open an RFC for normative changes.
2. Keep public contracts in `/pkg`.
3. Keep runtime-only machinery in `/internal`.
4. Prefer adding fixtures before broadening heuristics in adapters or normalizers.

## Testing

Run:

```bash
go test ./...
```

Validate local fixture integrity:

```bash
go run ./cmd/oas validate
```


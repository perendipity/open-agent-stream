# Release Process

Each release should include:

- implementation changes
- updated compatibility notes
- spec diffs, if any
- fixture and conformance changes

Normative changes should cite the RFC that authorized them.

## Release checklist

1. Run `go test ./...`.
2. Run `scripts/smoke_cli.sh`.
3. Run `scripts/release_smoke.sh` to exercise the built binary plus a local remote sink path.
4. Update `docs/governance/compatibility-matrix.md` if spec or replay semantics changed.
5. Update install and operator docs if service, delivery, or destination behavior changed.
6. Build release archives for the supported target matrix:
   - Linux `amd64`, `arm64`
   - macOS `amd64`, `arm64`
   - Windows `amd64`, `arm64`
7. Publish `SHA256SUMS` alongside the archives.
8. Verify that the service templates in `/packaging` still match the supported foreground daemon contract.

## What a release artifact must contain

- the `oas` binary
- `README.md`
- `SHA256SUMS` for all published archives

Signed binaries are not yet required, but checksums are part of the baseline release contract.

# Install and Verify OAS

OAS now supports two install paths:

- released binaries with SHA256 checksums for common macOS, Linux, and Windows targets
- `go install` from source for contributors and local checkout workflows

Use released binaries when you want reproducible installs across multiple machines.

## Install from a release archive

1. Download the archive for your OS/architecture from the latest GitHub release.
2. Download the matching `SHA256SUMS`.
3. Verify the checksum before placing the binary on your `PATH`.

Example on Linux or macOS:

```bash
curl -LO https://github.com/open-agent-stream/open-agent-stream/releases/download/vX.Y.Z/oas_vX.Y.Z_linux_amd64.tar.gz
curl -LO https://github.com/open-agent-stream/open-agent-stream/releases/download/vX.Y.Z/SHA256SUMS
grep 'oas_vX.Y.Z_linux_amd64.tar.gz' SHA256SUMS | shasum -a 256 -c -
tar -xzf oas_vX.Y.Z_linux_amd64.tar.gz
sudo install -m 0755 oas_vX.Y.Z_linux_amd64/oas /usr/local/bin/oas
```

On Windows, download the `.zip` archive and verify its SHA256 hash with `certutil`.

## Install from the latest published module version

```bash
go install github.com/open-agent-stream/open-agent-stream/cmd/oas@latest
```

If you are testing a local checkout instead, run:

```bash
go install ./cmd/oas
```

## Verify the binary you installed

```bash
oas version
oas --help
```

`oas version` should print the installed CLI version when module metadata is
available, or `oas dev` for local development builds.

If you want to use the fixture-backed demo flow from the repo README, do that
from a local checkout so the generated starter config can point at
`./fixtures/...`. If you are evaluating OAS against real local artifacts
instead, edit the generated source roots to match those directories.

Also note that `oas validate` is repo-checkout-aware because it validates the
bundled fixtures as well as the config. If you run it outside an
`open-agent-stream` checkout, pass `-root /path/to/open-agent-stream` or skip
that step.

## Run OAS as a service

- Linux `systemd`: use [`/packaging/systemd/oas.service`](../../packaging/systemd/oas.service)
- macOS `launchd`: use [`/packaging/launchd/dev.open-agent-stream.oas.plist`](../../packaging/launchd/dev.open-agent-stream.oas.plist)
- Other supervisors: run `oas daemon run -config /path/to/oas.json` in the foreground under your existing process manager

The service templates are intentionally conservative. Update the config path, log path, working directory, and binary path before installation.

## Upgrades and rollback

1. Stop the service cleanly.
2. Replace the `oas` binary with the new verified release.
3. Run `oas version`, `oas validate`, and `oas doctor` against the installed config.
4. Start the service again and confirm `oas delivery status` and `oas daemon status`.

To roll back, repeat the same steps with the previous verified binary.

## What to expect from distribution right now

- release artifacts ship as archives plus `SHA256SUMS`
- there is not yet a Homebrew formula or apt package
- code signing is not yet part of the release contract

## Common setup issues

If `oas` is not found after installation:

1. confirm your Go version matches the repo's `go.mod`
2. make sure `$(go env GOBIN)` is on your `PATH`, or if `GOBIN` is empty,
   ensure `$(go env GOPATH)/bin` is on your `PATH`
3. re-run `oas version` after opening a new shell

If a released binary fails health checks after installation, run:

```bash
oas validate -config /path/to/oas.json
oas doctor -config /path/to/oas.json
oas delivery status -config /path/to/oas.json
```

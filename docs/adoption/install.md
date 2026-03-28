# Install and Verify OAS

OAS now supports two install paths:

- released binaries with SHA256 checksums for common macOS, Linux, and Windows targets
- `go install` from source for contributors and local checkout workflows

Use released binaries when you want reproducible installs across one or more
machines and operating systems.

## Where live configs should live

For real use, keep the live OAS config outside the `open-agent-stream` repo
checkout.

Recommended locations:

- per-user: `~/.config/open-agent-stream/oas.json`
- system-wide: `/etc/open-agent-stream/oas.json`

For multi-machine teams, commit the real configs to a private ops repo or an
existing infrastructure repo. Do not commit personal or environment-specific
live configs back into the public OAS repo checkout.

See [`config-management.md`](config-management.md) for the recommended sharing
pattern.

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

For built-in sources, prefer session trees rather than top-level agent home
directories:

- Codex: `~/.codex/sessions`, optionally `~/.codex/archived_sessions`
- Claude: `~/.claude/projects`

Also note that `oas validate` is repo-checkout-aware because it validates the
bundled fixtures as well as the config. If you run it outside an
`open-agent-stream` checkout, pass `-root /path/to/open-agent-stream` or skip
that step.

## Run OAS continuously

The simplest per-user continuous path is:

```bash
oas daemon start -config /path/to/oas.json
oas daemon status -config /path/to/oas.json
```

Use an OS service manager only when you need restart-on-boot, login/session
integration, or supervisor-managed restart policy:

- Linux `systemd`: use [`/packaging/systemd/oas.service`](../../packaging/systemd/oas.service), which runs `oas daemon run`
- macOS `launchd`: use [`/packaging/launchd/dev.open-agent-stream.oas.plist`](../../packaging/launchd/dev.open-agent-stream.oas.plist), which runs `oas daemon run`
- Other supervisors: run `oas daemon run -config /path/to/oas.json` in the foreground under your existing process manager

The service templates are intentionally conservative. Update the config path, log path, working directory, and binary path before installation.

If an `s3` sink uses `settings.auth.mode: "profile"`, prefer pinning
`credentials_file_ref` and `config_file_ref` in the sink config rather than
depending on ambient `AWS_PROFILE`. This is the most reliable path for Linux
services and other minimal process-manager environments. Run the service as the
same user that owns those AWS config files.

For a first-time machine setup, a dedicated local AWS shared-config directory
is often cleaner than reusing ambient `~/.aws`, for example:

- `~/.config/open-agent-stream/aws/credentials`
- `~/.config/open-agent-stream/aws/config`

Keep those files on a local POSIX filesystem with restrictive permissions such
as `0600`, then point `credentials_file_ref` and `config_file_ref` at them.
Do not point file refs at Windows-mounted, network-mounted, or symlinked AWS
files such as WSL `~/.aws -> /mnt/c/...`; OAS may reject those as missing or
insecure during static auth checks.

When you install OAS across multiple machines, keep the service file or plist
pointed at a local config path such as `~/.config/open-agent-stream/oas.json`
or `/etc/open-agent-stream/oas.json`. Share the config content through a
private repo or render step, not through edits inside the public source tree.
Each machine keeps its own local ledger and state; shared sinks are optional
convergence points, not shared machine state.

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

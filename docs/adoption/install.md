# Install and Verify OAS

For early adopters today, the canonical distribution path is a Go install from
source.

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

## What to expect from distribution right now

- there is not yet a Homebrew formula, apt package, or signed release archive
- `go install ...@latest` is the intended install path for the first wave of
  adopters
- upgrading is currently just re-running the install command

## Common setup issues

If `oas` is not found after installation:

1. confirm your Go version matches the repo's `go.mod`
2. make sure `$(go env GOBIN)` is on your `PATH`, or if `GOBIN` is empty,
   ensure `$(go env GOPATH)/bin` is on your `PATH`
3. re-run `oas version` after opening a new shell

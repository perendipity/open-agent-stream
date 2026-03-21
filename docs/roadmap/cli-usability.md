# CLI Usability Roadmap

This roadmap defines how `oas` should move from a functional CLI to an
excellent Unix-style interface with strong progressive disclosure.

## Current gap

Today the CLI has the core commands, but it still leans too heavily on the
README:

- top-level help is only a usage line
- subcommand help is minimal
- config remains mostly JSON-driven with limited CLI inspection
- defaults and advanced behaviors are not surfaced well enough in the command
  interface itself

## Target state

Users should be able to:

- discover the command surface from `oas --help`
- learn each command from `oas <cmd> --help`
- inspect the effective config without reading source
- understand defaults, paths, and failure modes from the CLI
- use terse output for daily work and structured output for automation

## Implementation phases

### Phase 1: Fix help quality

- improve `oas --help` with command summaries
- improve `oas daemon --help` and all other subcommand help
- add examples to each command
- distinguish common vs advanced flags

### Phase 2: Make config inspectable

- add `oas config init`
- add `oas config print`
- add `oas config validate`
- show effective defaults after config loading
- surface resolved paths for state, ledger, and daemon files

### Phase 3: Improve operational visibility

- add optional JSON output for status/doctor-style commands
- make daemon status more structured and script-friendly
- expose storage-guard status, current usage, and prune activity

### Phase 4: Lock in standards

- require new CLI-facing features to update help/examples/docs
- keep the CLI standards doc as a merge bar
- add tests or snapshots for critical help output where useful

## Recommended sequencing

Recommended next sequence:

1. `help` improvements for existing commands
2. `oas config print`
3. `oas config init`
4. `oas config validate` refinement and better error messages
5. structured status output and storage visibility

## Acceptance bar

We should consider this roadmap materially complete when:

- a new user can learn the command surface from `--help`
- a user can inspect effective config without source diving
- advanced features remain discoverable through deeper help
- new CLI-facing features consistently ship with help, examples, and config
  visibility

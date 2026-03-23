# CLI Standards

The `oas` CLI should meet a high bar for Unix-style usability:

- obvious defaults
- composable commands
- stable text output where practical
- progressive disclosure through `--help`
- config-file-first ergonomics without making users read the source

## Core standards

### 1. Every command must be discoverable from the CLI

The top-level `oas --help` output should:

- list the major commands
- explain each command in one short phrase
- point users toward deeper help

Every subcommand should support `--help` and provide:

- what the command does
- the important flags
- the default behavior
- one or more concrete examples

### 2. Progressive disclosure is required

Default help should stay concise, but advanced behavior must still be reachable
without leaving the CLI.

That means:

- basic usage first
- advanced flags grouped clearly
- examples that move from common to advanced cases
- config-related defaults described in help text or adjacent inspect commands

### 3. Config must be inspectable, not just editable

Config-file-first is acceptable and Unix-friendly, but users should not need to
reverse-engineer JSON fields from code or README prose.

The CLI should make it easy to:

- initialize a starting config
- print the effective config after defaults
- validate a config with precise errors
- understand which paths and defaults will be used

### 4. Output should be shaped for both humans and automation

Human-facing commands should prefer:

- terse default output
- stable field labels
- readable summaries

Machine-facing output should prefer:

- explicit JSON modes where appropriate
- predictable field names
- compatibility-conscious structure

### 5. New features must come with CLI affordances

If a new feature adds meaningful user-facing behavior, it should also add:

- discoverable help text
- examples
- validation or inspection support when relevant
- documentation of defaults and failure modes

Adding a config field without making it discoverable through the CLI should be
treated as incomplete.

## Bar for future additions

This document is the **merge bar** for CLI-facing work. A PR that changes the
user-facing `oas` surface should be treated as incomplete until the items below
are handled in the same change.

Before merging a new CLI-facing feature, reviewers should ask:

1. Can a new user discover this from `oas --help` or subcommand help?
2. Can a user understand the defaults without reading code?
3. Can a user validate or inspect the effective behavior?
4. Does the output support both normal human use and scripted use?
5. Are examples included for the common path?

If the answer to several of these is no, the feature is not yet at the desired
CLI quality bar.

## Minimum checklist for CLI-facing PRs

If a PR changes command behavior, flags, defaults, or operational output, it
should also include:

- updated help text for the affected command(s)
- one or more examples for the common path
- docs updates when the behavior is user-meaningful
- validation/inspection affordances when new config or runtime behavior is added
- regression coverage for critical output where practical (for example help
  snapshots or focused output tests)

Reviewers should push back on CLI-facing changes that add capability without
also making that capability discoverable and testable.

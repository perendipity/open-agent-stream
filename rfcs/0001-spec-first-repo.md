# RFC 0001: Spec-First Repository Structure

- Status: Accepted
- Authors: open-agent-stream maintainers
- Spec version impact: None
- Implementation impact: Foundational

## Summary

Keep the normative spec, fixtures, conformance suite, and reference implementation in one repository.

## Motivation

Splitting them early would create drift and make the implementation the real standard in practice.

## Proposal

- `/spec` is normative
- `/fixtures` expresses compatibility examples
- `/conformance` proves contracts
- `/internal` remains non-normative implementation detail

## Compatibility

This is compatible with future extraction of spec assets if the project later needs a separate distribution model.

## Migration

None.


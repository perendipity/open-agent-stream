# Compatibility Policy

Compatibility is defined by:

- the published schemas
- the public Go contracts in `/pkg`
- the fixture corpus
- the conformance suite

An adapter or sink is considered compatible when it passes the relevant conformance tests for its declared spec version.

See the [`compatibility matrix`](compatibility-matrix.md) for the current
reference implementations and the current third-party extension status.

Compatibility is not the same thing as runtime registration. A type can be
compatible with the published contracts before the stock `oas` CLI knows how to
instantiate it automatically.

A credible compatibility claim should also document:

- the spec version being targeted
- the type string and required config options
- replay behavior for sinks
- privacy or redaction assumptions when data leaves the machine


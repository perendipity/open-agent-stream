# Portability Goals

Portability in OAS means a developer can keep control of retained session data
while moving it between tools, machines, and destinations without re-parsing
vendor-native artifacts or depending on a vendor service to replay history.

The standard should be portable across:

- operating systems
- local directory layouts
- single-machine and multi-machine deployments
- source vendors
- sink vendors
- inspection, export, replay, and migration workflows

The repo prioritizes simple on-disk formats, a single-binary collector,
deterministic exports, and explicit versioned schemas.

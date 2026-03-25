# Examples

These example configs are shape references for common setups.

- [`config.example.json`](config.example.json): local-first starter with `stdout`
  plus a placeholder `http` sink showing the `settings` and `delivery` blocks
- [`config.command.example.json`](config.command.example.json): command-based
  remote delivery, suitable for `rsync`, `scp`, or a wrapper transport
- [`config.s3.example.json`](config.s3.example.json): S3 archival delivery with
  a deterministic object key

All example configs assume you run commands from the repo root so the fixture
source paths `./fixtures/...` resolve correctly.

Treat remote endpoints, bucket names, tokens, and destination paths in these
files as placeholders. Edit them before you point OAS at a real destination.

# Examples

These example configs are shape references for common setups.

- [`config.example.json`](config.example.json): local-first starter with `stdout`
  plus a placeholder `http` sink showing the `settings` and `delivery` blocks
- [`config.command.example.json`](config.command.example.json): command-based
  remote delivery, suitable for `rsync`, `scp`, or a wrapper transport
- [`config.s3.example.json`](config.s3.example.json): S3 archival delivery with
  a deterministic object key
- [`config.external.example.json`](config.external.example.json): out-of-process
  external sink runtime example for proprietary or unbundled destinations
- [`config.multi-machine.template.json`](config.multi-machine.template.json):
  full per-machine template for serious multi-machine setups
- [`shared-s3-destination.template.json`](shared-s3-destination.template.json):
  shared sink and privacy snippet intended to live in a private ops repo, not
  as a standalone OAS config

All example configs assume you run commands from the repo root so the fixture
source paths `./fixtures/...` resolve correctly.

Treat remote endpoints, bucket names, tokens, and destination paths in these
files as placeholders. Edit them before you point OAS at a real destination.

For serious multi-machine use, do not keep the live machine configs inside the
public OAS repo checkout. Keep templates here for reference, but keep committed
live configs in a private ops repo or render them into
`~/.config/open-agent-stream/` on each host.

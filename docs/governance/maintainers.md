# Maintainer Model

Maintainers are responsible for:

- keeping the standard product-neutral
- requiring fixtures and conformance for normative changes
- versioning the spec independently from the binary
- documenting migrations and deprecations

Normative changes should land through RFCs. Runtime-only implementation details can land through normal review.

## Branch Protection

`main` is protected in GitHub. Normal changes should land through pull requests and pass the required `build` and `cli-smoke` checks before merge.

An administrative bypass exists for emergency maintenance, but it is not the normal development path. If workflow or job names change, update the repository ruleset so required checks stay aligned with CI.

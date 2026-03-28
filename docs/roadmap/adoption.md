# Adoption Criteria

For practical install notes and early-adopter operating guidance, see:

- [`../adoption/install.md`](../adoption/install.md)
- [`../adoption/early-adopters.md`](../adoption/early-adopters.md)

The project is succeeding when:

- two or more source families can emit the same canonical taxonomy
- third parties can build adapters or sinks without importing `/internal`
- replay works across implementation upgrades
- a developer can gather and retain session history across one or more machines
  without depending on vendor-hosted infrastructure
- inspection and deterministic export work as first-class local workflows
- users can keep full local fidelity while exporting only redacted views

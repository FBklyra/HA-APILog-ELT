# Governance

## Roles

| Role | Who | Responsibilities |
|---|---|---|
| Maintainer (project lead) | **Freddy Beltran** | roadmap, releases, final say on design and merges, security response, code of conduct enforcement |
| Committers | invited by the maintainer after sustained, high-quality contributions | review and merge pull requests in their area |
| Contributors | anyone | issues, pull requests, docs, tests, connector kits |

## Decisions

- Day-to-day: lazy consensus in the pull request; a committer or the maintainer approves.
- Larger changes (new connector SPI, protocol change, new dependency with a non-permissive licence, security-relevant defaults): written proposal as an ADR under `docs/adr/`, open for comment ≥ 7 days, maintainer decides.
- Non-negotiable project rules (read-only sources, outbound-only agent, whitelisted commands, exactly-once checkpointing, never drop data silently, public APIs only) change only by ADR approved by the maintainer.

## Contributions

- Licence: Apache-2.0, inbound = outbound; DCO sign-off required (see [`CONTRIBUTING.md`](CONTRIBUTING.md)).
- Clean-room rule and provenance record: [`docs/legal/PROVENANCE.md`](docs/legal/PROVENANCE.md).
- Every source file carries an SPDX header: [`docs/legal/SPDX.md`](docs/legal/SPDX.md).

## Releases

- Semantic versioning; release notes list breaking changes, migrations and security fixes.
- Published artefacts: linux/amd64 container images, x64 Windows agent, x64 Linux agent, Helm chart, compose bundles, SBOM, scan report, SHA-256 checksums, signatures (see `deploy/ci/`).

## Changes to this document

By pull request approved by the maintainer.

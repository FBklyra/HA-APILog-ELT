# Security policy

## Reporting a vulnerability

- **Do not open a public issue** for security problems.
- Report privately through the repository's **"Report a vulnerability"** button (GitHub private vulnerability reporting, *Security → Advisories*).
- If that is not available, contact the maintainer listed in [`GOVERNANCE.md`](GOVERNANCE.md) and ask for a private channel. Do not send exploit details in the first message.

Include:

| Item | Detail |
|---|---|
| Component | hub, edge agent (Windows / Linux), connector, console, deploy files |
| Version | `alelt version` output or image tag / digest |
| Impact | what an attacker can do, prerequisites |
| Reproduction | minimal steps or proof of concept — **fake data only**, never production data or credentials |

## What to expect

| Step | Target |
|---|---|
| Acknowledgement | 5 working days |
| Initial assessment | 15 working days |
| Fix or mitigation | depends on severity; coordinated disclosure, normally ≤ 90 days |
| Credit | in the advisory, if you wish |

Best effort by volunteers; no bounty programme.

## Supported versions

| Version | Security fixes |
|---|---|
| latest minor release | yes |
| older releases | no — upgrade |

## Scope notes

- In scope: authentication / RBAC bypass, agent ↔ hub protocol (mTLS, enrolment tokens, command whitelist), secret leakage, injection, deny-list bypass, supply chain of the published images.
- Out of scope: issues in third-party vendor software, insecure deployments that ignore the documented defaults (e.g. dev auth enabled in production), the throw-away local lab (`lab/`), which is for 127.0.0.1 use only.

## Hardening references

- Production checklist: [`deploy/INSTALL-IN-A-MORNING.md`](deploy/INSTALL-IN-A-MORNING.md), security zones in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- Every release ships an SBOM, a vulnerability scan report, SHA-256 checksums and signed images.

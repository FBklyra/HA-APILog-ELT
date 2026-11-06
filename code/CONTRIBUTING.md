# Contributing to ALELT

Licence: Apache-2.0 ([`LICENSE`](LICENSE), [`NOTICE`](NOTICE)). By contributing you agree your contribution is licensed under the same terms.

## Developer Certificate of Origin (DCO)

Every commit must be signed off (`git commit -s`), certifying the [Developer Certificate of Origin 1.1](https://developercertificate.org/):

```
Signed-off-by: Your Name <you@example.com>
```

Commits without a valid `Signed-off-by` line matching the author are not merged.

## Clean-room rule

| Allowed | Not allowed |
|---|---|
| Public vendor documentation (API references, log-record format docs, SQL references) | Proprietary vendor source code, or code derived from it |
| Behaviour observed on your own licensed test systems (e.g. `tests/dev-db2` spikes) | Documents received under NDA or any confidentiality obligation |
| Public sample code **read** as reference, re-written independently | Copy-pasted vendor sample code or headers |
| Your own original work | Decompiled, disassembled or reverse-engineered binaries |

- Record any new external source in [`docs/legal/PROVENANCE.md`](docs/legal/PROVENANCE.md) in the same pull request.
- Never commit vendor client libraries or headers (e.g. IBM Db2 `db2app64.dll`, `sqlenv.h`); build scripts locate them from a local install.
- Vendor-specific migration tooling belongs in `extensions/`, not in the core (see [`docs/legal/PUBLISHING.md`](docs/legal/PUBLISHING.md)).

## Pull requests

- Follow the non-negotiable rules R1–R10 and the test ladder (`tests/ladder/`); every change ships with tests.
- CI must be green: build, lint/type checks, unit tests, secret scan, dependency audit, vendor-neutrality check.
- No secrets, credentials, customer data or real hostnames in code, config, tests or logs.

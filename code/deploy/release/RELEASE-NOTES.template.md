# ALELT {{VERSION}} — release notes ({{DATE}})

> Universal HA API Log ELT (ALELT). Apache-2.0. Provided "AS IS", without warranty — see `DISCLAIMER.md`.
> Public release artefacts are **linux/amd64** (hub image) and **x64** (Windows agent, Linux daemon).

## Highlights
- <!-- 3–5 bullets: what changed for users -->

## Connectors
| Area | Change | Status |
|---|---|---|
| Sources | <!-- e.g. MySQL binlog: … --> | stable / beta / planned |
| Destinations | | |

## Upgrade notes
- Metadata migrations run automatically (`alelt migrate` / the `migrate` service). Back up the metadata database first.
- Breaking changes: <!-- none / list -->

## Assets
| File | Size |
|---|---|
{{ASSETS}}

Verify: `sha256sum -c SHA256SUMS` (Linux) · `Get-FileHash <file> -Algorithm SHA256` (Windows).
Image signature (registry builds): `cosign verify <registry>/<namespace>/alelt:{{VERSION}} --certificate-identity-regexp '<repo>' --certificate-oidc-issuer <issuer>`.
Offline image install: `docker load -i alelt-{{VERSION}}-linux-amd64.image.tar`.

## Known issues
- <!-- list -->

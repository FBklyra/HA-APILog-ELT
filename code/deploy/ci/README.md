# deploy/ci — reference release pipelines

Reference pipelines to build, test, scan, sign and publish an ALELT release. Copy them into your fork; they are **not** wired into this repository's own CI.

| File | Platform | Copy to |
|---|---|---|
| `github/release.yml` | GitHub Actions | `.github/workflows/release.yml` |
| `gitlab/.gitlab-ci.yml` | GitLab CI | `.gitlab-ci.yml` (repo root) |

## What a release contains (public release = amd64 / x64 only)

| Asset | Built by |
|---|---|
| `alelt:<version>` image, **linux/amd64**, pushed to any OCI registry | buildx (`deploy/image/Dockerfile`) |
| `alelt-<v>-linux-amd64.image.tar` (offline `docker load`) | `docker save` |
| `alelt-agent-<v>-windows-x64.zip` (+ `.msi` when WiX v4 is available) | `edge-agent/core/build.ps1` (MSVC x64) + `deploy/release/build-release.ps1 -AgentOnly` |
| `alelt-agentd-<v>-linux-x64.tar.gz` | `deploy/release/build-release.sh` |
| `alelt-compose-<v>.tar.gz`, `alelt-helm-<v>.tgz` | `deploy/release/build-release.sh` |
| `alelt-<v>.sbom.spdx.json` | syft |
| `RELEASE-NOTES.md`, `SHA256SUMS` | release scripts |

- arm64: optional **test build only** (GitHub input `test_arm64`, GitLab `TEST_ARM64=1`). Never pushed, never attached. Local arm64 builds: `deploy/build-image.ps1` (host arch by default).

## Pipeline stages

| Stage | GitHub job | GitLab job | Gate |
|---|---|---|---|
| Tests | `test-python` (hub on SQLite metadata, Linux daemon), `test-console` (tsc, vitest, vite build), `test-decoder` (C ctest) | `test:*` | must pass before the image |
| Image | `image`: `verify` target (decoder tests + golden corpus on amd64), then amd64 build | `build:image` | — |
| Windows agent | `agent-windows` (windows-2022, MSVC x64, ctest, zip + MSI) | `build:agent-windows` (runner tag `windows`) | ctest |
| SBOM | syft (SPDX JSON) | `scan:image` | — |
| Scan | trivy: fails on fixable **CRITICAL** | `scan:image` | blocking |
| Sign | cosign keyless (OIDC) by default; key mode when `COSIGN_PRIVATE_KEY` is set; SBOM attested | `push-sign:image` | tags only |
| Release | `bundle`: assets + `SHA256SUMS`, draft GitHub release | `release:bundle`, `release:publish` | tags only |

## Settings

| Name | Kind | Default | Meaning |
|---|---|---|---|
| `REGISTRY` | variable | `ghcr.io` / `$CI_REGISTRY` | any OCI registry (Harbor, Artifactory, ECR, ACR, GAR, Quay …) |
| `IMAGE_NAMESPACE` | variable | repo owner / project namespace | path under the registry |
| `REGISTRY_USER`, `REGISTRY_PASSWORD` | secret | built-in token | push credentials |
| `COSIGN_PRIVATE_KEY`, `COSIGN_PASSWORD` | secret | empty = keyless | key-based signing |
| Tool versions | env | syft v1.18.1 · trivy 0.58.1 · cosign v2.4.1 · helm v3.16.2 · WiX 4.0.6 | bump deliberately |

- Pin third-party actions to commit SHAs in your fork (tags are shown for readability).
- Verify a signed image: `cosign verify <registry>/<ns>/alelt:<v> --certificate-identity-regexp '<your repo>' --certificate-oidc-issuer <issuer>` (key mode: `--key cosign.pub`).

## Local equivalent

```powershell
.\deploy\release\build-release.ps1 -Version 0.2.0 -DryRun      # plan only
.\deploy\release\build-release.ps1 -Version 0.2.0 -SkipImages  # agent zip, daemon, compose, helm, checksums
.\deploy\release\build-release.ps1 -Version 0.2.0              # + linux/amd64 image tar (buildx, emulated on ARM hosts)
```

```sh
deploy/release/build-release.sh --version 0.2.0 [--skip-images] [--dry-run] [--agent-zip <windows zip>]
```

Output: `dist/release/<version>/`.

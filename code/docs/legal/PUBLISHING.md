# Publishing the public repository

The internal repository contains material that is never published. [`/.publicignore`](../../.publicignore) lists those paths; the public repo is an export of everything else.

## Procedure

| # | Step | Command / check |
|---|---|---|
| 1 | Start from a clean, green `main` | CI green, including `vendor-neutral` job (`.github/workflows/security.yml`) |
| 2 | Export history without excluded paths into a fresh clone | `git clone --no-local . ../alelt-public && cd ../alelt-public` then `grep -v '^#' ../code/.publicignore > ../paths.txt && git filter-repo --invert-paths --paths-from-file ../paths.txt` (or, for a single subtree, `git subtree split`) |
| 3 | Vendor-neutrality check on the export (must print nothing) | `git grep -i -l "$(printf 'five%s' 'tran')"` and the same over all commit messages: `git log --all --format=%B \| grep -i "$(printf 'five%s' 'tran')"` |
| 4 | Secret scan on the full export history | `gitleaks detect --source . --log-opts="--all"` |
| 5 | Legal files present at root | `LICENSE` (Apache-2.0, "Copyright 2026 Freddy Beltran"), `NOTICE`, `CONTRIBUTING.md` (DCO + clean-room rule), `docs/legal/PROVENANCE.md` |
| 6 | Every commit has a DCO sign-off | `git log --format='%H %(trailers:key=Signed-off-by)'` — no empty trailer |
| 7 | No vendor binaries or headers | `git ls-files \| grep -Ei '\.(dll\|lib\|so)$\|sqlenv\.h\|db2ApiDf\.h'` prints nothing |
| 8 | Build and test the export standalone | console `npm ci && npm test && npm run build`; Python/C jobs as in CI |
| 9 | Push to the public remote (two-person review of steps 3–7) | `git push public main --tags` |

Any failure at steps 3–8 stops the publish; fix in the internal repo and start again.

## Rules

- Vendor-specific migration tooling lives under `extensions/<name>/` and is listed in `.publicignore` unless explicitly cleared for publication.
- Core code, console, docs, runbooks and ADRs stay vendor-neutral: "legacy pipeline", switch targets `LEGACY` / `ALELT`, output profiles `alelt` / `golden_spec`.
- Never weaken step 3 by adding exceptions; move the offending file to an extension instead.

# SPDX headers policy

| Rule | Detail |
|---|---|
| Every new source file | first comment line (after a shebang, if any): `SPDX-License-Identifier: Apache-2.0` |
| Copyright | not repeated per file; held in [`NOTICE`](../../NOTICE) (© 2026 Freddy Beltran and contributors) |
| Third-party files copied in (only if licence-compatible) | keep their original SPDX identifier and copyright; record them in [`PROVENANCE.md`](PROVENANCE.md) |
| Generated files | header added by the generator (e.g. `lab/gen_catalog.py`) |
| Files without comments (JSON, lock files, images) | exempt; covered by the repository licence |
| Vendor code / headers | never committed (clean-room rule) |

Comment syntax per language:

| Language | Header |
|---|---|
| Python, shell, YAML, Dockerfile, PowerShell, properties | `# SPDX-License-Identifier: Apache-2.0` |
| C, C headers, TypeScript, JavaScript | `// SPDX-License-Identifier: Apache-2.0` (or `/* ... */` in C) |
| SQL | `-- SPDX-License-Identifier: Apache-2.0` |
| Markdown | not required |

Existing files get headers in a dedicated pull request (mechanical, no other change). A CI check (`reuse lint` or a grep over tracked source files) enforces the rule once that pass is merged.

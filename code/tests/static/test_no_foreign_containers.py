# SPDX-License-Identifier: Apache-2.0
"""Guard (rule X-109): no repo file may reference the Docker containers of another project on this host.

Fails if any text file names one of those containers or uses their host ports (15432, 13306, 13308, host-side 1433).
Tests and defaults use only our own containers (alelt-test-postgres :55433, alelt-test-pgland :15435,
alelt-test-mysql9 :53307, alelt-test-mariadb :53308, alelt-test-sqledge :51433, alelt-test-oracle :51521,
product stack Postgres :15433). The port 1433 itself stays allowed as the SQL Server default / container-side port.

Allowed: this file and the internal working notes listed in .publicignore (never published), which hold the rule text.
Run: python -m pytest tests/static -q   (stdlib only, no repo imports)
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ALLOWED = {"tests/static/test_no_foreign_containers.py"}


def _unpublished() -> list[str]:
    """Paths excluded from the public export (.publicignore): internal notes that state the rule itself."""
    f = REPO / ".publicignore"
    if not f.exists():
        return []
    return [ln.split("#", 1)[0].strip() for ln in f.read_text(encoding="utf-8").splitlines() if ln.split("#", 1)[0].strip()]


def _is_unpublished(rel: str, patterns: list[str]) -> bool:
    return any(rel.startswith(x) if x.endswith("/") else rel == x for x in patterns)

SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", "dist", "build", "out", ".pytest_cache",
             ".mypy_cache", ".ruff_cache", ".alelt"}
MAX_BYTES = 4_000_000

_NAMES = ["local" + "-" + n for n in ("sql-edge", "mariadb", "postgres", "mysql9")]
FORBIDDEN = re.compile(
    r"(?<![\w-])(" + "|".join(re.escape(n) for n in _NAMES) + r")(?![\w-])"
    r"|(?:[:,=]\s*|\bport\S{0,4}\s*|-p\s+)(?:1543" r"2|1330" r"6|1330" r"8)(?!\d)"
    r"|\b(?:localhost|127\.0\.0\.1|host\.docker\.internal)[:,]143" r"3(?!\d)"
    r"|(?<![\d.])143" r"3:1433(?!\d)",
    re.IGNORECASE,
)


def violations_in(text: str) -> list[str]:
    return [m.group(0) for m in FORBIDDEN.finditer(text)]


def _files() -> list[Path]:
    out = []
    unpublished = _unpublished()
    for p in REPO.rglob("*"):
        rel = p.relative_to(REPO)
        if any(part in SKIP_DIRS for part in rel.parts[:-1]) or not p.is_file():
            continue
        if rel.as_posix() in ALLOWED or _is_unpublished(rel.as_posix(), unpublished) or p.stat().st_size > MAX_BYTES:
            continue
        out.append(p)
    return out


def test_patterns_self_check() -> None:
    assert violations_in("docker inspect " + _NAMES[0])
    assert violations_in("url = 'postgresql://u:p@localhost:1543" "2/db'")
    assert violations_in("mssql://sa:x@127.0.0.1:143" "3")
    assert violations_in("--port 1330" "8")
    assert not violations_in("alelt-test-sqledge :51433, default port 1433, lab 47143:1433")
    assert not violations_in("my-local-postgres-thing alelt-test-postgres 55433 15435 53307 53308")


def test_repo_has_no_foreign_container_references() -> None:
    bad = []
    for p in _files():
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for hit in violations_in(text):
            bad.append(f"{p.relative_to(REPO).as_posix()}: {hit!r}")
    assert not bad, "references to another project's containers/ports (rule X-109):\n" + "\n".join(bad[:50])

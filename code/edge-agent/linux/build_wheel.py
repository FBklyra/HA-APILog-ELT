"""Build a minimal wheel of the Linux daemon for ONE source engine (F-27).

The DB host gets only what the daemon needs: `aleltd` + `alelt_common` + the chosen source connector package(s) -
never `alelt_hub`, the loader, destinations or other connectors (smaller attack surface than the whole source tree).

  python edge-agent/linux/build_wheel.py --engine postgres [--out dist/agent] [--version 0.1.0]

Engines: postgres, mysql, mariadb (includes alelt_src_mysql), sqlserver, oracle, fake (tests). Db2 on Linux (X-128,
future) is not packaged here. Third-party drivers are declared as dependencies, not vendored.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ENGINES: dict[str, tuple[list[str], list[str]]] = {          # engine -> (connector packages, pip requirements)
    "postgres": (["connectors/sources/postgres/alelt_src_postgres"], ["psycopg2-binary>=2.9"]),
    "mysql": (["connectors/sources/mysql/alelt_src_mysql"], ["PyMySQL>=1.1"]),
    "mariadb": (["connectors/sources/mysql/alelt_src_mysql", "connectors/sources/mariadb/alelt_src_mariadb"], ["PyMySQL>=1.1"]),
    "sqlserver": (["connectors/sources/sqlserver/alelt_src_sqlserver"], ["python-tds>=1.15"]),
    "oracle": (["connectors/sources/oracle/alelt_src_oracle"], ["oracledb>=2.0"]),
    "fake": ([], []),
}
CORE = ["edge-agent/linux/aleltd", "hub/alelt_common"]
BASE_REQ = ["pydantic>=2.6"]


def stage(engine: str, dst: Path, version: str) -> list[str]:
    pkgs, reqs = ENGINES[engine]
    names = []
    for rel in CORE + pkgs:
        src = ROOT / rel
        shutil.copytree(src, dst / src.name, ignore=shutil.ignore_patterns("tests", "__pycache__", "*.pyc", "corpus"))
        names.append(src.name)
    deps = ", ".join(f'"{r}"' for r in BASE_REQ + reqs)
    (dst / "pyproject.toml").write_text(f"""[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "alelt-agentd-{engine}"
version = "{version}"
description = "ALELT Linux capture daemon ({engine} source only)"
license = {{text = "Apache-2.0"}}
requires-python = ">=3.9"
dependencies = [{deps}]
optional-dependencies = {{zstd = ["zstandard>=0.22"]}}

[project.scripts]
alelt-agentd = "aleltd.__main__:main"

[tool.setuptools]
packages = {{find = {{include = [{", ".join(f'"{n}", "{n}.*"' for n in names)}]}}}}
""", encoding="utf-8")
    lic = ROOT / "LICENSE"
    if lic.exists():
        shutil.copy2(lic, dst / "LICENSE")
    return names


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=sorted(ENGINES))
    ap.add_argument("--out", default=str(ROOT / "dist" / "agent"))
    ap.add_argument("--version", default="0.1.0")
    a = ap.parse_args(argv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="alelt-agentd-") as tmp:
        stage(a.engine, Path(tmp), a.version)
        r = subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "-w", str(out),
                            tmp], capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-2000:], r.stderr[-2000:], file=sys.stderr)
            return r.returncode
    print(next(iter(sorted(out.glob(f"alelt_agentd_{a.engine}-*.whl"))), ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())

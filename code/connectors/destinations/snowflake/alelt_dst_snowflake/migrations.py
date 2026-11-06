"""Numbered, idempotent migration runner with ALELT_CONTROL.MIGRATION_HISTORY."""
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .executor import Executor
from .sqlgen import check_ident, control, render, split_statements

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_FILE = re.compile(r"^(\d{3})_([a-z0-9_]+)\.sql$")


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path

    @property
    def text(self) -> str:
        return self.path.read_text(encoding="utf-8")

    @property
    def checksum(self) -> str:
        """sha256 of the template (not the rendered SQL) so parameters do not count as edits."""
        return hashlib.sha256(self.text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found: list[Migration] = []
    for p in sorted(directory.glob("*.sql")):
        m = _FILE.match(p.name)
        if not m:
            raise MigrationError(f"bad migration file name {p.name} (want NNN_name.sql)")
        found.append(Migration(int(m.group(1)), m.group(2), p))
    versions = [m.version for m in found]
    if len(set(versions)) != len(versions):
        raise MigrationError("duplicate migration version")
    return found


def default_params(database: str, source: str, legacy_schema: str | None = None, **over: object) -> dict[str, object]:
    """Template parameters; schema names follow ALELT_<SOURCE> / <SOURCE>_RAW."""
    src = check_ident(source.upper())
    p: dict[str, object] = {
        "DATABASE": check_ident(database), "LANDING_SCHEMA": f"ALELT_{src}", "VIEW_SCHEMA": f"{src}_RAW",
        "LEGACY_SCHEMA": check_ident(legacy_schema) if legacy_schema else "",
        "WAREHOUSE": "ALELT_WH", "WAREHOUSE_SIZE": "XSMALL", "CREDIT_QUOTA": 100, "RAW_RETENTION_DAYS": 60,
    }
    p.update(over)
    return p


def history_bootstrap_sql(database: str) -> list[str]:
    return [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        f"CREATE SCHEMA IF NOT EXISTS {database}.ALELT_CONTROL",
        f"CREATE TABLE IF NOT EXISTS {control(database, 'MIGRATION_HISTORY')} (VERSION NUMBER(6,0) NOT NULL PRIMARY KEY, "
        f"NAME VARCHAR(200) NOT NULL, CHECKSUM VARCHAR(64) NOT NULL, "
        f"APPLIED_AT TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), APPLIED_BY VARCHAR(200) DEFAULT CURRENT_USER())",
    ]


class MigrationRunner:
    def __init__(self, executor: Executor, params: Mapping[str, object], directory: Path = MIGRATIONS_DIR) -> None:
        self.ex = executor
        self.params = dict(params)
        self.database = str(self.params["DATABASE"])
        self.directory = directory

    def applied(self) -> dict[int, str]:
        rows = self.ex.execute(f"SELECT VERSION, CHECKSUM FROM {control(self.database, 'MIGRATION_HISTORY')}")
        return {int(v): str(c) for v, c in rows}

    def plan(self) -> list[Migration]:
        done = self.applied()
        pending: list[Migration] = []
        for m in discover(self.directory):
            if m.version in done:
                if done[m.version] != m.checksum:
                    raise MigrationError(f"migration {m.version:03d}_{m.name} was edited after being applied")
                continue
            pending.append(m)
        return pending

    def run(self) -> list[int]:
        """Apply pending migrations in order; returns applied versions."""
        for s in history_bootstrap_sql(self.database):
            self.ex.execute(s)
        applied: list[int] = []
        for m in self.plan():
            for stmt in split_statements(render(m.text, self.params)):
                self.ex.execute(stmt)
            self.ex.execute(
                f"INSERT INTO {control(self.database, 'MIGRATION_HISTORY')} (VERSION, NAME, CHECKSUM) VALUES (%s, %s, %s)",
                (m.version, m.name, m.checksum))
            applied.append(m.version)
        return applied

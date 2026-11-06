"""View interface (<SOURCE>_RAW): explicit column lists, never a star select (ADR-006)."""
from __future__ import annotations

from collections.abc import Sequence

from alelt_common.models import TableInfo

from .config import OutputProfile


def view_columns(table: TableInfo, profile: OutputProfile, golden_columns: Sequence[str] | None = None,
                 retained: Sequence[str] = ()) -> list[str]:
    """Golden column order when available (R8); otherwise data columns by ordinal, retained (dropped-at-source)
    columns, then system columns of the output profile."""
    if golden_columns:
        return list(golden_columns)
    cols = [c.name for c in sorted(table.columns, key=lambda c: c.ordinal)]
    cols += [c for c in retained if c not in cols]
    if not table.key_columns():
        cols.append(profile.id_column)
    cols += [profile.synced_column, profile.deleted_column]
    return cols


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def view_ddl(database: str, view_schema: str, table: str, source_schema: str, columns: Sequence[str]) -> str:
    """Engine-neutral preview of the view the destination publishes (used for review / console diff)."""
    if not columns:
        raise ValueError("explicit column list required")
    cols = ",\n  ".join(_q(c) for c in columns)
    return (f"CREATE OR REPLACE VIEW {database}.{view_schema}.{_q(table)} COPY GRANTS AS\nSELECT\n  {cols}\n"
            f"FROM {database}.{source_schema}.{_q(table)}")

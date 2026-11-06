"""Built-in PostgreSQL destination = the generic SQL destination (alelt_dst_sql, dialect postgres) - F-16.

Same contract as before (one code path for every SQL engine, no duplicated apply logic):
- landing tables `<schema>.<dest_name>`: source columns + `<synced> timestamptz` + `<deleted> boolean` (names from
  the output profile), identifiers folded to lower case;
- control schema: CHECKPOINT (stream PK, position, batch_id, updated_at, watermarks) and AUDIT_BATCH;
- apply() = ONE transaction: checkpoint row lock + compare-and-set (F-05) + bulk upserts / soft deletes +
  checkpoint + per-table watermarks + audit row; no-op if checkpoint >= to_pos.
Control tables created by the earlier hand-written connector (bigserial audit id, jsonb rows_by_table, position
NOT NULL) keep working: the watermarks column is added at bootstrap, the audit id is left to the database.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from alelt_common.models import ColumnInfo, Position
from alelt_dst_kit.base import default_compare
from alelt_dst_sql import SqlDestination

__all__ = ["PostgresDestination", "default_compare", "pg_type"]


class PostgresDestination(SqlDestination):
    engine = "postgres"
    display_name = "PostgreSQL"

    def __init__(self, url: str, schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Callable[[Position, Position], int] | None = None,
                 **kw: Any) -> None:
        super().__init__(url, schema=schema.lower(), control_schema=control_schema.lower(),
                         output_profile=output_profile, compare=compare, dialect="postgres", **kw)


def pg_type(c: ColumnInfo) -> str:
    """Physical Postgres type of a canonical column (config/type_maps/canonical_to_postgres.yaml)."""
    from alelt_dst_sql.dialects import DIALECTS

    return DIALECTS["postgres"]().types.physical(c)  # type: ignore[no-any-return]

"""Amazon Redshift destination: temp stage + MERGE + checkpoint + audit in ONE transaction (R6).

URL (ALELT_DEST_URL): redshift://user:${ALELT_RS_PASSWORD}@cluster.xxxx.region.redshift.amazonaws.com:5439/dev?sslmode=require
  driver=psycopg (default, PostgreSQL wire protocol) | redshift_connector (Apache-2.0, IAM auth)
  compat=postgres  - run against PostgreSQL >= 15 as a rough approximation (local tests only): VARBYTE/SUPER are
                     replaced by bytea/text; Redshift-only behaviour (distribution, SUPER, late binding) is NOT tested.

Transaction: LOCK checkpoint -> re-check checkpoint -> per table: [truncate] -> per op group:
CREATE TEMP TABLE + executemany INSERT -> MERGE (upsert / soft delete) or DELETE ... WHERE EXISTS (hard delete)
-> DELETE+INSERT checkpoint row -> audit row -> COMMIT. Any error => ROLLBACK, nothing visible.
"""
from __future__ import annotations

from typing import Any

from alelt_dst_kit.sqlmerge import DbapiExecutor, Dialect, Executor, SqlMergeDestination

_FALLBACK = {"types": {
    "BOOLEAN": "BOOLEAN", "SHORT": "SMALLINT", "INT": "INTEGER", "LONG": "BIGINT", "DECIMAL": "DECIMAL({p},{s})",
    "FLOAT": "REAL", "DOUBLE": "DOUBLE PRECISION", "LOCALDATE": "DATE", "LOCALTIME": "TIME",
    "LOCALDATETIME": "TIMESTAMP", "INSTANT": "TIMESTAMPTZ", "STRING": "VARCHAR({n})", "BINARY": "VARBYTE",
    "JSON": "SUPER", "XML": "VARCHAR(65535)"},
    "defaults": {"STRING_without_length": "VARCHAR(65535)", "STRING_over_max": "VARCHAR(65535)",
                 "DECIMAL_without_precision": "DECIMAL(38,0)"},
    "limits": {"STRING_max_length": 65535, "STRING_bytes_per_char": 4, "DECIMAL_max_precision": 38}}
POSTGRES_COMPAT = {"BINARY": "bytea", "JSON": "text", "XML": "text"}


class RedshiftDialect(Dialect):
    name = "redshift"
    type_engine = "redshift"
    fallback = _FALLBACK
    stage_mode = "temp"
    param = "%s"

    def lock(self, ckpt: str) -> list[str]:
        return [f"LOCK {ckpt}"]

    def param_for(self, typ: str) -> str:
        # DST-redshift-02: a string bound into SUPER is a SUPER string scalar; JSON_PARSE stores the document
        return f"JSON_PARSE({self.param})" if typ.upper().startswith("SUPER") else self.param


class DuplicateKeyError(RuntimeError):
    """The landing table holds duplicate keys (Redshift does not enforce primary keys): MERGE would fail on every
    batch with 'attempted to update or delete the same row more than once' (DST-redshift-04)."""


class RedshiftDestination(SqlMergeDestination):
    engine = "redshift"

    def ensure_table(self, table: Any, dest_name: str, golden_ddl: str | None) -> None:
        super().ensure_table(table, dest_name, golden_ddl)
        keys = [self.ident(k) for k in self.key_names(table)]
        if not keys or self.u.flag("skip_duplicate_check"):
            return
        q = self.d.q
        kl = ", ".join(map(q, keys))
        _, rows = self.ex.query(f"SELECT {kl}, COUNT(*) FROM {self.t(dest_name)} GROUP BY {kl} HAVING COUNT(*) > 1 "
                                "LIMIT 3")
        if rows:
            raise DuplicateKeyError(
                f"{self.t(dest_name)}: duplicate key(s) {[tuple(r[:-1]) for r in rows]} - MERGE needs unique keys; "
                f"remove the extra rows (e.g. keep the newest {self.c_synced}) or resync the table")

    def make_dialect(self) -> Dialect:
        return RedshiftDialect(POSTGRES_COMPAT if (self.u.get("compat") or "") == "postgres" else None)

    def make_executor(self) -> Executor:
        u = self.u
        db = (u.path or "/dev").lstrip("/") or "dev"
        if (u.get("driver") or "psycopg") == "redshift_connector":
            def connect() -> Any:
                import redshift_connector  # Apache-2.0

                return redshift_connector.connect(host=u.host, port=u.port or 5439, database=db, user=u.user,
                                                  password=u.password, timeout=int(u.get("timeout") or 60))
        else:
            def connect() -> Any:
                import psycopg

                return psycopg.connect(host=u.host or "localhost", port=u.port or 5439, dbname=db, user=u.user,
                                       password=u.password, sslmode=u.get("sslmode") or "prefer",
                                       connect_timeout=int(u.get("timeout") or 30))
        return DbapiExecutor(connect)

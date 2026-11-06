"""Microsoft Fabric Warehouse / Azure Synapse dedicated pool (T-SQL) destination.

URL (ALELT_DEST_URL):
  fabric://<client-id>:${ALELT_FABRIC_SECRET}@<endpoint>.datawarehouse.fabric.microsoft.com:1433/<warehouse>
        ?authentication=ActiveDirectoryServicePrincipal&flavor=fabric
  flavor = fabric (default) | synapse | sqlserver ;  driver = ODBC driver name (default ODBC Driver 18 for SQL Server)
  encrypt=yes|no, trust_server_certificate=yes|no
Client: pyodbc (MIT) + Microsoft ODBC driver.

One T-SQL transaction per batch: [checkpoint lock: synapse/sqlserver UPDLOCK+HOLDLOCK; Fabric uses snapshot
isolation - a concurrent conflicting commit fails instead] -> per table: [truncate] -> per op group:
#temp table + fast executemany -> MERGE (upsert / soft delete) or DELETE ... WHERE EXISTS (hard delete)
-> checkpoint DELETE+INSERT -> audit row -> COMMIT TRANSACTION. Errors => ROLLBACK.
Fabric notes: varchar (UTF-8 collation) instead of nvarchar, datetime2(6) for instants (UTC), primary keys are
NONCLUSTERED NOT ENFORCED; MERGE / #temp tables require a current Fabric Warehouse release.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from alelt_common.models import CanonicalType, Position
from alelt_common.spi import ApplyStats
from alelt_dst_kit import ControlState, TablePlan
from alelt_dst_kit.sqlmerge import DbapiExecutor, Dialect, Executor, SqlMergeDestination

_FALLBACK = {"types": {
    "BOOLEAN": "bit", "SHORT": "smallint", "INT": "int", "LONG": "bigint", "DECIMAL": "decimal({p},{s})",
    "FLOAT": "real", "DOUBLE": "float", "LOCALDATE": "date", "LOCALTIME": "time(6)",
    "LOCALDATETIME": "datetime2(6)", "INSTANT": "datetime2(6)", "STRING": "varchar({n})",
    "BINARY": "varbinary(max)", "JSON": "varchar(max)", "XML": "varchar(max)"},
    "defaults": {"STRING_without_length": "varchar(max)", "STRING_over_max": "varchar(max)",
                 "DECIMAL_without_precision": "decimal(38,0)"},
    "limits": {"STRING_max_length": 8000, "STRING_bytes_per_char": 4, "DECIMAL_max_precision": 38}}


class TsqlDialect(Dialect):
    name = "tsql"
    type_engine = "fabric"
    fallback = _FALLBACK
    stage_mode = "temp"
    param = "?"
    true, false = "1", "0"
    end = ";"

    def __init__(self, flavor: str = "fabric", overrides: dict[str, str] | None = None) -> None:
        super().__init__(overrides)
        self.flavor = flavor
        self.lock_checkpoint = flavor != "fabric"

    def q(self, ident: str) -> str:
        return "[" + ident.replace("]", "]]") + "]"

    def string_lit(self, s: str) -> str:
        return "N'" + s.replace("'", "''") + "'"

    def create_schema(self, schema: str) -> list[str]:
        return [f"IF SCHEMA_ID({self.string_lit(schema)}) IS NULL "
                f"EXEC({self.string_lit('CREATE SCHEMA ' + self.q(schema))})"]

    def create_table(self, fqn: str, coldefs: list[str], keys: list[str]) -> str:
        if keys and self.flavor == "sqlserver":
            pk = f", PRIMARY KEY ({', '.join(map(self.q, keys))})"
        elif keys:
            pk = f", PRIMARY KEY NONCLUSTERED ({', '.join(map(self.q, keys))}) NOT ENFORCED"
        else:
            pk = ""
        return (f"IF OBJECT_ID({self.string_lit(fqn)}, N'U') IS NULL "
                f"CREATE TABLE {fqn} ({', '.join(coldefs)}{pk})")

    def columns_sql(self, schema: str, table: str) -> tuple[str, Any]:
        return ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (schema, table))

    def add_column(self, fqn: str, col: str, typ: str) -> str:
        return f"ALTER TABLE {fqn} ADD {self.q(col)} {typ}"

    def temp_table(self, name: str, coldefs: list[str]) -> tuple[list[str], str]:
        ref = f"#{name}"
        if self.flavor != "fabric":
            # SQL Server / Synapse: #temp columns take the tempdb collation; a database with another collation
            # (UTF-8 for varchar) fails the MERGE join with error 468 - use the database collation
            coldefs = [_collate_db_default(c) for c in coldefs]
        return [f"IF OBJECT_ID('tempdb..{ref}') IS NOT NULL DROP TABLE {ref}",
                f"CREATE TABLE {ref} ({', '.join(coldefs)})"], ref

    def drop_temp(self, ref: str) -> list[str]:
        return [f"DROP TABLE {ref}"]

    def widen_sql(self, fqn: str, col: str, typ: str, nullable: bool = True) -> list[str]:
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(col)} {typ} {'NULL' if nullable else 'NOT NULL'}"]

    def lock(self, ckpt: str) -> list[str]:
        return [f"SELECT COUNT(*) FROM {ckpt} WITH (TABLOCKX, HOLDLOCK)"] if self.lock_checkpoint else []

    def bytes_lit(self, b: bytes) -> str:
        return "0x" + b.hex()

    def param_value(self, v: Any, ct: CanonicalType | None) -> Any:
        v = super().param_value(v, ct)
        if isinstance(v, datetime) and v.tzinfo is not None:     # datetime2 holds UTC
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        if isinstance(v, Decimal):                                # fast_executemany infers precision from row 1
            return format(v, "f")
        return v


class FabricDestination(SqlMergeDestination):
    engine = "fabric"

    def make_dialect(self) -> Dialect:
        return TsqlDialect((self.u.get("flavor") or "fabric").lower())

    def make_executor(self) -> Executor:
        u = self.u
        parts = {
            "DRIVER": "{" + (u.get("driver") or "ODBC Driver 18 for SQL Server") + "}",
            "SERVER": f"{u.host},{u.port or 1433}", "DATABASE": (u.path or "/").lstrip("/") or "master",
            "Encrypt": u.get("encrypt") or "yes",
            "TrustServerCertificate": u.get("trust_server_certificate") or "no",
            "Connection Timeout": u.get("timeout") or "30"}
        if u.get("authentication"):
            parts["Authentication"] = str(u.get("authentication"))
        if u.user:
            parts["UID"] = u.user
        if u.password:
            parts["PWD"] = "{" + u.password.replace("}", "}}") + "}"
        cs = ";".join(f"{k}={v}" for k, v in parts.items())

        def connect() -> Any:
            import pyodbc

            c = pyodbc.connect(cs, autocommit=False, timeout=int(u.get("timeout") or 30))
            c.timeout = int(u.get("query_timeout") or 600)
            return c
        return DbapiExecutor(connect)

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        super().commit_batch(stream, plans, to_pos, batch_id, prev,
                             ts.astimezone(timezone.utc).replace(tzinfo=None), stats)


def _collate_db_default(coldef: str) -> str:
    low = coldef.lower()
    for t in (" varchar(", " nvarchar(", " char(", " nchar("):
        i = low.find(t)
        if i >= 0:
            j = low.index(")", i) + 1
            return coldef[:j] + " COLLATE DATABASE_DEFAULT" + coldef[j:]
    return coldef

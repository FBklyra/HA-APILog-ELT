"""Databricks SQL warehouse destination (Delta tables via SQL). Client: databricks-sql-connector (Apache-2.0).

URL (ALELT_DEST_URL):
  databricks://token:${DATABRICKS_TOKEN}@adb-123.azuredatabricks.net/?http_path=/sql/1.0/warehouses/abc&catalog=main
Tables: <catalog>.<schema>.<table> (Delta, timestampNtz feature enabled), control <catalog>.<control>.checkpoint.

Exactly-once: Databricks SQL commits every statement on its own (no multi-table transaction), so each write is an
idempotent Delta write: before each statement the session sets
  spark.databricks.delta.write.txnAppId   = <stream>
  spark.databricks.delta.write.txnVersion = <seq> * 10000 + <statement no>
and Delta skips a write whose (appId, version) is already recorded for that table (Databricks "idempotent writes").
The checkpoint is one conditional MERGE (atomic single-table commit, `WHEN MATCHED AND seq = <prev seq>`) executed
last and read back; the checkpoint is also re-read before the first write (compare-and-set, F-05). A replay after a crash re-runs the batch
and Delta skips the statements that already committed. Atomicity is therefore per statement/table, never partial
within a table. Alternative path without a SQL warehouse: write the files with alelt_dst_delta (same txn mechanism).
Not verified against a live workspace (no local environment): SQL generation is unit-tested only.
"""
from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from alelt_common.models import CanonicalType, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError
from alelt_dst_kit import ControlState, TablePlan
from alelt_dst_kit.sqlmerge import Dialect, Executor, SqlMergeDestination, Tx, _col, bind_literals

_FALLBACK = {"types": {
    "BOOLEAN": "BOOLEAN", "SHORT": "SMALLINT", "INT": "INT", "LONG": "BIGINT", "DECIMAL": "DECIMAL({p},{s})",
    "FLOAT": "FLOAT", "DOUBLE": "DOUBLE", "LOCALDATE": "DATE", "LOCALTIME": "STRING", "LOCALDATETIME": "TIMESTAMP_NTZ",
    "INSTANT": "TIMESTAMP", "STRING": "STRING", "BINARY": "BINARY", "JSON": "STRING", "XML": "STRING"},
    "defaults": {"DECIMAL_without_precision": "DECIMAL(38,0)", "STRING_without_length": "STRING"},
    "limits": {"DECIMAL_max_precision": 38}}
STMT_SLOTS = 10000


def txn_app_id(stream: str) -> str:
    """Parse-safe, deterministic txnAppId (DST-databricks-03): `SET key = value` takes the raw rest of the line, so
    stream names like `<cid>#catchup:<t>` are reduced to [A-Za-z0-9_.-] plus a hash of the original name."""
    import hashlib
    import re

    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", stream)
    return safe if safe == stream else f"{safe[:80]}-{hashlib.sha1(stream.encode()).hexdigest()[:12]}"


class DatabricksDialect(Dialect):
    name = "databricks"
    type_engine = "databricks"
    fallback = _FALLBACK
    stage_mode = "inline"
    param = "?"
    lock_checkpoint = False

    def __init__(self, catalog: str | None = None) -> None:
        super().__init__({"STRING": "STRING"})
        self.catalog = catalog

    def q(self, ident: str) -> str:
        return "`" + ident.replace("`", "``") + "`"

    def table(self, schema: str, name: str) -> str:
        return (f"{self.q(self.catalog)}." if self.catalog else "") + f"{self.q(schema)}.{self.q(name)}"

    def create_schema(self, schema: str) -> list[str]:
        return [f"CREATE SCHEMA IF NOT EXISTS {(self.q(self.catalog) + '.') if self.catalog else ''}{self.q(schema)}"]

    def create_table(self, fqn: str, coldefs: list[str], keys: list[str]) -> str:
        return (f"CREATE TABLE IF NOT EXISTS {fqn} ({', '.join(coldefs)}) USING DELTA "
                "TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")

    def widen_sql(self, fqn: str, col: str, typ: str, nullable: bool = True) -> list[str]:
        return []                                       # STRING is unbounded; Delta cannot widen DECIMAL in place

    def types_sql(self, schema: str, table: str) -> tuple[str, Any] | None:
        return None

    def add_column(self, fqn: str, col: str, typ: str) -> str:
        return f"ALTER TABLE {fqn} ADD COLUMNS ({self.q(col)} {typ})"

    def string_lit(self, s: str) -> str:
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def bytes_lit(self, b: bytes) -> str:
        return f"X'{b.hex()}'"

    def lit(self, v: Any, typ: str, ct: CanonicalType | None = None) -> str:
        if ct is CanonicalType.LOCALTIME and v is not None and not isinstance(v, str):
            v = v.isoformat()
        return super().lit(v, typ, ct)

    def hard_delete(self, target: str, source: str, keys: list[str]) -> str:
        on = " AND ".join(f"s.{self.q(k)} = t.{self.q(k)}" for k in keys)
        return f"DELETE FROM {target} AS t WHERE EXISTS (SELECT 1 FROM {source} AS s WHERE {on})"


class DatabricksExecutor(Executor):
    """Autocommit statements on one SQL warehouse session (transaction() = same session, no rollback)."""

    supports_query_in_tx = True

    def __init__(self, connect: Any, dialect: Dialect) -> None:
        self._connect, self.d, self._conn = connect, dialect, None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None

    def _q(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        with self.conn.cursor() as cur:
            cur.execute(bind_literals(sql, params, self.d))
            names = [x[0] for x in cur.description or []]
            return names, ([tuple(r) for r in cur.fetchall()] if cur.description else [])

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Tx]:
        ex = self

        class T(Tx):
            def execute(self, sql: str, params: Any = None) -> None:
                ex._q(sql, params)

            def executemany(self, sql: str, rows: list[Any]) -> None:  # pragma: no cover
                raise NotImplementedError

            def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
                return ex._q(sql, params)
        yield T()

    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        return self._q(sql, params)

    def execute(self, sql: str, params: Any = None) -> None:
        self._q(sql, params)


class DatabricksDestination(SqlMergeDestination):
    engine = "databricks"
    # DST-databricks-04: one inline VALUES table per statement, sized by rendered characters (not 500 rows):
    # 50 000 rows = a few statements instead of 100 warehouse round trips
    chunk_rows = 50_000
    chunk_chars = 4_000_000
    atomic_tables = False

    def source(self, pl: Any, cols: tuple[str, ...], ops: list[Any], types: dict[str, str], cts: dict[str, Any],
               ts: datetime, name: str) -> tuple[str, list[tuple[str, Any, bool]], list[tuple[str, Any, bool]]]:
        """`(SELECT * FROM VALUES (...), (...) AS v(cols))`: an inline table (Databricks SQL), far smaller than one
        `SELECT ... UNION ALL` per row."""
        d = self.d
        tsty = d.ptype(_col(CanonicalType.INSTANT))
        names = [*(self.ident(c) for c in cols), self.c_synced]
        tsl = d.lit(ts, tsty)
        rows = ["(" + ", ".join([*(d.lit(op.row.get(c), types[c], cts.get(c)) for c in cols), tsl]) + ")"
                for op in ops]
        return (f"(SELECT * FROM VALUES {', '.join(rows)} AS v({', '.join(map(d.q, names))}))", [], [])

    def make_dialect(self) -> Dialect:
        return DatabricksDialect(self.u.get("catalog"))

    def make_executor(self) -> Executor:
        u = self.u

        def connect() -> Any:
            from databricks import sql  # databricks-sql-connector, Apache-2.0

            return sql.connect(server_hostname=u.host, http_path=u.get("http_path"), access_token=u.password,
                               _socket_timeout=int(u.get("timeout") or 900))
        return DatabricksExecutor(connect, self.d)

    def _existing(self, dest: str) -> set[str]:
        names, _ = self.ex.query(f"SELECT * FROM {self.t(dest)} LIMIT 0")
        return {n.lower() for n in names}

    def checkpoint_sql(self, stream: str, seq: int, to_pos: Position, batch_id: str, ts: datetime,
                       prev: ControlState | None = None) -> list[tuple[str, Any]]:
        """Conditional MERGE (single-table atomic Delta commit): updates only the row still at prev.seq, inserts only
        when the stream has none - checked by reading the row back (cas_mode 'readback')."""
        d = self.d
        prev = prev or ControlState(seq=seq - 1)
        tsl = d.lit(ts, d.ptype(_col(CanonicalType.INSTANT)))
        src = (f"(SELECT {d.string_lit(stream)} AS `stream`, CAST({seq} AS BIGINT) AS `seq`, "
               f"{d.string_lit(to_pos.text)} AS `position`, {d.string_lit(batch_id)} AS `batch_id`, "
               f"{tsl} AS `updated_at`, {d.string_lit(self.wm_text())} AS `watermarks`)")
        return [(f"MERGE INTO {self.c('checkpoint')} AS t USING {src} AS s ON t.`stream` = s.`stream` "
                 f"WHEN MATCHED AND t.`seq` = {prev.seq} THEN UPDATE SET * "
                 + ("WHEN NOT MATCHED THEN INSERT *" if prev.seq == 0 else ""), None)]

    def control_ddl(self) -> list[str]:
        return [s.replace(", PRIMARY KEY (`stream`)", "") for s in super().control_ddl()]

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        seq = prev.seq + 1
        app = txn_app_id(stream or f"snapshot-{batch_id}")
        n = 0

        def idem(tx: Tx) -> None:
            nonlocal n
            n += 1
            if n >= STMT_SLOTS:
                raise ValueError(f"more than {STMT_SLOTS} statements in one batch")
            tx.execute(f"SET spark.databricks.delta.write.txnAppId = {app}")
            tx.execute(f"SET spark.databricks.delta.write.txnVersion = {seq * STMT_SLOTS + n}")

        if stream is not None and to_pos is not None:   # CAS before any write (statements autocommit)
            self.cas(stream, prev, self.control_state(stream), to_pos)
        with self.ex.transaction() as tx:
            try:
                for i, pl in enumerate(plans):
                    self.count(pl, stats[pl.dest])
                    for sql, params, _many in self.plan_sql(pl, ts, stream, seq, i):
                        idem(tx)
                        tx.execute(sql, params)
                    if i == 0:
                        self._hook("after_data")
                self._hook("before_commit")
                if stream is not None and to_pos is not None:
                    sql, params = self.audit_sql(self.audit_record(stream, prev, to_pos, batch_id, stats), ts)
                    idem(tx)
                    tx.execute(sql, params)
                    for sql, params in self.checkpoint_sql(stream, seq, to_pos, batch_id, ts, prev):
                        idem(tx)
                        tx.execute(sql, params)                  # stream COMMIT POINT (conditional)
            finally:
                tx.execute("RESET spark.databricks.delta.write.txnAppId")
                tx.execute("RESET spark.databricks.delta.write.txnVersion")
        if stream is not None and to_pos is not None:   # read back: our row, or another writer won
            got = self.control_state(stream)
            if not (got.seq == seq and got.position == to_pos.text and got.batch_id == batch_id):
                if self.pos_ge(got.position, to_pos) and got.batch_id == batch_id:
                    return
                raise CheckpointConflictError(stream, prev.position, got.position,
                                              "conditional checkpoint MERGE matched no row (concurrent writer)")

    def table_ddl(self, table: TableInfo, dest: str) -> list[str]:
        return super().table_ddl(table, dest)

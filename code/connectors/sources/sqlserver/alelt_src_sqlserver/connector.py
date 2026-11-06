"""SqlServerSource: remote capture through SQL Server Change Data Capture (documented cdc.* functions), python-tds.

Contract: connectors/sources/_spi/REMOTE.md. ``SqlServerSource()`` = decode-only; ``SqlServerSource(settings=,
password=, connection_id=)`` = live (connects lazily).

Settings: host, port (1433), database, user, schemas (dbo), tables (optional), poll_s (1.0), window_txns (5000),
server_tz (IANA name, default: current server offset), start_pos (first start when the hub has no
batch: e.g. the snapshot start position; default = current end), connect_timeout_s (15), call_timeout_s (300),
dev_manual_scan (false - DEV / TEST ONLY: run ``sys.sp_cdc_scan`` before each read on servers without SQL Server
Agent, e.g. Azure SQL Edge; never enable it on a production source).
"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import tzinfo
from typing import Any

from alelt_common import completeness as FC
from alelt_common.models import CanonicalType, CatalogSnapshot, ChangeEvent, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.spi import DecodeResult, SourceConnector

from . import types as T
from .assemble import CaptureInstance, assemble
from .position import MAX, ZERO, Lsn, compare_lsn

log = logging.getLogger("alelt_src_sqlserver")
ENGINE = "sqlserver"


def _bool(v: Any) -> bool:
    return v is True or str(v).strip().lower() in ("1", "true", "yes", "on")


class SqlServerSource(SourceConnector):
    engine = ENGINE
    capture_mode = "remote"
    decoder_version = "sqlserver-1"   # HC-02: bump on ANY change of decoded output (replay cache key)
    display_name = "SQL Server (CDC)"
    connection_fields = [
        {"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True, "default": 1433},
        {"name": "database", "label": "Database", "required": True},
        {"name": "user", "label": "User", "required": True},
        {"name": "password", "label": "Password", "secret": True, "required": True},
        {"name": "schemas", "label": "Schemas (comma separated)", "required": True, "default": "dbo"},
        {"name": "tables", "label": "Tables (optional, SCHEMA.TABLE comma separated)", "required": False,
         "default": ""},
        {"name": "dev_manual_scan", "label": "DEV/TEST ONLY: run sp_cdc_scan before each read (no SQL Agent)",
         "required": False, "default": False},
    ]

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str = "") -> None:
        self.settings = dict(settings or {})
        self.password = password if password is not None else self.settings.get("password")
        self.connection_id = connection_id or str(self.settings.get("connection_id") or "sqlserver")
        self._conn: Any = None
        self._instances: dict[str, CaptureInstance] | None = None
        self._snapshot_iso: bool | None = None
        self.poll_s = float(self.settings.get("poll_s", 1.0))
        self.window_txns = int(self.settings.get("window_txns", 5000))
        self.dev_manual_scan = _bool(self.settings.get("dev_manual_scan", False))

    # ------------------------------------------------------------------ positions / types / decode
    def parse_position(self, text: str) -> Position:
        return Position(engine=ENGINE, text=Lsn.parse(text).text)

    def compare(self, a: Position, b: Position) -> int:
        return compare_lsn(a.text, b.text)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        t = col.engine_type.lower().removesuffix("(max)")
        prec = col.length if t == "float" else None
        return CanonicalType(T.canonical_name(t, prec, col.scale))

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        from alelt_common.canonical_jsonl import decode_batch

        return decode_batch(raw, self.engine, batch_id)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------ connection
    @property
    def schemas(self) -> list[str]:
        s = self.settings.get("schemas") or "dbo"
        lst = s if isinstance(s, list) else str(s).split(",")
        return [x.strip() for x in lst if x.strip()]

    @property
    def table_filter(self) -> list[str] | None:
        t = self.settings.get("tables") or ""
        lst = t if isinstance(t, list) else str(t).split(",")
        out = [x.strip().upper() for x in lst if x.strip()]
        return out or None

    def _connect(self) -> Any:
        if self._conn is None:
            import pytds   # python-tds (MIT), pure Python

            st = self.settings
            self._conn = pytds.connect(
                server=st.get("host", "localhost"), port=int(st.get("port", 1433)), database=st.get("database"),
                user=st.get("user"), password=self.password, autocommit=True,
                login_timeout=float(st.get("connect_timeout_s", 15)), timeout=float(st.get("call_timeout_s", 300)),
                appname="alelt-capture")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def ack(self, pos: Position) -> None:
        """Nothing to release: CDC retention is owned by the cleanup job (DBA); ALELT never deletes changes."""

    def _server_tz(self) -> tzinfo | None:
        name = self.settings.get("server_tz")
        if not name:
            return None
        from zoneinfo import ZoneInfo

        return ZoneInfo(str(name))

    def _reader(self) -> Any:
        from .cdc import CdcReader

        if self._instances is None:
            self.read_catalog()
        return CdcReader(self._connect(), self._instances or {}, dev_manual_scan=self.dev_manual_scan,
                         window_txns=self.window_txns, server_tz=self._server_tz())

    # ------------------------------------------------------------------ catalog / snapshot
    def read_catalog(self) -> CatalogSnapshot:
        from .catalog import read_catalog

        conn = self._connect()
        pos = None
        try:
            cur = conn.cursor()
            cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
            r = cur.fetchone()
            if r and r[0] is not None:
                pos = Position(engine=ENGINE, text=Lsn(bytes(r[0]), MAX).text)
        except Exception:  # noqa: BLE001 - database not CDC-enabled
            pass
        cat, inst = read_catalog(conn, self.schemas, self.connection_id, pos, self.table_filter)
        self._instances = inst
        return cat

    def read_catalog_remote(self, settings: dict[str, Any], password: str | None, schemas: list[str],
                            connection_id: str) -> CatalogSnapshot:
        st = dict(settings)
        if schemas:
            st["schemas"] = schemas
        src = SqlServerSource(settings=st, password=password, connection_id=connection_id)
        try:
            return src.read_catalog()
        finally:
            src.close()

    def current_position(self) -> Position | None:
        """End of the captured log (commit token of fn_cdc_get_max_lsn); a snapshot started after it may overlap
        the stream, the overlap is replayed idempotently (full row images)."""
        rd = self._reader()
        if self.dev_manual_scan:
            rd.scan()
        return Position(engine=ENGINE, text=Lsn(rd.max_lsn(), MAX).text)

    def snapshot_rows(self, table: TableInfo, key_range: tuple[Any, Any] | None,
                      chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        from .catalog import snapshot_rows

        conn = self._connect()
        if self._snapshot_iso is None:
            cur = conn.cursor()
            cur.execute("SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()")
            r = cur.fetchone()
            self._snapshot_iso = bool(r and int(r[0]) == 1)
            if not self._snapshot_iso:
                log.warning("ALLOW_SNAPSHOT_ISOLATION is OFF: initial load reads READ COMMITTED chunks",
                            extra={"component": "alelt_src_sqlserver"})
        return snapshot_rows(conn, table, key_range, chunk_rows, snapshot_isolation=self._snapshot_iso)

    # ------------------------------------------------------------------ capture
    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:  # type: ignore[override]
        rd = self._reader()
        if from_pos is None and self.settings.get("start_pos"):
            from_pos = self.parse_position(str(self.settings["start_pos"]))   # REMOTE.md §5: settings.start_pos
        if from_pos is None:
            if self.dev_manual_scan:
                rd.scan()
            after = rd.max_lsn()
        else:
            p = Lsn.parse(from_pos.text)
            # commit token <lsn>:FF.. -> after lsn; any other position <lsn>:<seq> -> from lsn (re-deliver its txn)
            after = p.lsn if p.is_commit_token else (int.from_bytes(p.lsn, "big") - 1).to_bytes(10, "big")
        rd.check_retention(after)
        log.info("sqlserver capture after LSN %s", after.hex().upper(), extra={"component": "alelt_src_sqlserver"})
        refresh_at = time.monotonic() + float(self.settings.get("catalog_s", 60))
        late = self._late_instances(rd.instances, after, resumed=from_pos is not None)
        while True:
            end, rows, meta = rd.read(after)
            if end is None:
                yield None
                time.sleep(min(self.poll_s, 1.0))
                if time.monotonic() >= refresh_at:     # new capture instance (table enabled / re-enabled)
                    self.read_catalog()
                    rd.instances = self._instances or {}
                    late += self._late_instances(rd.instances, after, resumed=True)
                    refresh_at = time.monotonic() + float(self.settings.get("catalog_s", 60))
                continue
            rd.check_retention(after)                  # cleanup may have run since the previous window
            evs = assemble(rows, rd.instances, meta, rd.server_tz, census=self.census, pre_signals=late)
            if rows:
                late = []
            for ev in evs:
                yield ev
            after = end
            yield None
            if time.monotonic() >= refresh_at:         # SRC-ms-01: also on a continuously busy source
                self.read_catalog()
                rd.instances = self._instances or {}
                late += self._late_instances(rd.instances, after, resumed=True)
                refresh_at = time.monotonic() + float(self.settings.get("catalog_s", 60))

    def _late_instances(self, instances: dict[str, Any], after: bytes, resumed: bool) -> list[FC.SourceSignal]:
        """A capture instance whose start_lsn is after the resume position and that is the first instance of its
        table (table enabled / disabled + re-enabled while capturing): CDC has no changes of that table between the
        resume position and start_lsn -> hold + resync the table (never "nothing was missed" by assumption)."""
        from .position import increment

        out: list[FC.SourceSignal] = []
        seen = self.__dict__.setdefault("_late_seen", set())
        tables_with_older = {(ci.table.table.schema_, ci.table.table.name) for ci in instances.values()
                             if ci.valid_to is not None}
        for name, ci in sorted(instances.items()):
            key = (ci.table.table.schema_, ci.table.table.name)
            if not resumed or name in seen or ci.start_lsn is None or not any(after):
                continue
            if ci.start_lsn > increment(after) and key not in tables_with_older:
                seen.add(name)
                out.append(FC.unsupported(ci.table.table, f"capture instance {name} starts at LSN "
                                          f"{ci.start_lsn.hex().upper()} after the resume position "
                                          f"{after.hex().upper()}: changes before it are not in CDC (table "
                                          "enabled or re-enabled while capturing) - resync", "cdc:instance"))
                self.census.count("cdc:instance", FC.RecordClass.UNSUPPORTED, "capture instance starts after resume")
        return out

    def keyed_reread(self, table: TableInfo, keys: list[dict[str, Any]]) -> FC.KeyedReread:
        """Current rows by primary key in ONE read: SNAPSHOT isolation when ALLOW_SNAPSHOT_ISOLATION is ON, else READ
        COMMITTED (one statement per <= 200 keys); `read_from` = fn_cdc_get_max_lsn() taken before the read."""
        from .catalog import _sel, qi

        kcols = [c.name for c in table.key_columns()]
        if not kcols:
            raise NotImplementedError(f"{table.table.fqn}: no primary key - keyed re-read impossible (resync)")
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()")
        r = cur.fetchone()
        snap = bool(r and int(r[0]) == 1)
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        r = cur.fetchone()
        read_from = Position(engine=ENGINE, text=Lsn(bytes(r[0]), MAX).text) if r and r[0] is not None else None
        cols = sorted(table.columns, key=lambda c: c.ordinal)
        src = f"{qi(table.table.schema_)}.{qi(table.table.name)}"
        found: dict[tuple[Any, ...], dict[str, Any]] = {}
        stmts = []
        if snap:
            cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            cur.execute("BEGIN TRANSACTION")
        try:
            for i in range(0, len(keys), 200):
                part = keys[i:i + 200]
                where = " OR ".join("(" + " AND ".join(f"{qi(c)} = %s" for c in kcols) + ")" for _ in part)
                cur.execute(f"SELECT {', '.join(_sel(c) for c in cols)} FROM {src} WHERE {where}",
                            tuple(k[c] for k in part for c in kcols))
                for row in cur.fetchall():
                    out = {c.name: T.convert(v, c.canonical_type.value, c.scale) for c, v in zip(cols, row)}
                    found[tuple(out[c] for c in kcols)] = out
                stmts.append(len(part))
        finally:
            if snap:
                cur.execute("COMMIT")
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        rows, missing = [], []
        for k in keys:
            row = found.get(tuple(k[c] for c in kcols))
            (rows if row is not None else missing).append(row if row is not None else k)
        return FC.KeyedReread(rows=rows, missing=missing, read_from=read_from,
                              isolation="SNAPSHOT" if snap else "READ COMMITTED")

    # ------------------------------------------------------------------ prerequisites
    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        from .prereq import checks

        st = dict(self.settings)
        st.update(settings or {})
        conn = None
        if st.get("host"):
            try:
                if settings and not self._conn:
                    self.settings.update(settings)
                    if settings.get("password") and self.password is None:
                        self.password = settings["password"]
                conn = self._connect()
            except Exception as e:  # noqa: BLE001
                log.warning("prerequisite check could not connect: %s", e, extra={"component": "alelt_src_sqlserver"})
        resume = probes.get("resume_lsn") if isinstance(probes, dict) else None
        rl = Lsn.parse(str(resume)).lsn if resume else None
        return checks(conn, catalog, tables, st, rl)

    def prerequisites(self) -> list[str]:
        return ["CDC enabled on the database (sys.sp_cdc_enable_db)",
                "CDC enabled on each captured table (sys.sp_cdc_enable_table)",
                "SQL Server Agent running (CDC capture job)",
                "capture user: SELECT on captured tables + cdc schema, member of the gating role if any",
                "ALLOW_SNAPSHOT_ISOLATION ON for a consistent initial load",
                "cleanup retention covers the longest hub outage"]


__all__ = ["SqlServerSource", "ZERO"]

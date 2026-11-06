"""OracleSource: remote capture with LogMiner (DBMS_LOGMNR, DICT_FROM_ONLINE_CATALOG), python-oracledb thin mode.

Contract: connectors/sources/_spi/REMOTE.md. ``OracleSource()`` = decode-only; ``OracleSource(settings=,
password=, connection_id=)`` = live (connects lazily).
"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

from alelt_common.models import CanonicalType, CatalogSnapshot, ChangeEvent, ChangeKind, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.completeness import KeyedReread
from alelt_common.spi import DecodeResult, SourceConnector

from . import types as T
from .assemble import Assembler
from .position import Scn, compare_scn

log = logging.getLogger("alelt_src_oracle")
ENGINE = "oracle"


class OracleSource(SourceConnector):
    engine = ENGINE
    capture_mode = "remote"
    decoder_version = "oracle-1"   # HC-02: bump on ANY change of decoded output (replay cache key)
    display_name = "Oracle (LogMiner)"
    connection_fields = [
        {"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True, "default": 1521},
        {"name": "service_name", "label": "Service name (PDB or non-CDB)", "required": True},
        {"name": "user", "label": "User", "required": True},
        {"name": "password", "label": "Password", "secret": True, "required": True},
        {"name": "schemas", "label": "Schemas (comma separated)", "required": True},
        {"name": "tables", "label": "Tables (optional, SCHEMA.TABLE comma separated)", "required": False,
         "default": ""},
    ]

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str = "") -> None:
        self.settings = dict(settings or {})
        self.password = password if password is not None else self.settings.get("password")
        self.connection_id = connection_id or str(self.settings.get("connection_id") or "oracle")
        self._conn: Any = None
        self._snapshot_scn: int | None = None
        self._tables: dict[tuple[str, str], TableInfo] | None = None
        self.poll_s = float(self.settings.get("poll_s", 1.0))
        self.flush_lag_s = float(self.settings.get("flush_lag_s", 5.0))
        self.window_scns = int(self.settings.get("window_scns", 50_000))
        # catch-up windows grow up to max_window_scns while the source is far ahead and a window is quick (fewer
        # START_LOGMNR calls per hour of redo, SRC-ora-02); back to window_scns once caught up
        self.max_window_scns = max(self.window_scns, int(self.settings.get("max_window_scns", 2_000_000)))
        self.long_txn_alert_s = float(self.settings.get("long_txn_alert_s", 3600))
        self._alerted_xid: str | None = None

    # ------------------------------------------------------------------ positions / types / decode
    def parse_position(self, text: str) -> Position:
        return Position(engine=ENGINE, text=Scn.parse(text).text)

    def compare(self, a: Position, b: Position) -> int:
        return compare_scn(a.text, b.text)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        prec = col.length if T.base_type(col.engine_type) == "NUMBER" else None
        if T.base_type(col.engine_type) == "NUMBER" and col.length == 38 and col.scale is None:
            prec = None
        return CanonicalType(T.canonical_name(col.engine_type, prec, col.scale))

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        from alelt_common.canonical_jsonl import decode_batch

        return decode_batch(raw, self.engine, batch_id)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------ connection
    @property
    def schemas(self) -> list[str]:
        s = self.settings.get("schemas") or ""
        lst = s if isinstance(s, list) else str(s).split(",")
        return [x.strip().upper() for x in lst if x.strip()]

    @property
    def table_filter(self) -> list[str] | None:
        t = self.settings.get("tables") or ""
        lst = t if isinstance(t, list) else str(t).split(",")
        out = [x.strip().upper() for x in lst if x.strip()]
        return out or None

    def _connect(self) -> Any:
        if self._conn is None:
            import oracledb   # thin mode: no Oracle client libraries needed

            st = self.settings
            dsn = st.get("dsn") or f"{st.get('host', 'localhost')}:{int(st.get('port', 1521))}/{st.get('service_name')}"
            self._conn = oracledb.connect(user=st.get("user"), password=self.password, dsn=dsn,
                                          tcp_connect_timeout=float(st.get("connect_timeout_s", 15)))
            self._conn.call_timeout = int(float(st.get("call_timeout_s", 300)) * 1000)
            cur = self._conn.cursor()
            from .logminer import SESSION_NLS
            for s in SESSION_NLS:
                cur.execute(s)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def ack(self, pos: Position) -> None:
        """Nothing to release: redo/archive retention is owned by the DBA (ALELT never deletes logs)."""

    # ------------------------------------------------------------------ catalog / snapshot
    def read_catalog(self) -> CatalogSnapshot:
        from .catalog import read_catalog

        conn = self._connect()
        pos = None
        try:
            pos = Position(engine=ENGINE, text=str(self._one("SELECT CURRENT_SCN FROM V$DATABASE")))
        except Exception:  # noqa: BLE001
            pass
        cat = read_catalog(conn, self.schemas, self.connection_id, pos, self.table_filter)
        self._tables = {(t.table.schema_, t.table.name): t for t in cat.tables}
        return cat

    def read_catalog_remote(self, settings: dict[str, Any], password: str | None, schemas: list[str],
                            connection_id: str) -> CatalogSnapshot:
        st = dict(settings)
        if schemas:
            st["schemas"] = schemas
        src = OracleSource(settings=st, password=password, connection_id=connection_id)
        try:
            return src.read_catalog()
        finally:
            src.close()

    def _one(self, sql: str) -> Any:
        cur = self._connect().cursor()
        cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r else None

    def current_position(self) -> Position | None:
        """Current SCN + restart SCN of the oldest open transaction (so a snapshot start position is safe).

        Read order matters: `low` = CURRENT_SCN BEFORE V$TRANSACTION, `scn` = CURRENT_SCN after it. A transaction that
        started before `low` and committed before the V$TRANSACTION read is committed in a snapshot AS OF `scn`; one
        still open at that read is in MIN(START_SCN); one that started later starts at or after `low`. Mining from
        min(oldest, low) therefore sees the START of every transaction that commits after `scn` (no partial txn)."""
        low = int(self._one("SELECT CURRENT_SCN FROM V$DATABASE"))
        oldest = self._one("SELECT MIN(START_SCN) FROM V$TRANSACTION")
        scn = int(self._one("SELECT CURRENT_SCN FROM V$DATABASE"))
        restart = min(int(oldest), low) if oldest is not None else low
        self._snapshot_scn = scn
        return Position(engine=ENGINE, text=Scn(scn, "", restart).text)

    def _resolve_obj(self, obj: int) -> tuple[str, str] | None:
        """Object id from a LogMiner 'OBJ# n' row -> (owner, table) in the CURRENT dictionary (None = dropped)."""
        cur = self._connect().cursor()
        cur.execute("SELECT owner, object_name FROM all_objects WHERE (object_id = :o OR data_object_id = :o) "
                    "AND object_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION') "
                    "FETCH FIRST 1 ROWS ONLY", {"o": obj})
        r = cur.fetchone()
        return (str(r[0]), str(r[1])) if r else None

    def keyed_reread(self, table: TableInfo, keys: list[dict[str, Any]]) -> KeyedReread:
        """Current rows by primary key, all keys read AS OF one SCN (consistent by construction);
        `read_from` = that SCN as a commit token (exact)."""
        from .catalog import _select_expr, output_type_handler, q

        kcols = [c.name for c in table.key_columns()]
        if not kcols:
            raise NotImplementedError(f"{table.table.fqn}: no primary key - keyed re-read impossible (resync)")
        scn = int(self._one("SELECT CURRENT_SCN FROM V$DATABASE"))
        cols = sorted(table.columns, key=lambda c: c.ordinal)
        sel = ", ".join(f"{_select_expr(c)} AS {q(c.name)}" for c in cols)
        cur = self._connect().cursor()
        cur.outputtypehandler = output_type_handler
        found: dict[tuple[Any, ...], dict[str, Any]] = {}
        for i in range(0, len(keys), 200):
            part = keys[i:i + 200]
            binds: dict[str, Any] = {"scn": scn}
            ors = []
            for j, k in enumerate(part):
                ors.append("(" + " AND ".join(f"{q(c)} = :k{j}_{n}" for n, c in enumerate(kcols)) + ")")
                binds.update({f"k{j}_{n}": k[c] for n, c in enumerate(kcols)})
            cur.execute(f"SELECT {sel} FROM {q(table.table.schema_)}.{q(table.table.name)} AS OF SCN :scn "
                        f"WHERE {' OR '.join(ors)}", binds)
            names = [d[0] for d in cur.description]
            for r in cur.fetchall():
                raw = dict(zip(names, r))
                row = {c.name: T.convert_driver(raw[c.name], c.canonical_type.value, c.scale) for c in cols}
                found[tuple(row[c] for c in kcols)] = row
        rows, missing = [], []
        for k in keys:
            row = found.get(tuple(k[c] for c in kcols))
            (rows if row is not None else missing).append(row if row is not None else k)
        return KeyedReread(rows=rows, missing=missing, read_from=Position(engine=ENGINE, text=Scn(scn, "").text),
                           isolation="AS OF SCN (flashback query)")

    def snapshot_rows(self, table: TableInfo, key_range: tuple[Any, Any] | None,
                      chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        from .catalog import snapshot_rows

        if self._snapshot_scn is None:
            self.current_position()
        return snapshot_rows(self._connect(), table, key_range, chunk_rows, int(self._snapshot_scn or 0))

    # ------------------------------------------------------------------ capture
    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:  # type: ignore[override]
        from .logminer import Miner

        conn = self._connect()
        if self._tables is None:
            self.read_catalog()
        miner = Miner(conn, self.schemas, flush_lag_s=self.flush_lag_s, window_scns=self.window_scns)
        miner.ordered = miner.ordered or bool(self.settings.get("ordered_mining")) or getattr(self, "_ordered", False)
        if self.settings.get("server_tz"):
            # SRC-ora-05: V$LOGMNR_CONTENTS.TIMESTAMP is server-local; an IANA zone converts every commit with the
            # offset valid at that commit (DST-safe). Without it: the server's current offset (documented limit).
            from zoneinfo import ZoneInfo
            miner.db_tz = ZoneInfo(str(self.settings["server_tz"]))
        if from_pos is None:
            p = self.current_position()
            assert p is not None
            pos = Scn.parse(p.text)
        else:
            pos = Scn.parse(from_pos.text)
        asm = Assembler(self._tables or {}, self.schemas, skip=pos, db_tz=miner.db_tz,
                        max_lookback_scns=int(self.settings.get("max_lookback_scns") or 0),
                        empty_commit_every_s=float(self.settings.get("empty_commit_every_s", 5.0)),
                        census=self.census, resolve_obj=self._resolve_obj)
        next_start = pos.mine_from
        log.info("oracle capture from %s (mining from SCN %s)", pos.text, next_start,
                 extra={"component": "alelt_src_oracle"})
        try:
            yield from self._mine(miner, asm, next_start)
        finally:
            self._ordered = miner.ordered       # an SCN regression seen once keeps the ordered query (SRC-ora-07)
            miner.close()

    def next_window(self, cur: int, start: int, end: int, safe: int, seconds: float) -> int:
        """Window size after mining [start, end] in `seconds` with the safe SCN at `safe`."""
        if end >= safe:
            return self.window_scns                    # caught up: small windows, low latency
        if seconds < 10.0:
            return min(self.max_window_scns, cur * 2)  # catching up and cheap: fewer, larger sessions
        if seconds > 60.0:
            return max(self.window_scns, cur // 2)
        return cur

    def _mine(self, miner: Any, asm: Assembler, next_start: int) -> Iterator[ChangeEvent | None]:
        win = self.window_scns
        while True:
            safe = miner.safe_scn()
            if safe < next_start:
                yield None
                time.sleep(min(self.poll_s, 1.0))
                continue
            end = min(safe, next_start + win)
            tick = began = time.monotonic()
            ddl = False
            for row in miner.window(next_start, end):
                for ev in asm.feed(row):
                    ddl = ddl or ev.kind == ChangeKind.DDL
                    if self._captured(ev):
                        yield ev
                if time.monotonic() - tick >= 1.0:
                    tick = time.monotonic()
                    yield None
            for ev in asm.flush():
                ddl = ddl or ev.kind == ChangeKind.DDL
                if self._captured(ev):
                    yield ev
            if ddl:
                self.read_catalog()
                asm.tables = self._tables or {}
            win = self.next_window(win, next_start, end, safe, time.monotonic() - began)
            old = asm.oldest_open()
            if old is not None and old[1] >= self.long_txn_alert_s and old[0] != self._alerted_xid:
                self._alerted_xid = old[0]             # SRC-ora-06: one alert per long-open transaction
                log.error("oracle: transaction %s open for %.0f s: every restart mines the redo since its start "
                          "(set max_lookback_scns to bound it)", old[0], old[1],
                          extra={"component": "alelt_src_oracle", "correlation_id": old[0]})
            next_start = end + 1
            yield None
            if end >= safe:
                time.sleep(min(self.poll_s, 1.0))

    _ROW_KINDS = (ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE, ChangeKind.TRUNCATE)

    def _captured(self, ev: ChangeEvent) -> bool:
        """LogMiner mines whole schemas: row changes of tables outside the captured set are not delivered
        (catalog ``capture_enabled`` false = no (ALL) COLUMNS supplemental logging, or outside ``tables``), the same
        as a table outside the publication / CDC set. DDL always passes; OTHER (quarantine) passes unless its table
        is known to be outside the captured set (e.g. an UPDATE of a table without supplemental logging, whose redo
        carries only the ROWID)."""
        if ev.table is None or (ev.kind not in self._ROW_KINDS and ev.kind != ChangeKind.OTHER):
            return True
        key = (ev.table.schema_.upper(), ev.table.name.upper())
        info = (self._tables or {}).get(key)
        if info is not None:
            return bool(info.capture_enabled)
        flt = self.table_filter
        if flt is not None:
            return f"{key[0]}.{key[1]}" in flt
        return True          # uncataloged (e.g. created after the last catalog read): delivered, never dropped

    # ------------------------------------------------------------------ prerequisites
    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        from .prereq import checks

        st = dict(self.settings)
        st.update(settings or {})
        conn = None
        if st.get("host") or st.get("dsn"):
            try:
                if settings and not self._conn:
                    self.settings.update(settings)
                conn = self._connect()
            except Exception as e:  # noqa: BLE001
                log.warning("prerequisite check could not connect: %s", e)
        resume = probes.get("resume_scn") if isinstance(probes, dict) else None
        return checks(conn, catalog, tables, st, int(resume) if resume is not None else None)

    def prerequisites(self) -> list[str]:
        return ["ARCHIVELOG mode", "minimal supplemental logging (ALTER DATABASE ADD SUPPLEMENTAL LOG DATA)",
                "supplemental logging (ALL) COLUMNS on captured tables",
                "capture user: LOGMINING, SELECT ANY TRANSACTION, SELECT_CATALOG_ROLE, EXECUTE DBMS_LOGMNR, "
                "SELECT + FLASHBACK on captured tables",
                "archived logs retained for the longest hub outage"]

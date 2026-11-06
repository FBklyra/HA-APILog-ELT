"""MySQLSource / MariaDBSource: remote capture over the binlog replication protocol (connectors/sources/_spi/REMOTE.md).

Settings (wizard / pipeline_definition.source.settings):
  host, port (3306), user, databases (list or comma string; alias: schemas, database) = captured databases,
  tables (optional list / comma string of 'db.table'; default: every base table of the databases),
  server_id (replica id used for the binlog dump; default derived from the connection id),
  start_pos (first start only, when the hub has no durable batch yet: e.g. the position recorded before the initial
  snapshot; default = current end of the binlog, REMOTE.md S5),
  heartbeat_s (1.0), empty_commit_s (30), connect_timeout (10), ssl / ssl_ca.
Client: PyMySQL (MIT) for handshake / auth / packet I/O only - `pip install PyMySQL`.

Positions: '<gtid-state>@<file>:<pos>' (position.py). COMMIT positions are resume tokens: a MySQL GTID set or a
MariaDB GTID list is a set of transactions, so stream(p) asks the server for exactly the transactions not in p.
"""
from __future__ import annotations

import logging
import re
import time
import zlib
from collections.abc import Iterator
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal
from typing import Any

from alelt_common.canonical_jsonl import Undecodable, decode_batch
from alelt_common.completeness import KeyedReread
from alelt_common.models import CanonicalType, CatalogSnapshot, ChangeEvent, ChangeKind, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.spi import DecodeResult, SourceConnector

from . import catalog as C
from . import protocol as P
from . import types as T
from .assemble import TxnAssembler
from .decode import BinlogDecoder, TableSpec, specs_from_catalog
from .position import MARIADB, MYSQL, BinlogPosition, compare_positions
from .rows import float32, json_text
from .values import time_of_day

log = logging.getLogger("alelt_src_mysql")


class PrerequisiteError(RuntimeError):
    pass


def _q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _lit(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def _list(v: Any) -> list[str]:
    if v is None or v == "":
        return []
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return [str(x).strip() for x in v if str(x).strip()]


_SHAPE_DDL = re.compile(r"^\s*(?:/\*.*?\*/\s*)*(?:CREATE|ALTER|DROP|RENAME)(?![A-Za-z0-9_])", re.I | re.S)


def shape_changing(note: str | None) -> bool:
    """DDL statement text that can change a table shape (ANALYZE / OPTIMIZE / GRANT / FLUSH ... cannot)."""
    return note is None or bool(_SHAPE_DDL.match(note))


# ---------------------------------------------------------------------------------------------- snapshot values
def snapshot_value(col: ColumnInfo, v: Any) -> Any:
    """SELECT value (PyMySQL, session time_zone +00:00) -> the same canonical value the binlog decoder yields."""
    if v is None:
        return None
    ct = col.canonical_type.value if isinstance(col.canonical_type, CanonicalType) else str(col.canonical_type)
    base = T.base_type(col.engine_type)
    if ct == "BOOLEAN":
        return int.from_bytes(v, "big") != 0 if isinstance(v, (bytes, bytearray)) else bool(v)
    if ct in ("SHORT", "INT", "LONG"):
        return int(v)
    if ct == "DECIMAL":
        return v if isinstance(v, Decimal) else Decimal(str(v))
    if ct == "FLOAT":
        return float32(float(v))
    if ct == "DOUBLE":
        return float(v)
    if ct == "LOCALDATE":
        return v if isinstance(v, date) and not isinstance(v, datetime) else None      # zero date -> NULL
    if ct == "LOCALDATETIME":
        return v if isinstance(v, datetime) else None
    if ct == "INSTANT":
        return v.replace(tzinfo=timezone.utc) if isinstance(v, datetime) else None
    if ct == "LOCALTIME":
        if isinstance(v, timedelta):
            tod = time_of_day(v)
            return tod if tod is not None else Undecodable(f"TIME {v} outside 00:00:00..23:59:59 (LOCALTIME)")
        return v if isinstance(v, dtime) else Undecodable(str(v))
    if ct == "JSON":
        s = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
        import json
        try:
            return json_text(json.loads(s))
        except ValueError:
            return Undecodable(s[:2000])
    if ct == "BINARY":
        return bytes(v) if isinstance(v, (bytes, bytearray, memoryview)) else str(v).encode("utf-8")
    if ct == "STRING":
        if isinstance(v, (bytes, bytearray)):
            codec = T.codec_for(col.codepage) or "utf-8"
            try:
                return bytes(v).decode(codec)
            except UnicodeDecodeError:
                return Undecodable(bytes(v).hex())
        if base == "time" and isinstance(v, timedelta):
            from .rows import _fmt_duration
            return _fmt_duration(v)                      # same text as the binlog decoder (SRC-my-05)
        return str(v)
    return Undecodable(f"{col.engine_type}: {v!r}"[:2000])


# ---------------------------------------------------------------------------------------------- connector
class _BinlogSource(SourceConnector):
    flavor: str = MYSQL
    engine = MYSQL                   # = flavor (subclasses set both)
    capture_mode = "remote"
    decoder_version = "binlog-1"   # HC-02: bump on ANY change of decoded output (replay cache key)
    connection_fields = [
        {"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True, "default": 3306},
        {"name": "user", "label": "User (REPLICATION SLAVE, REPLICATION CLIENT, SELECT)", "required": True},
        {"name": "password", "label": "Password", "secret": True, "required": False},
        {"name": "databases", "label": "Databases (comma separated)", "required": True},
        {"name": "tables", "label": "Tables (db.table, comma separated; empty = all)", "required": False,
         "default": ""},
        {"name": "server_id", "label": "Replica server id (unique on the source)", "required": False,
         "default": ""},
        {"name": "ssl", "label": "Use TLS", "required": False, "default": False},
    ]

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str = "") -> None:
        self.settings = dict(settings or {})
        self.password = password
        self.cid = connection_id or str(self.settings.get("connection_id") or self.flavor)
        self._meta: Any = None
        self._client: P.BinlogClient | None = None
        self._dec: BinlogDecoder | None = None
        self._specs: dict[tuple[str, str], TableSpec] = {}
        self._cols: dict[tuple[str, str], list[str]] = {}
        self._acked: Position | None = None
        self.snapshot_positions: dict[str, dict[str, Any]] = {}   # fqn -> {position, exact}

    # ------------------------------------------------------------------ settings
    def _schemas(self) -> list[str]:
        st = self.settings
        out = _list(st.get("databases") or st.get("schemas") or st.get("database"))
        if not out:
            out = sorted({t.split(".", 1)[0].strip("`") for t in _list(st.get("tables")) if "." in t})
        return out

    def _table_filter(self) -> set[tuple[str, str]] | None:
        tabs = _list(self.settings.get("tables"))
        if not tabs:
            return None
        out = set()
        for t in tabs:
            s, dot, n = t.partition(".")
            if not dot:
                raise PrerequisiteError(f"table {t!r}: expected 'database.table'")
            out.add(self._nk(s.strip("`"), n.strip("`")))
        return out

    def _nk(self, schema: str, table: str) -> tuple[str, str]:
        """Name key: exact on a case-sensitive server (lower_case_table_names = 0, SRC-my-11), else lower-case."""
        if getattr(self, "case_sensitive", False):
            return schema, table
        return schema.lower(), table.lower()

    @property
    def server_id(self) -> int:
        sid = self.settings.get("server_id")
        if sid not in (None, ""):
            return int(sid)  # type: ignore[arg-type]
        return 0x40000000 + zlib.crc32(f"alelt:{self.cid}".encode()) % 0x3FFFFFFF

    def _meta_conn(self) -> Any:
        if self._meta is None or not getattr(self._meta, "open", False):
            if not self.settings.get("host"):
                raise PrerequisiteError(f"{self.flavor} source needs settings host + databases")
            conn = P.connect(self.settings, self.password, read_timeout=60)
            fl = P.detect_flavor(conn)
            if fl != self.flavor:
                conn.close()
                raise PrerequisiteError(f"source at {self.settings.get('host')}:{self.settings.get('port')} is "
                                        f"{fl}, connector is {self.flavor} - pick engine '{fl}'")
            with conn.cursor() as c:
                c.execute("SET SESSION TRANSACTION READ ONLY")
            self._meta = conn
        return self._meta

    # ------------------------------------------------------------------ positions / types / decode
    def parse_position(self, text: str) -> Position:
        return Position(engine=self.flavor, text=BinlogPosition.parse(text, self.flavor).text)

    def compare(self, a: Position, b: Position) -> int:
        return compare_positions(BinlogPosition.parse(a.text, self.flavor), BinlogPosition.parse(b.text, self.flavor))

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        if self.settings.get("time_as_string") and T.base_type(col.engine_type) == "time":
            return CanonicalType("STRING")
        n = T.canonical_name(col.engine_type, col.length if T.base_type(col.engine_type) == "bit" else None)
        return CanonicalType(n)

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        return decode_batch(raw, self.flavor, batch_id)

    # ------------------------------------------------------------------ catalog
    def read_catalog(self) -> CatalogSnapshot:
        conn = self._meta_conn()
        schemas = self._schemas()
        if not schemas:
            raise PrerequisiteError("settings.databases is empty: list the databases to capture")
        pos = P.current_position(conn, self.flavor) if self._binlog_on(conn) else None
        with conn.cursor() as c:
            c.execute("SELECT @@lower_case_table_names")
            r = c.fetchone()
        self.case_sensitive = r is not None and str(r[0]) == "0"     # SRC-my-11
        d = C.read_catalog(conn, schemas, self.cid, self.flavor,
                           position={"engine": self.flavor, "text": pos.text} if pos else None)
        return self._apply_catalog(d)

    def _apply_catalog(self, d: dict[str, Any]) -> CatalogSnapshot:
        """Catalog dict (catalog.read_catalog) -> CatalogSnapshot; captured tables become the decoder specs."""
        want = self._table_filter()
        if self.settings.get("time_as_string"):
            # SRC-my-05: MySQL TIME is a signed duration (-838:59:59..838:59:59); as LOCALTIME every value outside
            # 00:00:00..23:59:59 is a quarantined row. time_as_string: TIME -> STRING '[-]HH:MM:SS[.ffffff]'
            for t in d["tables"]:
                for c in t["columns"]:
                    if T.base_type(c["engine_type"]) == "time":
                        c["canonical_type"] = "STRING"
        for t in d["tables"]:
            key = self._nk(t["table"]["schema"], t["table"]["name"])
            if want is not None and key not in want:
                t["capture_enabled"] = False
        snap = CatalogSnapshot.model_validate(d)
        cap = {"tables": [t for t in d["tables"] if t["capture_enabled"]]}
        self._specs = specs_from_catalog(cap)
        self._cols = {self._nk(s, n): [c.name for c in sp.columns] for (s, n), sp in self._specs.items()}
        self._catalog_pos = (BinlogPosition.parse(d["position"]["text"], self.flavor)
                             if (d.get("position") or {}).get("text") else None)
        if self._dec is not None:
            self._dec.case_sensitive = getattr(self, "case_sensitive", False)
            self._dec.set_specs(self._specs, self._catalog_pos)
        return snap

    def read_catalog_remote(self, settings: dict[str, Any], password: str | None, schemas: list[str],
                            connection_id: str) -> CatalogSnapshot:
        src = type(self)({**settings, **({"databases": schemas} if schemas else {})}, password, connection_id)
        try:
            return src.read_catalog()
        finally:
            src.close()

    @staticmethod
    def _binlog_on(conn: Any) -> bool:
        with conn.cursor() as c:
            c.execute("SELECT @@global.log_bin")
            return str(c.fetchone()[0]).upper() in ("1", "ON")

    def current_position(self) -> Position | None:
        p = P.current_position(self._meta_conn(), self.flavor)
        return Position(engine=self.flavor, text=p.text)

    # ------------------------------------------------------------------ capture
    def _columns_of(self, schema: str, table: str) -> list[str] | None:
        return self._cols.get(self._nk(schema, table))

    def _keys_of(self, schema: str, table: str) -> list[str] | None:
        sp = next((v for (s, n), v in self._specs.items() if self._nk(s, n) == self._nk(schema, table)), None)
        return [c.name for c in sorted(sp.columns, key=lambda c: c.key_seq or 0) if c.key_seq] if sp else None

    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:
        conn = self._meta_conn()
        if not self._binlog_on(conn):
            raise PrerequisiteError("binary logging is disabled on the source (log_bin=OFF)")
        first = str(self.settings.get("start_pos") or "").strip()     # first start only (no durable batch yet)
        start = (BinlogPosition.parse(from_pos.text, self.flavor) if from_pos
                 else BinlogPosition.parse(first, self.flavor) if first
                 else P.current_position(conn, self.flavor))
        self.read_catalog()
        hb = float(self.settings.get("heartbeat_s") or 1.0)
        from .jsonhook import load_json_decoder

        dec = self._dec = BinlogDecoder(self.flavor, self._specs, census=self.census,
                                        json_decoder=load_json_decoder(self.settings))
        dec.case_sensitive = getattr(self, "case_sensitive", False)
        dec.set_start(start)
        dec.catalog_pos = getattr(self, "_catalog_pos", None)   # catch-up window (SRC-my-03)
        asm = TxnAssembler(self.flavor, empty_commit_s=float(self.settings.get("empty_commit_s") or 30),
                           captured_schemas=set(self._schemas()), columns_of=self._columns_of,
                           keys_of=self._keys_of, census=self.census)
        self._client = P.BinlogClient(self.settings, self.password, self.flavor, replica_server_id=self.server_id,
                                      heartbeat_s=hb)
        self._client.open(start)
        log.info("binlog capture started", extra={"component": "alelt_src_mysql", "position": start.text,
                                                   "correlation_id": f"{self.cid}/server_id={self.server_id}"})
        last_yield = time.monotonic()
        last_refresh: str | None = None
        for raw in self._client:
            out = dec.feed(raw)
            if out.events or out.quarantined or out.signals:
                evs = asm.feed(out, dec.txn.txid if dec.txn is not None else None)
                for ev in evs:
                    last_yield = time.monotonic()
                    yield ev
                    if ev.kind is ChangeKind.DDL and shape_changing(ev.note) and ev.txid != last_refresh:
                        # SRC-my-08: only CREATE/ALTER/DROP/RENAME change table shapes; once per DDL transaction
                        last_refresh = ev.txid
                        self.read_catalog()       # new table shapes for the next TABLE_MAP (also sets dec specs)
            if time.monotonic() - last_yield >= 1.0:
                last_yield = time.monotonic()
                yield None
        raise P.BinlogProtocolError("binlog stream ended (server closed the dump)")

    def ack(self, pos: Position) -> None:
        """The server keeps no replica progress (binlog retention is time/size based): remember for status only."""
        self._acked = pos

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._meta is not None:
            try:
                self._meta.close()
            except Exception:  # noqa: BLE001
                pass
            self._meta = None
        self._dec = None

    # ------------------------------------------------------------------ snapshot
    def _snapshot_txn(self, conn: Any) -> tuple[str, bool]:
        """Open a consistent-snapshot read-only transaction; return (binlog position text, exact)."""
        with conn.cursor() as c:
            c.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            if self.flavor == MARIADB:
                c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                c.execute("SHOW STATUS LIKE 'binlog_snapshot_%'")
                st = {str(k).lower(): v for k, v in c.fetchall()}
                f, p = st.get("binlog_snapshot_file"), st.get("binlog_snapshot_position")
                if f:
                    c.execute("SELECT BINLOG_GTID_POS(%s, %s)", (f, int(p or 4)))
                    g = c.fetchone()[0] or ""
                    return f"{g}@{f}:{int(p or 4)}", True
                return "", False
            low = P.current_position(conn, self.flavor).text       # low watermark (<= snapshot point)
            c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            return low, False

    def keyed_reread(self, table: TableInfo, keys: list[dict[str, Any]]) -> KeyedReread:
        """Current rows by primary key in ONE consistent-snapshot read-only REPEATABLE READ transaction;
        `read_from` = the snapshot's binlog position (MariaDB exact, MySQL low watermark taken before the snapshot)."""
        cols = sorted(table.columns, key=lambda c: c.ordinal)
        kcols = table.key_columns()
        if not kcols:
            raise NotImplementedError(f"{table.table.fqn}: no primary key - keyed re-read impossible (resync)")
        conn = P.connect(self.settings, self.password, read_timeout=120)
        found: dict[tuple[Any, ...], dict[str, Any]] = {}
        try:
            pos, _ = self._snapshot_txn(conn)
            src = f"{_q(table.table.schema_)}.{_q(table.table.name)}"
            ktuple = "(" + ", ".join(_q(k.name) for k in kcols) + ")"
            ph = "(" + ", ".join(["%s"] * len(kcols)) + ")"
            for i in range(0, len(keys), 500):
                part = keys[i:i + 500]
                with conn.cursor() as cur:
                    cur.execute(f"SELECT {', '.join(_q(c.name) for c in cols)} FROM {src} WHERE {ktuple} IN "
                                f"({', '.join([ph] * len(part))})", [k[c.name] for k in part for c in kcols])
                    for r in cur.fetchall():
                        row = {c.name: snapshot_value(c, v) for c, v in zip(cols, r)}
                        found[tuple(row[k.name] for k in kcols)] = row
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()
        rows, missing = [], []
        for k in keys:
            row = found.get(tuple(k[c.name] for c in kcols))
            (rows if row is not None else missing).append(row if row is not None else k)
        return KeyedReread(rows=rows, missing=missing, read_from=self.parse_position(pos) if pos else None,
                           isolation="REPEATABLE READ, CONSISTENT SNAPSHOT")

    def snapshot_rows(self, table: TableInfo, key_range: tuple[Any, Any] | None,
                      chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        """Consistent (REPEATABLE READ, WITH CONSISTENT SNAPSHOT) keyset-paged read ordered by the primary key.
        The snapshot's binlog position is recorded in snapshot_positions[fqn] (MariaDB exact, MySQL low watermark)."""
        import pymysql.cursors

        conn = P.connect(self.settings, self.password, read_timeout=int(self.settings.get("snapshot_timeout") or 600))
        try:
            pos, exact = self._snapshot_txn(conn)
            self.snapshot_positions[table.table.fqn] = {"position": pos, "exact": exact}
            log.info("snapshot transaction open", extra={"component": "alelt_src_mysql", "position": pos,
                                                         "table": table.table.fqn})
            cols = sorted(table.columns, key=lambda c: c.ordinal)
            keys = table.key_columns()
            sel = ", ".join(_q(c.name) for c in cols)
            src = f"{_q(table.table.schema_)}.{_q(table.table.name)}"

            def conv(row: tuple[Any, ...]) -> dict[str, Any]:
                return {c.name: snapshot_value(c, v) for c, v in zip(cols, row)}

            if not keys:
                with conn.cursor(pymysql.cursors.SSCursor) as cur:
                    cur.execute(f"SELECT {sel} FROM {src}")
                    while True:
                        rows = cur.fetchmany(chunk_rows)
                        if not rows:
                            break
                        yield [conv(r) for r in rows]
                return
            kidx = [cols.index(next(c for c in cols if c.name == k.name)) for k in keys]
            ktuple = "(" + ", ".join(_q(k.name) for k in keys) + ")"
            ph = "(" + ", ".join(["%s"] * len(keys)) + ")"
            order = ", ".join(_q(k.name) for k in keys)
            last = list(key_range[0]) if key_range and key_range[0] is not None else None
            upto = list(key_range[1]) if key_range and key_range[1] is not None else None
            while True:
                where, params = [], []
                if last is not None:
                    where.append(f"{ktuple} > {ph}")
                    params += last
                if upto is not None:
                    where.append(f"{ktuple} <= {ph}")
                    params += upto
                w = (" WHERE " + " AND ".join(where)) if where else ""
                with conn.cursor() as cur:
                    cur.execute(f"SELECT {sel} FROM {src}{w} ORDER BY {order} LIMIT {int(chunk_rows)}", params)
                    rows = cur.fetchall()
                if not rows:
                    break
                last = [rows[-1][i] for i in kidx]          # server-typed key: exact keyset paging
                yield [conv(r) for r in rows]
                if len(rows) < chunk_rows:
                    break
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()

    # ------------------------------------------------------------------ prerequisites
    def prerequisites(self) -> list[str]:
        return ["binary logging ON (log_bin, server restart)", "binlog_format = ROW", "binlog_row_image = FULL",
                "no partial JSON updates (binlog_row_value_options empty)", "binlog retention covers outages",
                "capture user: REPLICATION SLAVE, REPLICATION CLIENT (MariaDB: BINLOG MONITOR), SELECT",
                "captured tables have a primary key (InnoDB)"]

    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        # SRC-my-09: never mutate this (shared, decode-only) instance: probe with a throw-away copy when settings
        # are given (same pattern as the Postgres connector)
        probe = type(self)({**self.settings, **settings}, self.password, self.cid) if settings else self
        try:
            return probe.check_prerequisites(tables)
        except Exception as e:  # noqa: BLE001 - cannot connect: show what to check
            out = [{"id": "connect", "label": "Hub can connect to the source", "status": "FAIL",
                    "detail": f"{type(e).__name__}: {str(e).strip()[:300]}", "check_sql": None, "fix_sql": None}]
            out += [{"id": f"p{i}", "label": p, "status": "UNKNOWN", "detail": "", "check_sql": None,
                     "fix_sql": None} for i, p in enumerate(self.prerequisites(), 1)]
            return out
        finally:
            if probe is not self:
                probe.close()

    def check_prerequisites(self, tables: list[TableRef] | None = None) -> list[dict[str, Any]]:
        """Read-only checks with fix-it SQL / config for the DBA (never executed by ALELT)."""
        conn = self._meta_conn()
        maria = self.flavor == MARIADB
        out: list[dict[str, Any]] = []
        persist = "SET GLOBAL" if maria else "SET PERSIST"
        cnf = "[mariadb]" if maria else "[mysqld]"

        def add(id_: str, label: str, ok: bool | None, detail: str, fix: str | None = None,
                check: str | None = None, info: bool = False) -> None:
            st = "UNKNOWN" if ok is None else ("PASS" if ok else ("INFO" if info else "FAIL"))
            out.append({"id": id_, "label": label, "status": st, "detail": detail, "check_sql": check,
                        "fix_sql": None if ok else fix})

        with conn.cursor() as c:
            c.execute("SELECT @@version, CURRENT_USER()")
            version, cur_user = (str(x) for x in c.fetchone())
            c.execute("SHOW GLOBAL VARIABLES")
            gv = {str(k).lower(): str(v) for k, v in c.fetchall()}
            grants = self._grants(c)
        v = gv.get
        m = re.match(r"(\d+)\.(\d+)", version)
        ver = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        need = (10, 3) if maria else (8, 0)
        add("version", f"{'MariaDB 10.3' if maria else 'MySQL 8.0'} or later", ver >= need, version)
        restart = "\n-- then restart the server (required)"
        log_bin = v("log_bin", "OFF").upper() in ("ON", "1")
        add("log_bin", "binary logging ON (log_bin)", log_bin, f"log_bin = {v('log_bin')}",
            f"-- my.cnf / my.ini {cnf}\nlog_bin = {'mariadb-bin' if maria else 'binlog'}\nbinlog_format = ROW\n"
            f"binlog_row_image = FULL\nserver_id = 1" + restart, "SELECT @@global.log_bin;")
        add("server_id", "server_id is not 0", v("server_id", "0") != "0", f"server_id = {v('server_id')}",
            f"{persist} server_id = 1;" + ("" if maria else "  -- or server_id in my.cnf"), "SELECT @@server_id;")
        # SRC-my-12: our replica server_id must not collide with another replica (the server drops one of them)
        others: list[str] = []
        with conn.cursor() as c:
            for q in (("SHOW REPLICAS",) if not maria and ver >= (8, 0) else ()) + ("SHOW SLAVE HOSTS",):
                try:
                    c.execute(q)
                    cols = [str(d[0]).lower() for d in (c.description or [])]
                    rows = c.fetchall()
                except Exception:  # noqa: BLE001 - statement unknown on this version / no privilege
                    continue
                i = cols.index("server_id") if "server_id" in cols else 0
                h = cols.index("host") if "host" in cols else None
                others = [f"{r[h] if h is not None else '?'}" for r in rows if str(r[i]) == str(self.server_id)]
                break
        add("replica_server_id", f"replica server_id {self.server_id} not used by another connected replica",
            not others, f"used by {', '.join(others)} (ignore if that is this pipeline's own running capture)"
            if others else "ok", "-- set settings.server_id to a value unique among the source's replicas",
            "SHOW REPLICAS;" if not maria else "SHOW SLAVE HOSTS;", info=True)
        fmt = v("binlog_format", "").upper()
        add("binlog_format", "binlog_format = ROW", fmt == "ROW", f"binlog_format = {fmt}",
            f"{persist} binlog_format = 'ROW';" + (f"\n-- and in my.cnf {cnf}: binlog_format = ROW" if maria else ""),
            "SELECT @@global.binlog_format;")
        img = v("binlog_row_image", "").upper()
        add("binlog_row_image", "binlog_row_image = FULL (full before images)", img == "FULL",
            f"binlog_row_image = {img}", f"{persist} binlog_row_image = 'FULL';", "SELECT @@global.binlog_row_image;")
        meta = v("binlog_row_metadata")
        if meta is not None:
            add("binlog_row_metadata", "binlog_row_metadata = FULL (column names + key in the binlog) - recommended",
                meta.upper() == "FULL", f"binlog_row_metadata = {meta}: columns are matched to the catalog by "
                "position; a DDL inside a catch-up window holds the table for resync, and catalog drift can only "
                "hold the table (no key to re-read) - set FULL",
                f"{persist} binlog_row_metadata = 'FULL';", info=True)
        ck = v("binlog_checksum", "NONE").upper()
        add("binlog_checksum", "binlog_checksum CRC32 or NONE", ck in ("CRC32", "NONE"), f"binlog_checksum = {ck}",
            f"{persist} binlog_checksum = 'CRC32';")
        if maria:
            comp = v("log_bin_compress", "OFF").upper()
            add("log_bin_compress", "binlog compression (log_bin_compress): decoded (zlib, documented format)", True,
                f"log_bin_compress = {comp}", None, info=True)
            add("gtid", "GTID state available (gtid_binlog_pos)", True,
                f"gtid_domain_id = {v('gtid_domain_id')}, gtid_strict_mode = {v('gtid_strict_mode')}, "
                f"gtid_binlog_pos = {v('gtid_binlog_pos') or '(empty)'}", info=True)
        else:
            comp = v("binlog_transaction_compression", "OFF").upper()
            try:
                import zstandard  # noqa: F401
                zst = True
            except ImportError:
                zst = False
            add("binlog_transaction_compression", "compressed transaction payloads decodable (zstandard installed)",
                comp in ("OFF", "0") or zst, f"binlog_transaction_compression = {comp}, zstandard "
                f"{'installed' if zst else 'missing'}", "-- hub: pip install zstandard\n-- or: SET PERSIST "
                "binlog_transaction_compression = OFF;")
            pj = v("binlog_row_value_options", "")
            add("binlog_row_value_options", "no partial JSON updates (binlog_row_value_options empty)",
                "PARTIAL_JSON" not in pj.upper(), f"binlog_row_value_options = '{pj}'",
                "SET PERSIST binlog_row_value_options = '';")
            gm = v("gtid_mode", "OFF").upper()
            # SRC-my-04: without GTIDs the snapshot start is a file:pos read before the consistent snapshot; binlog
            # group commit writes the binlog before the engine commit, so a transaction can be past that position
            # yet invisible to the snapshot -> missed by both. GTID sets are updated after the engine commit.
            add("gtid_mode", "gtid_mode = ON (required: consistent snapshot start, positions survive failover)",
                gm == "ON",
                f"gtid_mode = {gm}, enforce_gtid_consistency = {v('enforce_gtid_consistency')}",
                "SET PERSIST enforce_gtid_consistency = ON;\nSET PERSIST gtid_mode = OFF_PERMISSIVE;\n"
                "SET PERSIST gtid_mode = ON_PERMISSIVE;\n-- wait until no anonymous transactions remain:\n"
                "SHOW STATUS LIKE 'Ongoing_anonymous_transaction_count';\nSET PERSIST gtid_mode = ON;",
                info=bool(self.settings.get("allow_gtid_off")))
        exp = int(v("binlog_expire_logs_seconds", "0") or 0)
        if not exp and v("expire_logs_days"):
            exp = int(float(v("expire_logs_days") or 0) * 86400)
        add("retention", "binlog retention >= 3 days (outage window)", exp == 0 or exp >= 3 * 86400,
            f"binlog_expire_logs_seconds = {exp}" + (" (never purged automatically)" if exp == 0 else ""),
            f"{persist} binlog_expire_logs_seconds = 604800;  -- 7 days", info=True)
        # privileges
        user_sql = self._user_sql(cur_user)
        repl = self._has(grants, "REPLICATION SLAVE") or self._has(grants, "REPLICATION REPLICA")
        add("priv_replication", "REPLICATION SLAVE (binlog dump)", repl, f"user {cur_user}",
            f"GRANT REPLICATION SLAVE ON *.* TO {user_sql};", "SHOW GRANTS;")
        if maria:
            mon = (self._has(grants, "BINLOG MONITOR") or self._has(grants, "REPLICATION CLIENT")
                   or self._has(grants, "SUPER"))
            add("priv_monitor", "BINLOG MONITOR (SHOW BINLOG STATUS)", mon, f"user {cur_user}",
                f"GRANT BINLOG MONITOR ON *.* TO {user_sql};", "SHOW GRANTS;")
        else:
            mon = self._has(grants, "REPLICATION CLIENT")
            add("priv_client", "REPLICATION CLIENT (SHOW BINARY LOG STATUS)", mon, f"user {cur_user}",
                f"GRANT REPLICATION CLIENT ON *.* TO {user_sql};", "SHOW GRANTS;")
        schemas = self._schemas()
        nosel = [s for s in schemas if not self._has(grants, "SELECT", s)]
        add("priv_select", "SELECT on captured databases (catalog, snapshots)", not nosel,
            f"{len(nosel)} database(s) without SELECT" if nosel else f"{len(schemas)} ok",
            "\n".join(f"GRANT SELECT ON {_q(s)}.* TO {user_sql};" for s in nosel) or None)
        # tables
        if schemas:
            snap = self.read_catalog()
            want = [(t.schema_.lower(), t.name.lower()) for t in tables or []]
            cap = [t for t in snap.tables if t.capture_enabled]
            if want:
                have = {(t.table.schema_.lower(), t.table.name.lower()) for t in snap.tables}
                miss = [w for w in want if w not in have]
                add("tables_exist", "captured tables exist", not miss,
                    f"missing: {', '.join('.'.join(x) for x in miss)}" if miss else f"{len(want)} ok")
                cap = [t for t in snap.tables if (t.table.schema_.lower(), t.table.name.lower()) in set(want)]
            add("tables_found", "captured databases contain base tables", bool(cap),
                f"{len(cap)} table(s) in {', '.join(schemas)}")
            nopk = [t.table for t in cap if not t.key_columns()]
            add("primary_keys", "captured tables have a primary key", not nopk,
                f"{len(nopk)} without PK (updates/deletes matched on the full row)" if nopk else "ok",
                "\n".join(f"ALTER TABLE {_q(r.schema_)}.{_q(r.name)} ADD PRIMARY KEY (<key columns>);  -- optional"
                          for r in nopk) or None, info=True)
            with conn.cursor() as c:
                ph = ", ".join(["%s"] * len(schemas))
                c.execute(f"SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE "
                          f"TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA IN ({ph})", schemas)
                eng = {(str(s).lower(), str(n).lower()): str(e or "") for s, n, e in c.fetchall()}
            capset = {(t.table.schema_.lower(), t.table.name.lower()) for t in cap}
            nontx = sorted(k for k, e in eng.items() if k in capset and e.lower() not in ("innodb", "rocksdb"))
            add("engine", "captured tables are transactional (InnoDB)", not nontx,
                f"{len(nontx)} non-transactional table(s): a ROLLBACK touching them holds the table for resync"
                if nontx else "ok",
                "\n".join(f"ALTER TABLE {_q(s)}.{_q(n)} ENGINE = InnoDB;  -- optional" for s, n in nontx) or None,
                info=True)
        if log_bin:
            try:
                p = P.current_position(conn, self.flavor)
                add("position", "current binlog position readable", True, p.text, info=True)
            except Exception as e:  # noqa: BLE001
                add("position", "current binlog position readable", False, f"{type(e).__name__}: {e}"[:300],
                    f"GRANT {'BINLOG MONITOR' if maria else 'REPLICATION CLIENT'} ON *.* TO {user_sql};")
        return out

    @staticmethod
    def _grants(c: Any) -> list[tuple[set[str], str]]:
        """[(privileges, scope)] from SHOW GRANTS; scope '*' (global) or the database name (lower case)."""
        c.execute("SHOW GRANTS")
        out: list[tuple[set[str], str]] = []
        for (g,) in c.fetchall():
            m = re.match(r"^GRANT\s+(.+?)\s+ON\s+(\S+)\s+TO\s", str(g), re.I | re.S)
            if not m:
                continue
            privs = {p.strip().upper() for p in re.sub(r"\([^)]*\)", "", m.group(1)).split(",")}
            obj = m.group(2)
            db = obj.split(".", 1)[0].strip("`").replace("\\_", "_").lower()
            out.append((privs, "*" if obj.startswith("*.*") or db == "*" else db))
        return out

    @staticmethod
    def _has(grants: list[tuple[set[str], str]], priv: str, db: str | None = None) -> bool:
        for privs, scope in grants:
            if scope != "*" and (db is None or scope != db.lower()):
                continue
            if priv in privs or "ALL PRIVILEGES" in privs or "ALL" in privs:
                return True
        return False

    @staticmethod
    def _user_sql(cur_user: str) -> str:
        u, _, h = cur_user.rpartition("@")
        return f"{_lit(u)}@{_lit(h or '%')}"


class MySQLSource(_BinlogSource):
    flavor = MYSQL
    engine = MYSQL
    display_name = "MySQL"


class MariaDBSource(_BinlogSource):
    flavor = MARIADB
    engine = MARIADB
    display_name = "MariaDB"
    connection_fields = [dict(f, label="User (REPLICATION SLAVE, BINLOG MONITOR, SELECT)") if f["name"] == "user"
                         else f for f in _BinlogSource.connection_fields]

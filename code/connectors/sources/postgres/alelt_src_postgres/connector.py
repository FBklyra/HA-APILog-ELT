"""PostgresSource: remote capture with logical decoding (built-in pgoutput) over the streaming replication protocol.

Settings (wizard / pipeline_definition.source.settings):
  host, port (5432), database, user, sslmode (prefer), schemas (list or comma string; default: schemas of the
  publication tables, else 'public'), publication ('alelt'), slot ('alelt_<connection_id>'),
  proto_version (1; 2 + streaming=true streams large in-progress txns, PG >= 14), create_slot (false),
  snapshot_name (exported snapshot id for snapshot_rows, optional), connect_timeout_s (10).
Client: psycopg2 (LGPL-3.0 with linking exception, used as a dynamic library) - `pip install psycopg2-binary`.
"""
from __future__ import annotations

import logging
import select
import time
from collections.abc import Iterator
from typing import Any

from alelt_common.canonical_jsonl import decode_batch
from alelt_common.completeness import KeyedReread
from alelt_common.models import CanonicalType, CatalogSnapshot, ChangeEvent, ChangeKind, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.spi import DecodeResult, SourceConnector, SourceGapError

from . import catalog as C
from .changes import ENGINE, ChangeBuilder
from .position import compare_lsn, format_lsn, normalize, parse_lsn
from .types import canonical_name, parse_text

log = logging.getLogger("alelt_src_postgres")
SESSION_OPTS = "-c client_encoding=UTF8 -c DateStyle=ISO -c TimeZone=UTC -c IntervalStyle=iso_8601 -c extra_float_digits=3 -c bytea_output=hex"


class PrerequisiteError(RuntimeError):
    pass


class SlotLostError(SourceGapError):
    """The replication slot can no longer deliver the changes after the hub's durable position: WAL removed
    (wal_status = lost / invalidated) or the slot was confirmed beyond the durable position (e.g. hub metadata
    restored from an older backup, slot dropped and re-created). Retrying cannot help: resync (REQ-20)."""


def check_slot_resume(durable_lsn: int | None, confirmed_lsn: int | None, wal_status: str | None,
                      invalidation: str | None = None) -> None:
    """Raise SlotLostError when streaming from the slot would silently skip changes after `durable_lsn`.
    PostgreSQL starts a logical slot at max(start_lsn, confirmed_flush_lsn): a confirmed position beyond the hub's
    durable position means the changes in between are gone for this slot."""
    if wal_status == "lost" or invalidation:
        raise SlotLostError(f"replication slot invalidated (wal_status={wal_status}, reason={invalidation}): "
                            "WAL needed by the slot was removed")
    if durable_lsn is not None and confirmed_lsn is not None and confirmed_lsn > durable_lsn:
        raise SlotLostError(f"replication slot confirmed_flush_lsn {format_lsn(confirmed_lsn)} is beyond the hub's "
                            f"durable position {format_lsn(durable_lsn)}: the changes in between cannot be "
                            "re-delivered (hub metadata restored? slot re-created?)")


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


class PostgresSource(SourceConnector):
    engine = ENGINE
    capture_mode = "remote"
    decoder_version = "pgoutput-1"   # HC-02: bump on ANY change of decoded output (replay cache key)
    display_name = "PostgreSQL"
    connection_fields = [
        {"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True, "default": 5432},
        {"name": "database", "label": "Database", "required": True},
        {"name": "user", "label": "User (REPLICATION role)", "required": True},
        {"name": "password", "label": "Password", "secret": True, "required": False},
        {"name": "schemas", "label": "Schemas", "required": False, "default": "public"},
        {"name": "publication", "label": "Publication", "required": False, "default": "alelt"},
        {"name": "slot", "label": "Replication slot", "required": False, "default": ""},
        {"name": "sslmode", "label": "SSL mode", "required": False, "default": "prefer"},
    ]

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str = "") -> None:
        self.settings = dict(settings or {})
        self.password = password
        self.cid = connection_id or str(self.settings.get("connection_id") or "postgres")
        self._meta: Any = None                  # normal connection: catalog, types, prerequisites
        self._repl: Any = None
        self._cur: Any = None
        self._flushed = 0
        self._last_commit: int | None = None    # last COMMIT handed to the runner
        self._acked = 0
        self._in_txn = False
        self._shapes: dict[tuple[str, str], tuple[tuple[str, str], ...]] = {}
        self._next_idle_token = 0.0
        self.snapshot_positions: dict[str, dict[str, Any]] = {}   # fqn -> {position, exact} (SRC-pg-06)

    # ------------------------------------------------------------------ settings
    @property
    def publication(self) -> str:
        return str(self.settings.get("publication") or "alelt")

    @property
    def slot(self) -> str:
        s = str(self.settings.get("slot") or f"alelt_{self.cid}")
        return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in s.lower())[:63]

    def _schemas(self) -> list[str]:
        s = self.settings.get("schemas")
        if isinstance(s, str):
            return [x.strip() for x in s.split(",") if x.strip()]
        return [str(x) for x in s] if s else []

    def _dsn(self) -> dict[str, Any]:
        st = self.settings
        if not st.get("host") or not st.get("database"):
            raise PrerequisiteError("postgres source needs settings host + database")
        d: dict[str, Any] = dict(host=st["host"], port=int(st.get("port") or 5432), dbname=st["database"],
                                 user=st.get("user"), connect_timeout=int(st.get("connect_timeout_s") or 10),
                                 sslmode=st.get("sslmode") or "prefer", application_name="alelt",
                                 keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        if self.password:
            d["password"] = self.password
        return d

    def _meta_conn(self) -> Any:
        if self._meta is None or self._meta.closed:
            import psycopg2

            self._meta = psycopg2.connect(**self._dsn(), options=SESSION_OPTS + " -c statement_timeout=60000"
                                          " -c default_transaction_read_only=on")
            self._meta.autocommit = True
        return self._meta

    # ------------------------------------------------------------------ positions / types / decode
    def parse_position(self, text: str) -> Position:
        return Position(engine=ENGINE, text=normalize(text))

    def compare(self, a: Position, b: Position) -> int:
        return compare_lsn(a.text, b.text)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        return CanonicalType(canonical_name(col.engine_type))

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        return decode_batch(raw, ENGINE, batch_id)

    # ------------------------------------------------------------------ catalog
    def _catalog_schemas(self, cur: Any) -> list[str]:
        if self._schemas():
            return self._schemas()
        pub = C.publication_tables(cur, self.publication)
        return sorted({s for s, _ in pub}) if pub else ["public"]

    def read_catalog(self) -> CatalogSnapshot:
        conn = self._meta_conn()
        with conn.cursor() as cur:
            schemas = self._catalog_schemas(cur)
        snap = C.read_catalog(conn, schemas, self.cid, self.publication, self._current_lsn())
        self._shapes = {(t.table.schema_, t.table.name): tuple((c.name, c.engine_type) for c in t.columns)
                        for t in snap.tables}
        return snap

    def read_catalog_remote(self, settings: dict[str, Any], password: str | None, schemas: list[str],
                            connection_id: str) -> CatalogSnapshot:
        src = PostgresSource({**settings, **({"schemas": schemas} if schemas else {})}, password, connection_id)
        try:
            return src.read_catalog()
        finally:
            src.close()

    def _current_lsn(self) -> str:
        with self._meta_conn().cursor() as cur:
            cur.execute("SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() "
                        "ELSE pg_current_wal_lsn() END::text")
            return normalize(str(cur.fetchone()[0]))

    def current_position(self) -> Position | None:
        return Position(engine=ENGINE, text=self._current_lsn())

    @staticmethod
    def _one(cur: Any, sql: str) -> Any:
        cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r else None

    def _resolve_relation(self, oid: int) -> tuple[str, str] | None:
        with self._meta_conn().cursor() as cur:
            cur.execute("SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE c.oid = %s", (oid,))
            r = cur.fetchone()
            return (str(r[0]), str(r[1])) if r else None

    def _resolve_types(self, oids: list[int]) -> dict[int, str]:
        with self._meta_conn().cursor() as cur:
            return {k: v[0] for k, v in C.type_names(cur, oids).items()}

    # ------------------------------------------------------------------ capture
    def _ensure_slot(self) -> None:
        with self._meta_conn().cursor() as cur:
            cur.execute("SELECT plugin, database, slot_type FROM pg_replication_slots WHERE slot_name = %s",
                        (self.slot,))
            row = cur.fetchone()
            if C.publication_tables(cur, self.publication) is None:
                raise PrerequisiteError(f"publication {self.publication!r} does not exist; DBA: "
                                        f"CREATE PUBLICATION {_q(self.publication)} FOR TABLE <schema.table>, ...;")
        if row is None:
            if not self.settings.get("create_slot"):
                raise PrerequisiteError(f"replication slot {self.slot!r} does not exist; DBA: SELECT "
                                        f"pg_create_logical_replication_slot({_lit(self.slot)}, 'pgoutput');")
            self._cur.create_replication_slot(self.slot, output_plugin="pgoutput")
            log.info("replication slot created", extra={"component": "alelt_src_postgres", "correlation_id": self.slot})
        elif row[0] != "pgoutput" or row[1] != self.settings["database"]:
            raise PrerequisiteError(f"slot {self.slot!r} uses plugin {row[0]!r} on database {row[1]!r}; "
                                    "need pgoutput on the captured database")

    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:
        import psycopg2
        import psycopg2.extras

        self._repl = psycopg2.connect(**self._dsn(), options=SESSION_OPTS,
                                      connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self._cur = self._repl.cursor()
        self._ensure_slot()
        with self._meta_conn().cursor() as cur:
            ver = int(self._one(cur, "SHOW server_version_num"))
            cols = "confirmed_flush_lsn::text, " + ("wal_status" if ver >= 130000 else "NULL") + ", " + (
                "invalidation_reason" if ver >= 170000 else "NULL")
            cur.execute(f"SELECT {cols} FROM pg_replication_slots WHERE slot_name = %s", (self.slot,))
            r = cur.fetchone()
        if r is not None:
            check_slot_resume(parse_lsn(from_pos.text) if from_pos else None,
                              parse_lsn(r[0]) if r[0] else None, r[1], r[2])
        proto = int(self.settings.get("proto_version") or 1)
        opts = {"proto_version": str(proto), "publication_names": self.publication}
        if proto >= 2 and self.settings.get("streaming"):
            opts["streaming"] = "on"
        start = parse_lsn(from_pos.text) if from_pos else 0
        self._cur.start_replication(slot_name=self.slot, slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                    start_lsn=start, options=opts, decode=False, status_interval=10)
        self._flushed = self._acked = start
        self._last_commit, self._in_txn = None, False
        outside = self._outside_filter()
        builder = ChangeBuilder(self._resolve_types, self._shapes, census=self.census,
                                resolve_relation=self._resolve_relation,
                                fallback_codec=self.settings.get("sql_ascii_codec") or None,
                                empty_commit_s=float(self.settings.get("empty_commit_s") or 30))
        log.info("replication started", extra={"component": "alelt_src_postgres", "position": format_lsn(start),
                                                "correlation_id": self.slot})
        last_yield = time.monotonic()
        while True:
            msg = self._cur.read_message()
            if msg is None:
                tok = self._idle_token()
                if tok is not None:
                    last_yield = time.monotonic()
                    yield tok
                    continue
                if time.monotonic() - last_yield >= 1.0:
                    last_yield = time.monotonic()
                    yield None
                    continue
                ready = select.select([self._repl], [], [], 1.0)[0]
                if not ready:
                    last_yield = time.monotonic()
                    yield None
                continue
            for ev in builder.feed(msg.payload, msg.data_start):
                if outside(ev):
                    continue
                if ev.kind is ChangeKind.COMMIT:
                    self._last_commit, self._in_txn = parse_lsn(ev.position.text), False
                elif ev.kind is not ChangeKind.DDL:
                    self._in_txn = True
                last_yield = time.monotonic()
                yield ev

    def _outside_filter(self) -> Any:
        """SRC-pg-10: a publication can hold tables outside settings.schemas; their changes (and their "new relation"
        on every session) are not captured: dropped here, counted, never a catalog refresh per reconnect."""
        caps = {s.lower() for s in self._schemas()}

        def outside(ev: ChangeEvent) -> bool:
            if not caps or ev.table is None or ev.kind is ChangeKind.COMMIT or ev.table.schema_.lower() in caps:
                return False
            self.census.count("pgoutput:outside", "ignored", "table outside settings.schemas")
            return True
        return outside

    def _idle_token(self) -> ChangeEvent | None:
        """Nothing outstanding (all handed-out commits acked, no open txn) and the server end moved on: an empty COMMIT
        at the server end, so the hub's durable position (not only the slot) follows the server end. The slot is
        confirmed only through ack() of that token: confirmed_flush_lsn never runs ahead of the hub (XC-05)."""
        if self._cur is None or self._in_txn:
            return None
        if self._last_commit is not None and self._acked < self._last_commit:
            return None
        end = int(getattr(self._cur, "wal_end", 0) or 0)
        if end <= max(self._flushed, self._last_commit or 0) or time.monotonic() < self._next_idle_token:
            return None
        self._next_idle_token = time.monotonic() + float(self.settings.get("idle_token_s") or 10)
        self._last_commit = end
        self.census.count("pgoutput:keepalive", "ignored", "server end moved without captured changes (idle token)")
        return ChangeEvent(position=Position(engine=ENGINE, text=format_lsn(end)), txid=f"idle:{format_lsn(end)}",
                           kind=ChangeKind.COMMIT)

    def ack(self, pos: Position) -> None:
        """Batch is durable at the hub: confirm_flush up to its next_pos (the slot may release WAL)."""
        lsn = parse_lsn(pos.text)
        self._acked = max(self._acked, lsn)
        if self._cur is not None and lsn > self._flushed:
            self._cur.send_feedback(flush_lsn=lsn, force=True)
            self._flushed = lsn

    def keepalive(self) -> None:
        """XC-18: called by the capture runner while the hub applies backpressure (no stream read): a standby status
        update with the already-confirmed position keeps the walsender from timing out (wal_sender_timeout)."""
        if self._cur is not None:
            self._cur.send_feedback(flush_lsn=self._flushed, force=True)

    def close(self) -> None:
        for attr in ("_cur", "_repl", "_meta"):
            obj = getattr(self, attr)
            setattr(self, attr, None)
            try:
                if obj is not None:
                    obj.close()
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------ snapshot / keyed re-read
    @staticmethod
    def _select_list(cols: list[ColumnInfo]) -> str:
        # text output of each type (what pgoutput sends); bpchar::text strips the padding, concat() keeps it
        return ", ".join(f"CASE WHEN {_q(c.name)} IS NULL THEN NULL ELSE concat({_q(c.name)}) END"
                         if c.engine_type == "bpchar" else f"{_q(c.name)}::text" for c in cols)

    def keyed_reread(self, table: TableInfo, keys: list[dict[str, Any]]) -> KeyedReread:
        """Current rows by primary key: one REPEATABLE READ read-only transaction (one snapshot for every key);
        `read_from` = WAL position read before the snapshot. Values in the snapshot_rows forms."""
        import psycopg2

        cols = sorted(table.columns, key=lambda c: c.ordinal)
        kcols = table.key_columns()
        if not kcols:
            raise NotImplementedError(f"{table.table.fqn}: no primary key - keyed re-read impossible (resync)")
        read_from = self.current_position()
        conn = psycopg2.connect(**self._dsn(), options=SESSION_OPTS + " -c statement_timeout=60000")
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            cur = conn.cursor()
            src = f"{_q(table.table.schema_)}.{_q(table.table.name)}"
            ktuple = "(" + ", ".join(_q(k.name) for k in kcols) + ")"
            ph = "(" + ", ".join(["%s"] * len(kcols)) + ")"
            found: dict[tuple[Any, ...], dict[str, Any]] = {}
            for i in range(0, len(keys), 500):
                part = keys[i:i + 500]
                params = [k[c.name] for k in part for c in kcols]
                cur.execute(f"SELECT {self._select_list(cols)} FROM {src} WHERE {ktuple} IN "
                            f"({', '.join([ph] * len(part))})", params)
                for r in cur.fetchall():
                    row = {c.name: None if v is None else parse_text(c.engine_type, v) for c, v in zip(cols, r)}
                    found[tuple(row[k.name] for k in kcols)] = row
            conn.rollback()
        finally:
            conn.close()
        rows, missing = [], []
        for k in keys:
            row = found.get(tuple(k[c.name] for c in kcols))
            (rows if row is not None else missing).append(row if row is not None else k)
        return KeyedReread(rows=rows, missing=missing, read_from=read_from, isolation="REPEATABLE READ")

    def snapshot_start(self, conn: Any, cur: Any, attempts: int = 50) -> str:
        """SRC-pg-06: a WAL position P that is safe as catch-up start for the REPEATABLE READ snapshot opened by the
        next statement on `cur`: every transaction NOT visible in the snapshot commits at or after P.

        P = WAL position read together with the in-progress set X (txid_current_snapshot) BEFORE the snapshot. The
        only transactions that could be missed are those in X whose commit record is already before P but which
        were not yet visible when the snapshot was taken (window between the commit record and leaving the proc
        array, e.g. a synchronous-replication wait). So after taking the snapshot: if any transaction of X is
        committed (clog) but invisible in the snapshot, retry with a fresh snapshot. Snapshot position = P, recorded
        in `snapshot_positions` (the hub's orchestrator then replays from P)."""
        meta = self._meta_conn()
        rec = ("CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END::text")
        for _ in range(attempts):
            with meta.cursor() as m:
                m.execute(f"SELECT {rec}, txid_current_snapshot()::text")
                low, before = m.fetchone()
            cur.execute("SELECT txid_current_snapshot()::text")    # first statement: the RR snapshot is taken here
            snap = cur.fetchone()[0]
            with meta.cursor() as m:
                m.execute("SELECT count(*) FROM txid_snapshot_xip(%s::txid_snapshot) AS x "
                          "WHERE NOT txid_visible_in_snapshot(x, %s::txid_snapshot) "
                          "AND txid_status(x) = 'committed'", (before, snap))
                racing = int(m.fetchone()[0])
            if not racing:
                return normalize(str(low))
            conn.rollback()                                       # a new snapshot at the next statement
            time.sleep(0.05)
        raise RuntimeError("no consistent snapshot start after retries: transactions keep committing between "
                           "their commit record and visibility (synchronous replication stalled?)")

    def snapshot_rows(self, table: TableInfo, key_range: tuple[Any, Any] | None,
                      chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        """Consistent (REPEATABLE READ, optional exported snapshot) keyset-paged read ordered by the primary key."""
        import psycopg2

        conn = psycopg2.connect(**self._dsn(), options=SESSION_OPTS + " -c statement_timeout=0")
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            cur = conn.cursor()
            if self.settings.get("snapshot_name"):
                cur.execute("SET TRANSACTION SNAPSHOT %s", (str(self.settings["snapshot_name"]),))
            else:
                low = self.snapshot_start(conn, cur)
                self.snapshot_positions[table.table.fqn] = {"position": low, "exact": False}
            cols = sorted(table.columns, key=lambda c: c.ordinal)
            keys = table.key_columns()
            sel = self._select_list(cols)
            src = f"{_q(table.table.schema_)}.{_q(table.table.name)}"
            ktuple = "(" + ", ".join(_q(k.name) for k in keys) + ")"
            order = ", ".join(_q(k.name) for k in keys)
            last = list(key_range[0]) if key_range and key_range[0] is not None else None
            upto = list(key_range[1]) if key_range and key_range[1] is not None else None

            def conv(row: tuple[Any, ...]) -> dict[str, Any]:
                return {c.name: None if v is None else parse_text(c.engine_type, v) for c, v in zip(cols, row)}

            if not keys:
                cur.close()
                cur = conn.cursor(name="alelt_snapshot")          # server-side cursor
                cur.itersize = chunk_rows
                cur.execute(f"SELECT {sel} FROM {src}")
                while True:
                    rows = cur.fetchmany(chunk_rows)
                    if not rows:
                        break
                    yield [conv(r) for r in rows]
                return
            kidx = [cols.index(k) for k in keys]
            ph = "(" + ", ".join(["%s"] * len(keys)) + ")"
            while True:
                where, params = [], []
                if last is not None:
                    where.append(f"{ktuple} > {ph}")
                    params += [str(x) for x in last]
                if upto is not None:
                    where.append(f"{ktuple} <= {ph}")
                    params += [str(x) for x in upto]
                w = (" WHERE " + " AND ".join(where)) if where else ""
                cur.execute(f"SELECT {sel} FROM {src}{w} ORDER BY {order} LIMIT {int(chunk_rows)}", params)
                rows = cur.fetchall()
                if not rows:
                    break
                last = [rows[-1][i] for i in kidx]               # text form of the key: exact keyset paging
                yield [conv(r) for r in rows]
                if len(rows) < chunk_rows:
                    break
        finally:
            conn.close()

    # ------------------------------------------------------------------ prerequisites
    def prerequisites(self) -> list[str]:
        return ["wal_level = logical (server restart)", "max_replication_slots / max_wal_senders free",
                "capture user has REPLICATION (or superuser) and SELECT on captured tables",
                f"publication {self.publication!r} contains the captured tables",
                f"logical replication slot {self.slot!r} (pgoutput)",
                "REPLICA IDENTITY FULL on tables without primary key"]

    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        # never mutate this instance (the hub keeps one decode-only instance per engine): check on a fresh one
        merged = {**self.settings, **(settings or {})}
        pending = [{"id": f"p{i}", "label": p, "status": "UNKNOWN", "detail": "", "fix_sql": None}
                   for i, p in enumerate(self.prerequisites(), 1)]
        if not merged.get("host") or not merged.get("database"):
            return [{"id": "connect", "label": "Hub can connect to the source", "status": "UNKNOWN",
                     "detail": "connection settings not given yet", "fix_sql": None}] + pending
        src = self if merged == self.settings else PostgresSource(merged, self.password, self.cid)
        try:
            return src.check_prerequisites(tables)
        except Exception as e:  # noqa: BLE001 - cannot connect: show what to check
            return [{"id": "connect", "label": "Hub can connect to the source", "status": "FAIL",
                     "detail": f"{type(e).__name__}: {str(e).strip()[:300]}", "fix_sql": None}] + pending
        finally:
            if src is not self:
                src.close()

    def check_prerequisites(self, tables: list[TableRef] | None = None) -> list[dict[str, Any]]:
        """Read-only checks with fix-it SQL for the DBA (never executed by ALELT)."""
        conn = self._meta_conn()
        out: list[dict[str, Any]] = []

        def add(id_: str, label: str, ok: bool | None, detail: str, fix: str | None = None,
                check: str | None = None, info: bool = False) -> None:
            st = "UNKNOWN" if ok is None else ("PASS" if ok else ("INFO" if info else "FAIL"))
            out.append({"id": id_, "label": label, "status": st, "detail": detail,
                        "check_sql": check, "fix_sql": None if ok else fix})

        with conn.cursor() as cur:
            def one(sql: str, args: tuple[Any, ...] = ()) -> Any:
                cur.execute(sql, args)
                r = cur.fetchone()
                return r[0] if r else None

            ver = int(one("SHOW server_version_num"))
            add("version", "PostgreSQL 12 or later (pgoutput, generated-column catalog)", ver >= 120000,
                f"server_version_num {ver}")
            enc = str(one("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()"))
            add("encoding", "database encoding convertible to UTF-8 (the sessions run with client_encoding=UTF8)",
                enc.upper() != "SQL_ASCII", f"encoding {enc}",
                "-- SQL_ASCII stores bytes without an encoding: values that are not valid UTF-8 are quarantined "
                "per row\n-- migrate the database to UTF8 (dump/restore) or set settings.sql_ascii_codec "
                "(e.g. 'latin-1')")
            inuse = one("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s AND active", (self.slot,))
            add("slot_free", f"replication slot {self.slot} not in use by another consumer", inuse is None,
                f"active_pid {inuse}" if inuse is not None else "ok",
                f"SELECT pg_terminate_backend({inuse});  -- only if that consumer is a stale copy of this pipeline"
                if inuse is not None else None,
                "SELECT slot_name, active, active_pid FROM pg_replication_slots;")
            wal = one("SHOW wal_level")
            add("wal_level", "wal_level = logical", wal == "logical", f"wal_level = {wal}",
                "ALTER SYSTEM SET wal_level = logical;\n-- then restart PostgreSQL (required)", "SHOW wal_level;")
            slots = int(one("SHOW max_replication_slots"))
            used = int(one("SELECT count(*) FROM pg_replication_slots"))
            have_slot = one("SELECT plugin FROM pg_replication_slots WHERE slot_name = %s", (self.slot,))
            add("max_replication_slots", "a free replication slot", have_slot is not None or used < slots,
                f"{used} of {slots} slots used", "ALTER SYSTEM SET max_replication_slots = 10;\n-- then restart",
                "SHOW max_replication_slots;")
            senders = int(one("SHOW max_wal_senders"))
            active = int(one("SELECT count(*) FROM pg_stat_replication"))
            add("max_wal_senders", "a free WAL sender", active < senders, f"{active} of {senders} in use",
                "ALTER SYSTEM SET max_wal_senders = 10;\n-- then restart", "SHOW max_wal_senders;")
            user = str(one("SELECT current_user"))
            repl = bool(one("SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user"))
            add("replication_role", f"role {user} has REPLICATION", repl, "" if repl else "missing REPLICATION",
                f"ALTER ROLE {_q(user)} WITH REPLICATION;")
            pub = C.publication_tables(cur, self.publication)
            schemas = self._catalog_schemas(cur)
            add("publication", f"publication {self.publication} exists", pub is not None,
                f"{len(pub)} table(s)" if pub is not None else "missing",
                f"CREATE PUBLICATION {_q(self.publication)} FOR TABLE "
                + (", ".join(f"{_q(t.schema_)}.{_q(t.name)}" for t in tables) if tables else "<schema.table>, ...")
                + ";\n-- PostgreSQL 15+: CREATE PUBLICATION "
                + f"{_q(self.publication)} FOR TABLES IN SCHEMA {', '.join(_q(s) for s in schemas)};")
            if pub is not None:
                flags = cur.execute("SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication "
                                    "WHERE pubname = %s", (self.publication,)) or cur.fetchone()
                allops = bool(flags and all(flags))
                add("publication_ops", "publication publishes insert, update, delete, truncate", allops,
                    str(flags), f"ALTER PUBLICATION {_q(self.publication)} SET (publish = "
                                "'insert, update, delete, truncate');")
            want = [(t.schema_, t.name) for t in tables or []]
            if want and pub is not None:
                miss = [w for w in want if w not in pub]
                add("publication_tables", "captured tables are in the publication", not miss,
                    f"{len(miss)} missing" if miss else f"{len(want)} ok",
                    "\n".join(f"ALTER PUBLICATION {_q(self.publication)} ADD TABLE {_q(s)}.{_q(n)};"
                              for s, n in miss) or None)
            add("slot", f"logical replication slot {self.slot} (pgoutput)", have_slot == "pgoutput",
                f"plugin {have_slot}" if have_slot else "missing",
                f"SELECT pg_create_logical_replication_slot({_lit(self.slot)}, 'pgoutput');",
                "SELECT slot_name, plugin, confirmed_flush_lsn FROM pg_replication_slots;")
            check_tabs = want or sorted(pub or [])
            if check_tabs:
                cur.execute("""SELECT n.nspname, c.relname, c.relreplident,
                                      EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
                                      has_table_privilege(c.oid, 'SELECT')
                                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE (n.nspname, c.relname) IN (SELECT * FROM unnest(%s::text[], %s::text[]))""",
                            ([s for s, _ in check_tabs], [n for _, n in check_tabs]))
                rows = cur.fetchall()
                bad = [(s, n) for s, n, ident, haspk, _ in rows if ident == "n" or (not haspk and ident != "f")]
                add("replica_identity", "REPLICA IDENTITY FULL on tables without primary key (never NOTHING)", not bad,
                    f"{len(bad)} table(s)" if bad else "ok",
                    "\n".join(f"ALTER TABLE {_q(s)}.{_q(n)} REPLICA IDENTITY FULL;" for s, n in bad) or None)
                dflt = [(s, n) for s, n, ident, haspk, _ in rows if haspk and ident == "d"]
                add("before_images", "full before images (REPLICA IDENTITY FULL) - optional", not dflt,
                    f"{len(dflt)} table(s) send key-only before images (enough for ALELT)",
                    "\n".join(f"ALTER TABLE {_q(s)}.{_q(n)} REPLICA IDENTITY FULL;  -- optional" for s, n in dflt)
                    or None, info=True)
                cur.execute("""SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE c.relkind = 'p' AND (n.nspname, c.relname) IN
                                      (SELECT * FROM unnest(%s::text[], %s::text[]))""",
                            ([s for s, _ in check_tabs], [n for _, n in check_tabs]))
                parts = cur.fetchall()
                viaroot = bool(one("SELECT pubviaroot FROM pg_publication WHERE pubname = %s", (self.publication,))) \
                    if pub is not None else False
                add("partition_root", "partitioned tables published via the partition root", not parts or viaroot,
                    f"{len(parts)} partitioned table(s), publish_via_partition_root={viaroot}",
                    f"ALTER PUBLICATION {_q(self.publication)} SET (publish_via_partition_root = true);")
                nosel = [(s, n) for s, n, _, _, ok in rows if not ok]
                add("select", "SELECT on captured tables (snapshots, catalog)", not nosel,
                    f"{len(nosel)} table(s)" if nosel else "ok",
                    "\n".join(f"GRANT SELECT ON {_q(s)}.{_q(n)} TO {_q(user)};" for s, n in nosel) or None)
        return out

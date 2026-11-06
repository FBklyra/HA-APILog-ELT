"""Binlog event stream -> plain change events (stdlib only).

BinlogDecoder is a resumable state machine. Its state (format description, binlog file/offset, GTID state, open
transaction, table maps of the open transaction) serialises to JSON; the capture side writes it as the CONTEXT
frame of every raw batch and the hub restores it before decoding that batch (framing.py).

Transaction rules (both flavors):
- MySQL: GTID / ANONYMOUS_GTID starts a transaction; QUERY 'BEGIN' opens the DML part; XID or QUERY 'COMMIT' ends
  it; a statement outside BEGIN (DDL) is a transaction by itself.
- MariaDB: GTID_EVENT starts a transaction (it replaces BEGIN); FL_STANDALONE => one statement, no COMMIT.
- InnoDB rollbacks never reach the binlog; a QUERY 'ROLLBACK' (non-transactional tables) discards the transaction.
- 'ROLLBACK TO SAVEPOINT x' emits one COMPENSATION per DML event logged after 'SAVEPOINT x'.
- A transaction still open when a new transaction / format description arrives was cut by a server crash:
  it is discarded (ROLLBACK) - the server did not commit it either.

Completeness (alelt_common/completeness.py, X-132): every event is counted in `census` (record `binlog:<type>`):
decoded (control + row events), ignored with the documented reason (heartbeat, annotations, file headers,
statement-based context, group-replication info, LOG_EVENT_IGNORABLE_F, rows of tables outside the capture set), or
a signal in `DecodeOut.signals`: compressed transaction payloads (MySQL) and compressed events (MariaDB) are
uncompressed per the documented formats and their inner events decoded; tagged GTIDs (MySQL 8.3+) are decoded;
PARTIAL_JSON row events -> unsupported(table) (the JSON diff format is not in the reference manual: hold, resync);
catalog drift between TABLE_MAP and the catalog -> needs_catalog_refresh + needs_keyed_reread by the key decoded with
the TABLE_MAP's own metadata (binlog_row_metadata=FULL) else unsupported(table); rows for an unknown table id, XA
PREPARE, INCIDENT, encryption -> unsupported(connection); an unknown event type -> unexplained + unsupported.
"""
from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from alelt_common import completeness as FC
from alelt_common.models import TableRef

from . import events as E
from . import types as T
from .framing import CONTEXT, EVENT, FramingError, iter_frames, read_context
from .position import MARIADB, MYSQL, BinlogPosition, GtidSet, MariaGtidList
from .rows import ColumnPlan, RowError, plan_from_defs, read_image

_SAVEPOINT = re.compile(r"^\s*SAVEPOINT\s+`?([^`\s]+)`?\s*$", re.I)
_ROLLBACK_TO = re.compile(r"^\s*ROLLBACK\s+(?:WORK\s+)?TO\s+(?:SAVEPOINT\s+)?`?([^`\s]+)`?\s*$", re.I)
_STMT_DML = re.compile(r"^\s*(?:/\*.*?\*/\s*)*(INSERT|REPLACE|UPDATE|DELETE|LOAD\s+DATA|LOAD\s+XML|CALL|DO)\b",
                       re.I | re.S)
_STMT_TABLE = re.compile(r"\b(?:INTO|UPDATE|FROM|TABLE)\s+(?:LOW_PRIORITY\s+|IGNORE\s+|QUICK\s+)*"
                         r"((?:`[^`]+`|[\w$]+)(?:\s*\.\s*(?:`[^`]+`|[\w$]+))?)", re.I)
_DDL_TABLE = re.compile(
    r"^\s*(?:/\*.*?\*/\s*)*(CREATE|ALTER|DROP|RENAME|TRUNCATE)\s+(?:TEMPORARY\s+)?(?:TABLE\s+)?"
    r"(?:IF\s+(?:NOT\s+)?EXISTS\s+)?((?:`[^`]+`|[\w$]+)(?:\s*\.\s*(?:`[^`]+`|[\w$]+))?)", re.I | re.S)


@dataclass
class PlainEvent:
    kind: str
    txid: str
    position: str
    schema: str | None = None
    table: str | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    commit_ts: datetime | None = None
    offset: int = 0
    length: int = 0
    note: str | None = None


@dataclass
class PlainQuarantine:
    position: str | None
    schema: str | None
    table: str | None
    reason: str
    offset: int
    length: int
    raw_hex: str


@dataclass
class PlainSignal:
    position: str
    signal: FC.SourceSignal


@dataclass
class DecodeOut:
    events: list[PlainEvent] = field(default_factory=list)
    quarantined: list[PlainQuarantine] = field(default_factory=list)
    skipped: int = 0
    signals: list[PlainSignal] = field(default_factory=list)


@dataclass
class ColSpec:
    name: str
    ordinal: int
    engine_type: str
    canonical: str
    codepage: int | None = None
    key_seq: int | None = None
    length: int | None = None


@dataclass
class TableSpec:
    schema: str
    name: str
    columns: list[ColSpec]


@dataclass
class Txn:
    txid: str
    began: bool = False
    standalone: bool = False
    commit_us: int | None = None
    ndml: int = 0
    savepoints: list[tuple[str, int]] = field(default_factory=list)
    dml_tables: list[tuple[str, str]] = field(default_factory=list)  # only tracked once a savepoint exists
    xa: bool = False
    touched: list[tuple[str, str]] = field(default_factory=list)   # captured tables with DML in this txn

    def to_json(self) -> dict[str, Any]:
        return {"txid": self.txid, "began": self.began, "standalone": self.standalone, "commit_us": self.commit_us,
                "ndml": self.ndml, "savepoints": self.savepoints, "dml_tables": self.dml_tables, "xa": self.xa,
                "touched": self.touched}

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> Txn:
        return cls(d["txid"], d.get("began", False), d.get("standalone", False), d.get("commit_us"),
                   d.get("ndml", 0), [tuple(x) for x in d.get("savepoints", [])],  # type: ignore[misc]
                   [tuple(x) for x in d.get("dml_tables", [])], d.get("xa", False),  # type: ignore[misc]
                   [tuple(x) for x in d.get("touched", [])])  # type: ignore[misc]


def _split_name(text: str, default_schema: str) -> tuple[str, str]:
    parts = [p.strip().strip("`") for p in re.split(r"\s*\.\s*", text.strip(), maxsplit=1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return default_schema, parts[0]


_FAMILIES: dict[str, frozenset[int]] = {
    "int": frozenset({T.TINY, T.SHORT, T.INT24, T.LONG, T.LONGLONG}),
    "dec": frozenset({T.NEWDECIMAL, T.DECIMAL}),
    "float": frozenset({T.FLOAT, T.DOUBLE}),
    "str": frozenset({T.VARCHAR, T.VAR_STRING, T.STRING}),
    "blob": frozenset({T.BLOB, T.TINY_BLOB, T.MEDIUM_BLOB, T.LONG_BLOB}),
    "date": frozenset({T.DATE, T.NEWDATE}),
    "datetime": frozenset({T.DATETIME, T.DATETIME2}),
    "timestamp": frozenset({T.TIMESTAMP, T.TIMESTAMP2}),
    "time": frozenset({T.TIME, T.TIME2}),
}
_BASE_FAMILY = {
    **{b: "int" for b in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint")},
    "decimal": "dec", "numeric": "dec", "float": "float", "double": "float", "real": "float",
    "char": "str", "varchar": "str", "binary": "str", "varbinary": "str",
    "tinytext": "blob", "text": "blob", "mediumtext": "blob", "longtext": "blob", "tinyblob": "blob", "blob": "blob",
    "mediumblob": "blob", "longblob": "blob", "date": "date", "datetime": "datetime", "timestamp": "timestamp",
    "time": "time",
}


def build_plan(tm: E.TableMap, spec: TableSpec, flavor: str, json_decoder: Any = None) -> list[ColumnPlan]:
    """Join TABLE_MAP columns with catalog columns. Raises RowError on drift (=> quarantine, R7)."""
    cols = sorted(spec.columns, key=lambda c: c.ordinal)
    if all(c.name for c in tm.columns):
        by_name = {c.name.lower(): c for c in cols}
        try:
            matched = [by_name[str(d.name).lower()] for d in tm.columns]
        except KeyError as e:
            raise RowError(f"binlog column {e.args[0]} not in catalog of {spec.schema}.{spec.name} "
                           "(schema drift: refresh the catalog)") from None
    else:
        if len(cols) != len(tm.columns):
            raise RowError(f"binlog has {len(tm.columns)} columns, catalog {len(cols)} for {spec.schema}.{spec.name} "
                           "(schema drift: refresh the catalog)")
        matched = cols
    plan: list[ColumnPlan] = []
    for d, c in zip(tm.columns, matched):
        base = T.base_type(c.engine_type)
        fam = _BASE_FAMILY.get(base)
        if fam and d.type not in _FAMILIES[fam] and not (base == "json" or flavor == MARIADB and fam == "blob"):
            raise RowError(f"column {c.name}: catalog type {c.engine_type} but binlog type "
                           f"{T.TYPE_NAMES.get(d.type, d.type)} (schema drift: refresh the catalog)")
        unsigned = d.unsigned if d.unsigned is not None else T.is_unsigned(c.engine_type)
        char_like = base in T.STRING_TYPES or base in ("enum", "set") or (base in ("uuid", "inet4", "inet6"))
        codec = T.codec_for(c.codepage) if char_like else None
        labels = d.enum_values or d.set_values
        if base in ("enum", "set") and not labels:
            labels = T.enum_labels(c.engine_type)
        json_text = base == "json" and d.type != T.JSON
        if base in ("uuid", "inet4", "inet6") and d.type != T.STRING:
            raise RowError(f"column {c.name}: MariaDB {base} with binlog type {d.type} (schema drift?)")
        if base in ("uuid", "inet4", "inet6"):
            codec = None          # fixed binary: INET4/INET6 = network order (documented), UUID: storage order
        plan.append(ColumnPlan(c.name, d.type, d.meta, d.meta2, c.canonical, bool(unsigned), codec,
                               base == "char", labels, json_text, c.engine_type,
                               c.length if base == "binary" else None,
                               inet=4 if base == "inet4" else 6 if base == "inet6" else 0, reread=base == "uuid",
                               json_decoder=json_decoder))
    return plan


class BinlogDecoder:
    def __init__(self, flavor: str, specs: dict[tuple[str, str], TableSpec] | None = None,
                 decode_rows: bool = True, census: FC.Census | None = None, json_decoder: Any = None) -> None:
        if flavor not in (MYSQL, MARIADB):
            raise ValueError(flavor)
        self.flavor = flavor
        self.census = census if census is not None else FC.Census(flavor)
        self.json_decoder = json_decoder          # optional JsonBinaryDecoder plugin (jsonhook.py, X-133)
        self._cls: tuple[str, str] | None = None     # classification of the event being fed (census)
        self.specs = specs or {}
        self.decode_rows = decode_rows
        self.fde = E.FormatDescription(checksum=E.CHECKSUM_OFF)
        self.fde_raw: bytes | None = None
        self.file: str | None = None
        self.pos: int = 4
        self.gtid: GtidSet | MariaGtidList | None = None
        self.txn: Txn | None = None
        self.maps: dict[int, E.TableMap] = {}
        self.maps_raw: dict[int, bytes] = {}
        self._plans: dict[int, list[ColumnPlan] | RowError | None] = {}
        self._lower = {(s.lower(), t.lower()): v for (s, t), v in self.specs.items()}
        # catch-up window (SRC-my-03): binlog position at which the catalog (self.specs) was read. Until the stream
        # reaches it, the catalog may describe a LATER table shape than the row events being decoded.
        self.catalog_pos: BinlogPosition | None = None
        # binlog_row_metadata as seen in TABLE_MAPs: True = FULL (column names), False = MINIMAL, None = not seen yet
        self.tm_named: bool | None = None
        self.case_sensitive = False              # set by the connector from @@lower_case_table_names

    def set_specs(self, specs: dict[tuple[str, str], TableSpec], catalog_pos: BinlogPosition | None = None) -> None:
        """New catalog (after DDL); plans are rebuilt at the next TABLE_MAP."""
        self.specs = specs
        self._lower = {(s.lower(), t.lower()): v for (s, t), v in specs.items()}
        self._plans.clear()
        self.catalog_pos = catalog_pos

    def in_catchup_window(self) -> bool:
        """True while the decoder position is before the position the catalog was read at."""
        if self.catalog_pos is None:
            return False
        from .position import compare_positions
        if compare_positions(self.position(), self.catalog_pos) < 0:
            return True
        self.catalog_pos = None           # window closed: the catalog is older than everything that follows
        return False

    # ------------------------------------------------------------------ state
    def position(self) -> BinlogPosition:
        g = self.gtid.copy() if self.gtid is not None else None
        return BinlogPosition(self.flavor, g, self.file, self.pos if self.file is not None else None)

    def set_start(self, pos: BinlogPosition) -> None:
        self.gtid = pos.gtid.copy() if pos.gtid is not None else None
        if pos.file is not None:
            self.file, self.pos = pos.file, int(pos.pos or 4)

    def state(self) -> dict[str, Any]:
        return {"flavor": self.flavor, "file": self.file, "pos": self.pos,
                "gtid": self.gtid.text if self.gtid is not None else None,
                "fde": self.fde_raw.hex() if self.fde_raw else None,
                "fde_checksum": self.fde.checksum,
                "txn": self.txn.to_json() if self.txn else None,
                "maps": {str(k): v.hex() for k, v in self.maps_raw.items()}}

    def restore(self, st: dict[str, Any]) -> None:
        if st.get("flavor") != self.flavor:
            raise FramingError(f"batch flavor {st.get('flavor')} != decoder flavor {self.flavor}")
        self.file, self.pos = st.get("file"), int(st.get("pos") or 4)
        g = st.get("gtid")
        self.gtid = None if g is None else (GtidSet.parse(g) if self.flavor == MYSQL else MariaGtidList.parse(g))
        self.fde_raw = bytes.fromhex(st["fde"]) if st.get("fde") else None
        self.fde = E.parse_fde(self.fde_raw) if self.fde_raw else E.FormatDescription(
            checksum=int(st.get("fde_checksum") or 0))
        self.txn = Txn.from_json(st["txn"]) if st.get("txn") else None
        self.maps, self.maps_raw, self._plans = {}, {}, {}
        for k, h in (st.get("maps") or {}).items():
            raw = bytes.fromhex(h)
            self.maps[int(k)] = E.parse_table_map(raw, self.fde)
            self.maps_raw[int(k)] = raw

    # ------------------------------------------------------------------ helpers
    def _ptext(self) -> str:
        return self.position().text

    def _q(self, out: DecodeOut, reason: str, ev: bytes, off: int, schema: str | None = None,
           table: str | None = None) -> None:
        out.quarantined.append(PlainQuarantine(self._ptext(), schema, table, reason, off, len(ev), ev[:256].hex()))
        self._mark(FC.RecordClass.UNSUPPORTED, "quarantined: " + reason.split(":", 1)[0][:80])

    def _mark(self, cls: FC.RecordClass | str, reason: str = "") -> None:
        self._cls = (FC.RecordClass(cls).value if cls != "unexplained" else "unexplained", reason)

    def _sig(self, out: DecodeOut, sig: FC.SourceSignal, typ: int) -> None:
        sig.record = sig.record or f"binlog:{typ}"
        out.signals.append(PlainSignal(self._ptext(), sig))
        if self._cls is None or self._cls[0] != FC.RecordClass.UNSUPPORTED.value:
            self._mark(sig.cls, sig.reason.split(":", 1)[0][:100])

    def _end_txn(self, out: DecodeOut, kind: str, ts: int, off: int, ln: int) -> None:
        t = self.txn
        if t is None:
            return
        cts = None
        if kind == "COMMIT":
            us = t.commit_us
            cts = (datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc) if us
                   else datetime.fromtimestamp(ts, tz=timezone.utc))
        out.events.append(PlainEvent(kind, t.txid, self._ptext(), commit_ts=cts, offset=off, length=ln))
        self.txn = None
        self.maps.clear()
        self.maps_raw.clear()
        self._plans.clear()

    def _abandon(self, out: DecodeOut, off: int) -> None:
        if self.txn is not None:
            self._end_txn(out, "ROLLBACK", 0, off, 0)

    def _begin(self, out: DecodeOut, txid: str, off: int) -> Txn:
        self._abandon(out, off)
        self.txn = Txn(txid)
        return self.txn

    def _implicit(self, h: E.Header) -> Txn:
        if self.txn is None:
            self.txn = Txn(f"{self.file}:{h.log_pos - h.size}")
        return self.txn

    def _spec(self, schema: str, table: str) -> TableSpec | None:
        if self.case_sensitive:            # lower_case_table_names = 0: `T` and `t` are two tables (SRC-my-11)
            return self.specs.get((schema, table))
        return self.specs.get((schema, table)) or self._lower.get((schema.lower(), table.lower()))

    def _plan(self, tid: int) -> list[ColumnPlan] | RowError | None:
        if tid in self._plans:
            return self._plans[tid]
        tm = self.maps[tid]
        spec = self._spec(tm.schema, tm.table)
        res: list[ColumnPlan] | RowError | None
        if spec is None:
            res = None
        else:
            try:
                res = build_plan(tm, spec, self.flavor, self.json_decoder)
            except RowError as e:
                res = e
        self._plans[tid] = res
        return res

    # ------------------------------------------------------------------ feeding
    def feed(self, ev: bytes, off: int = 0, out: DecodeOut | None = None, inner: bool = False) -> DecodeOut:
        out = out if out is not None else DecodeOut()
        self._cls = None
        try:
            h = E.parse_header(ev)
        except E.EventError as e:
            self._q(out, str(e), ev, off)
            self._count("binlog:?")
            return out
        try:
            self._feed(h, ev, off, out, inner)
        finally:
            self._count(f"binlog:{h.type}")
        return out

    def _count(self, rec: str) -> None:
        cls, why = self._cls or (FC.RecordClass.DECODED.value, "")
        if cls == "unexplained":
            self.census.unexplained(rec, why)
        else:
            self.census.count(rec, cls, why)
        self._cls = None

    def _feed(self, h: E.Header, ev: bytes, off: int, out: DecodeOut, inner: bool) -> None:
        typ = h.type
        artificial = bool(h.flags & E.LOG_EVENT_ARTIFICIAL_F) or h.log_pos == 0
        try:
            if typ == E.FORMAT_DESCRIPTION_EVENT:
                self.fde = E.parse_fde(ev)
                self.fde_raw = bytes(ev)
                if not artificial and self.txn is not None:
                    self._abandon(out, off)       # server restarted inside a transaction
                if not artificial:
                    self.pos = h.log_pos
                return
            if typ == E.ROTATE_EVENT:
                r = E.parse_rotate(ev, self.fde, self.fde_raw is not None)
                self.file, self.pos = r.next_file, r.position
                return
            if not inner:
                E.body(ev, self.fde, typ)          # CRC check for every other event
            if typ in (E.HEARTBEAT_LOG_EVENT, E.HEARTBEAT_LOG_EVENT_V2):
                self._mark(FC.RecordClass.IGNORED, "heartbeat")
                return
            if not artificial and not inner:
                self.pos = h.log_pos
            if typ == E.TRANSACTION_PAYLOAD_EVENT:
                self._payload(h, ev, off, out)
                return
            self._dispatch(h, ev, off, out)
        except E.EventError as e:
            self._q(out, str(e), ev, off)
        except (IndexError, ValueError, KeyError, struct.error, AssertionError, TypeError, OverflowError) as e:
            # SRC-my-06: any parse failure of one event = that event quarantined, never a dead generator
            self._q(out, f"malformed event type {typ}: {type(e).__name__}: {e}",
                    ev, off)

    def _payload(self, h: E.Header, ev: bytes, off: int, out: DecodeOut) -> None:
        """Compressed transaction: decode the inner events as if they were in the stream (their own checksums are
        not written; positions stay at the payload event, the resume token is the following commit)."""
        inner = E.parse_transaction_payload(ev, self.fde)
        saved = self.fde
        self.fde = E.FormatDescription(binlog_version=saved.binlog_version, server_version=saved.server_version,
                                       header_len=saved.header_len, post_header=saved.post_header,
                                       checksum=E.CHECKSUM_OFF)
        try:
            for iev in inner:
                self.feed(iev, off, out, inner=True)
        finally:
            self.fde = saved
        self._cls = (FC.RecordClass.DECODED.value, "")

    def _dispatch(self, h: E.Header, ev: bytes, off: int, out: DecodeOut) -> None:
        typ = h.type
        if typ == E.GTID_LOG_EVENT or typ == E.ANONYMOUS_GTID_LOG_EVENT:
            g = E.parse_mysql_gtid(ev, self.fde, typ == E.ANONYMOUS_GTID_LOG_EVENT)
            if g.anonymous or g.gno <= 0:
                t = self._begin(out, f"{self.file}:{h.log_pos - h.size}", off)
            else:
                t = self._begin(out, f"{g.sid}:{g.gno}", off)
                if isinstance(self.gtid, GtidSet):
                    self.gtid.add(g.sid, g.gno)
            t.commit_us = g.commit_us
            return
        if typ == E.GTID_TAGGED_LOG_EVENT:
            tg = E.parse_tagged_gtid(ev, self.fde)
            t = self._begin(out, f"{tg.sid}:{tg.tag}:{tg.gno}", off)
            if isinstance(self.gtid, GtidSet):
                self.gtid.add(f"{tg.sid}:{tg.tag}", tg.gno)
            t.commit_us = tg.commit_us
            return
        if typ == E.MARIA_GTID_EVENT:
            g2 = E.parse_maria_gtid(ev, self.fde)
            t = self._begin(out, f"{g2.domain}-{g2.server}-{g2.seq}", off)
            t.began = not g2.standalone
            t.standalone = g2.standalone
            if isinstance(self.gtid, MariaGtidList):
                self.gtid.set(g2.domain, g2.server, g2.seq)
            return
        if typ in (E.QUERY_EVENT, E.QUERY_COMPRESSED_EVENT):
            self._query(h, E.parse_query(ev, self.fde), ev, off, out)
            return
        if typ == E.XID_EVENT:
            if self.txn is None:
                return
            self._end_txn(out, "COMMIT", h.timestamp, off, len(ev))
            return
        if typ == E.XA_PREPARE_LOG_EVENT:
            t = self._implicit(h)
            self._q(out, f"XA transaction {t.txid} not supported (discarded; re-snapshot the affected tables)", ev, off)
            self._sig(out, FC.unsupported(None, f"XA PREPARE of {t.txid}: the commit is a separate XA COMMIT that "
                                                "cannot be matched; changes of the XA transaction not applied"), typ)
            self._end_txn(out, "ROLLBACK", h.timestamp, off, len(ev))
            return
        if typ == E.TABLE_MAP_EVENT:
            tm = E.parse_table_map(ev, self.fde)
            self.maps[tm.table_id] = tm
            self.maps_raw[tm.table_id] = bytes(ev)
            if tm.columns:
                self.tm_named = all(c.name for c in tm.columns)
            self._plans.pop(tm.table_id, None)
            return
        if typ in E.ROWS_TYPES:
            self._rows(h, ev, off, out)
            return
        if typ == E.INCIDENT_EVENT:
            self._q(out, "INCIDENT event: the source reports possibly lost binlog events (re-snapshot)", ev, off)
            self._sig(out, FC.unsupported(None, "INCIDENT event: the source reports possibly lost binlog events"), typ)
            return
        if typ == E.START_ENCRYPTION_EVENT:
            self._q(out, f"event type {typ} (binlog encryption) is not supported", ev, off)
            self._sig(out, FC.unsupported(None, "START_ENCRYPTION event in the dump stream"), typ)
            return
        ignored = {E.ANNOTATE_ROWS_EVENT: "statement text annotation", E.ROWS_QUERY_LOG_EVENT: "statement text annotation",
                   E.PREVIOUS_GTIDS_LOG_EVENT: "binlog file header (GTID state is tracked from GTID events)",
                   E.GTID_LIST_EVENT: "binlog file header (GTID state is tracked from GTID events)",
                   E.BINLOG_CHECKPOINT_EVENT: "binlog checkpoint (crash recovery marker)",
                   E.STOP_EVENT: "server stop marker", E.INTVAR_EVENT: "statement-based context (row format used)",
                   E.RAND_EVENT: "statement-based context (row format used)",
                   E.USER_VAR_EVENT: "statement-based context (row format used)",
                   E.TRANSACTION_CONTEXT_EVENT: "group replication certification info",
                   E.VIEW_CHANGE_EVENT: "group replication view change"}
        if typ in ignored:
            self._mark(FC.RecordClass.IGNORED, ignored[typ])
            return
        if h.flags & 0x80:          # LOG_EVENT_IGNORABLE_F
            self._mark(FC.RecordClass.IGNORED, "LOG_EVENT_IGNORABLE_F set by the server")
            return
        self._q(out, f"unknown binlog event type {typ}", ev, off)
        self._sig(out, FC.unsupported(None, f"unknown binlog event type {typ}"), typ)
        self._mark("unexplained", f"unknown binlog event type {typ}")

    def _query(self, h: E.Header, q: E.Query, ev: bytes, off: int, out: DecodeOut) -> None:
        sql = q.sql.strip().rstrip(";").strip()
        up = sql.upper()
        if up == "BEGIN" or up.startswith("XA START"):
            t = self.txn or self._begin(out, f"{self.file}:{h.log_pos - h.size}", off)
            t.began = True
            if up.startswith("XA START"):
                t.xa = True
            return
        if up == "COMMIT" or up.startswith("XA COMMIT"):
            self._end_txn(out, "COMMIT", h.timestamp, off, len(ev))
            return
        if up == "ROLLBACK" or up.startswith("XA ROLLBACK"):
            t0 = self.txn
            if t0 is not None and t0.touched and not up.startswith("XA "):
                # XC-35: the server logs a ROLLBACK only when the transaction changed a non-transactional table
                # (MyISAM, MEMORY, ...): those rows persist on the source although the transaction is discarded.
                # Which rows survived is not in the binlog -> hold + resync every table the transaction touched.
                for s_, n_ in dict.fromkeys(t0.touched):
                    self._sig(out, FC.unsupported(TableRef(schema=s_, name=n_),
                                                  "ROLLBACK of a transaction with non-transactional changes (the "
                                                  "binlog logs ROLLBACK only then): rows of non-transactional tables "
                                                  "persist on the source - hold + resync; use InnoDB"), h.type)
            self._end_txn(out, "ROLLBACK", h.timestamp, off, len(ev))
            return
        m = _SAVEPOINT.match(sql)
        if m and self.txn is not None:
            self.txn.savepoints.append((m.group(1), self.txn.ndml))
            self._mark(FC.RecordClass.IGNORED, "SAVEPOINT marker (kept for ROLLBACK TO)")
            return
        m = _ROLLBACK_TO.match(sql)
        if m and self.txn is not None:
            self._rollback_to(m.group(1), off, len(ev), out)
            return
        if up.startswith("XA "):
            return
        if _STMT_DML.match(sql):                  # statement-based logging (binlog_format STATEMENT / MIXED)
            t = self.txn if self.txn is not None else self._begin(out, f"{self.file}:{h.log_pos - h.size}", off)
            mt = _STMT_TABLE.search(sql)
            ref = None
            if mt:
                s_, n_ = _split_name(mt.group(1), q.schema)
                if self._spec(s_, n_) is None:
                    self._mark(FC.RecordClass.IGNORED, "statement-logged DML on a table not captured")
                    if not t.began:
                        self._end_txn(out, "COMMIT", h.timestamp, off, len(ev))
                    return
                ref = TableRef(schema=s_, name=n_)
            self._sig(out, FC.unsupported(ref, f"statement-logged DML (binlog_format STATEMENT/MIXED): row values are "
                                               f"not in the binlog - hold + resync; set binlog_format=ROW "
                                               f"[{sql[:120]}]"), h.type)
            if not t.began:
                self._end_txn(out, "COMMIT", h.timestamp, off, len(ev))
            return
        # any other statement: DDL (or a statement logged inside a transaction)
        t = self.txn if self.txn is not None else self._begin(out, f"{self.file}:{h.log_pos - h.size}", off)
        schema = table = None
        md = _DDL_TABLE.match(sql)
        if md:
            schema, table = _split_name(md.group(2), q.schema)
        kind = "TRUNCATE" if md and md.group(1).upper() == "TRUNCATE" else "DDL"
        if kind == "TRUNCATE" and schema and table and self._spec(schema, table) is None:
            kind = "DDL"
        if (kind == "DDL" and schema and table and md and md.group(1).upper() in ("ALTER", "DROP", "RENAME", "CREATE")
                and self._spec(schema, table) is not None and self.tm_named is not True and self.in_catchup_window()):
            # SRC-my-03: without column names in the TABLE_MAP (binlog_row_metadata=MINIMAL) rows are matched to the
            # catalog by position. The catalog was read AFTER this DDL, so rows of this table logged before it (in
            # this or an earlier run of the same window) may have been matched to the post-DDL shape: hold + resync.
            self._sig(out, FC.unsupported(TableRef(schema=schema, name=table),
                                          "DDL inside the catch-up window with binlog_row_metadata=MINIMAL: earlier "
                                          "rows were matched to a later catalog by position - hold + resync; set "
                                          "binlog_row_metadata=FULL"), h.type)
        note = sql if len(sql) <= 2000 else sql[:2000] + "..."
        out.events.append(PlainEvent(kind, t.txid, self._ptext(), schema, table, offset=off, length=len(ev),
                                     note=note if kind == "DDL" else None))
        if kind == "DDL":
            self.maps.clear()
            self.maps_raw.clear()
            self._plans.clear()
        if not t.began:
            self._end_txn(out, "COMMIT", h.timestamp, off, len(ev))

    def _rollback_to(self, name: str, off: int, ln: int, out: DecodeOut) -> None:
        t = self.txn
        assert t is not None
        idx = next((i for i in range(len(t.savepoints) - 1, -1, -1) if t.savepoints[i][0].lower() == name.lower()),
                   None)
        if idx is None:
            return
        mark = t.savepoints[idx][1]
        del t.savepoints[idx + 1:]
        n_undo = t.ndml - mark
        undo = t.dml_tables[len(t.dml_tables) - n_undo:] if n_undo else []
        for schema, table in reversed(undo):
            out.events.append(PlainEvent("COMPENSATION", t.txid, self._ptext(), schema, table, offset=off, length=ln,
                                         note=f"rollback to savepoint {name}"))
        t.ndml = mark
        if n_undo:
            del t.dml_tables[len(t.dml_tables) - n_undo:]

    def _rows(self, h: E.Header, ev: bytes, off: int, out: DecodeOut) -> None:
        rh = E.parse_rows_header(ev, self.fde, h.type)
        tm = self.maps.get(rh.table_id)
        t = self._implicit(h)
        if tm is None:
            self._q(out, f"rows event for unknown table id {rh.table_id} (no TABLE_MAP)", ev, off)
            self._sig(out, FC.unsupported(None, f"rows event for unknown table id {rh.table_id} (no TABLE_MAP in "
                                                "this transaction): table not attributable"), h.type)
            return
        plan = self._plan(rh.table_id)
        if plan is None:
            out.skipped += 1          # table not in the captured catalog
            self._mark(FC.RecordClass.IGNORED, "table not captured")
            return
        if not self.decode_rows:
            return
        ref = TableRef(schema=tm.schema, name=tm.table)
        if h.type == E.PARTIAL_UPDATE_ROWS_EVENT:
            self._sig(out, FC.unsupported(ref, "partial JSON update (binlog_row_value_options=PARTIAL_JSON): the "
                                               "JSON diff format is not in the reference manual - hold + resync; "
                                               "set binlog_row_value_options=''"), h.type)
            return
        if isinstance(plan, RowError) or len(plan) != rh.ncols:
            why = str(plan) if isinstance(plan, RowError) else f"rows event has {rh.ncols} columns, catalog {len(plan)}"
            self._drift(out, h, rh, tm, ref, why)
            return
        kind = "INSERT" if h.type in E.WRITE_TYPES else "UPDATE" if h.type in E.UPDATE_TYPES else "DELETE"
        b = rh.body
        p = rh.data_off
        decoded: list[PlainEvent] = []
        pos = self._ptext()
        try:
            while p < len(b):
                before = after = None
                notes: list[str] = []
                if kind == "INSERT":
                    after, p, notes = read_image(b, p, plan, rh.present)
                elif kind == "DELETE":
                    before, p, notes = read_image(b, p, plan, rh.present)
                else:
                    before, p, n1 = read_image(b, p, plan, rh.present)
                    after, p, n2 = read_image(b, p, plan, rh.present_after or rh.present)
                    notes = n1 + n2
                decoded.append(PlainEvent(kind, t.txid, pos, tm.schema, tm.table, before, after, offset=off,
                                          length=len(ev), note="; ".join(notes) if notes else None))
        except RowError as e:
            self._q(out, f"{e} (row {len(decoded) + 1} of the event)", ev, off, tm.schema, tm.table)
            return
        out.events.extend(decoded)
        if h.type in E.COMPRESSED_ROWS:
            self._mark(FC.RecordClass.DECODED)
        if decoded and (tm.schema, tm.table) not in t.touched:
            t.touched.append((tm.schema, tm.table))
        if t.savepoints:
            t.dml_tables.extend((tm.schema, tm.table) for _ in decoded)
        t.ndml += len(decoded)


def _drift_method(self: BinlogDecoder, out: DecodeOut, h: E.Header, rh: E.RowsHeader, tm: E.TableMap, ref: TableRef,
                  why: str) -> None:
    """TABLE_MAP and catalog disagree (DDL between the row event and the catalog read, F-07): never decode against the
    wrong shape. Refresh the catalog; re-read the rows by key when the key can be decoded from the TABLE_MAP's own
    metadata (column names + primary key: binlog_row_metadata=FULL), else hold the table."""
    self._sig(out, FC.catalog_refresh(ref, f"catalog drift: {why}"), h.type)
    keys: list[dict[str, Any]] | None = None
    if tm.pk and all(c.name for c in tm.columns):
        try:
            plan = plan_without_catalog(tm)
            names = [plan[i].name for i in tm.pk]
            keys = []
            b, p = rh.body, rh.data_off
            while p < len(b):
                img, p, _ = read_image(b, p, plan, rh.present)
                if h.type in E.UPDATE_TYPES:
                    keys.append({n: img[n] for n in names})
                    img, p, _ = read_image(b, p, plan, rh.present_after or rh.present)
                if any(n not in img for n in names):
                    raise RowError("key column not in the row image")
                keys.append({n: img[n] for n in names})
        except (RowError, IndexError, KeyError):
            keys = None
    if keys:
        uniq = [dict(x) for x in {tuple(sorted(k.items())): k for k in keys}.values()]
        self._sig(out, FC.keyed_reread(ref, uniq, f"catalog drift: row images not decoded against a different "
                                                  f"shape ({why})"), h.type)
    else:
        self._sig(out, FC.unsupported(ref, f"catalog drift and no key decodable from the TABLE_MAP ({why})"), h.type)


BinlogDecoder._drift = _drift_method  # type: ignore[attr-defined]


def specs_from_catalog(catalog: dict[str, Any]) -> dict[tuple[str, str], TableSpec]:
    """CatalogSnapshot (model_dump(by_alias=True)) -> decoder table specs."""
    out: dict[tuple[str, str], TableSpec] = {}
    for t in catalog.get("tables", []):
        ref = t["table"]
        schema, name = ref.get("schema") or ref.get("schema_"), ref["name"]
        cols = [ColSpec(c["name"], int(c["ordinal"]), c["engine_type"], str(c["canonical_type"]).split(".")[-1],
                        c.get("codepage"), c.get("key_seq"), c.get("length")) for c in t.get("columns", [])]
        out[(schema, name)] = TableSpec(schema, name, cols)
    return out


def decode_batch(raw: bytes, flavor: str, specs: dict[tuple[str, str], TableSpec]) -> DecodeOut:
    """Decode one framed raw batch (framing.py). Never raises: bad frames become quarantine records (R7)."""
    out = DecodeOut()
    dec = BinlogDecoder(flavor, specs)
    try:
        dec.restore(read_context(raw))
        for kind, off, payload in iter_frames(raw):
            if kind == CONTEXT:
                continue
            if kind != EVENT:
                out.quarantined.append(PlainQuarantine(dec._ptext(), None, None, f"unknown frame kind {kind}", off,
                                                       len(payload), payload[:256].hex()))
                continue
            dec.feed(payload, off, out)
    except (FramingError, ValueError, KeyError) as e:
        out.quarantined.append(PlainQuarantine(None, None, None, f"undecodable batch: {e}", 0, len(raw),
                                               raw[:256].hex()))
    return out


def plan_without_catalog(tm: E.TableMap) -> list[ColumnPlan]:
    return plan_from_defs(tm.columns)

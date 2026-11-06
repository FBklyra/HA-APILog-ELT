"""Raw db2ReadLog buffer -> plain change events + record census (stdlib only; never raises on bad input).

Every record read is put in exactly one class (alelt_common.completeness.RecordClass, X-132; see ../DESIGN.md §9):
decoded / ignored(reason) / needs_keyed_reread / needs_catalog_refresh / unsupported, or `unexplained` (no rule:
also emitted as an `unsupported` signal, never silent). connector.py wraps events into alelt_common ChangeEvents
(signals into `alelt-signal:` events) and quarantines into QuarantineRecords.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, MutableSet
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from . import framing as F
from .position import Lri
from .rowimage import ColumnSpec, Err, RowImageError, decode_row, outer_class

UNDO_NAME = {F.F_UNDO_INSERT: "INSERT", F.F_UNDO_DELETE: "DELETE", F.F_UNDO_UPDATE: "UPDATE",
             F.F_UNDO_DELETE_EMPTY: "DELETE", F.F_UNDO_INSERT_EMPTY: "INSERT"}
VALUE_COMPRESSION = frozenset({"V", "B"})     # SYSCAT.TABLES.COMPRESSION: different row format

# record classes (values of alelt_common.completeness.RecordClass; kept as str: this module is stdlib only)
DECODED, IGNORED = "decoded", "ignored"
REREAD, REFRESH, UNSUPPORTED, UNEXPLAINED = "needs_keyed_reread", "needs_catalog_refresh", "unsupported", "unexplained"

DML_FUNCS = frozenset({F.F_INSERT, F.F_DELETE, F.F_UPDATE, F.F_INSERT_EMPTY, F.F_DELETE_EMPTY}) | set(UNDO_NAME)
INSERTS = frozenset({F.F_INSERT, F.F_INSERT_EMPTY})
# DMS functions that change the table definition (IBM "Data manager log records"): catalog must be re-read
DMS_DDL = {102: "add columns", 104: "undo add columns", 113: "alter column", 115: "undo alter column",
           116: "alter LOB column information", 119: "undo alter LOB column information", 122: "rename table/schema",
           123: "undo rename", 124: "alter table attribute", 169: "drop columns", 170: "undo drop columns",
           171: "change row version", 172: "undo change row version",
           108: "undo update partition state"}
# Update partition state (137) partition action: 1 add partition (empty) -> catalog; attach / SET INTEGRITY after attach /
# detach make rows appear or disappear without row images -> hold + resync
PART_ADD = 1
DMS_IGNORED = {211: "start of out-of-row data marker (6-byte header only; LOB / long field / XML records follow)",
               212: "undo of start of out-of-row data marker",
               128: "initialize table (CREATE TABLE; the DOM create-table record carries the event)"}
DOM_IGNORED = {2: "create index (no row data)", 3: "drop index (no row data)", 35: "reorg table (no row change)",
               130: "undo create table", 5: "undo drop table"}
LOB_IGNORED = {66: "LOB manager: delete LOB data (information only)",
               67: "LOB manager: non-update LOB data (information only)"}
LOB_DATA = frozenset({64, 65})                 # add LOB data / add LOB amount
LF_IGNORED = {114: "long field manager: delete long field record (information only)",
              115: "long field manager: non-update long field record (information only)"}
LF_DATA = frozenset({113})                     # add long field record
RDS_FUNCS = {4: "DDL statement", 5: "undo DDL statement", 6: "partition information", 8: "partition information"}
# transaction-end records (IBM "Transaction manager log records"): body offset of the u64 commit time (None: no time)
COMMITS = {0x0084: ("commit", 0), 0x008A: ("local pending list commit", 0), 0x0087: ("heuristic commit", 0),
           0x0085: ("MPP subordinator commit", 0), 0x0086: ("MPP coordinator commit", 0),
           0x0051: ("global pending list commit", None)}
ABORTS = {0x0041: "abort", 0x0049: "heuristic abort"}
# transaction / utility manager records without row data (IBM "Log record header" record types)
RT_IGNORED = {0x0042: "backout free", 0x0046: "subtransaction", 0x0069: "information only (ignored by recovery)",
              0x007E: "federated prepare (outcome record follows)", 0x0055: "undo record (no row data)",
              0x0052: "redo record (no row data)",
              0x0072: "timestamp", 0x008B: "application information", 0x0091: "topology change",
              0x007B: "MPP prepare (outcome record follows)", 0x007C: "XA prepare (outcome record follows)",
              0x007D: "TM prepare (outcome record follows)", 0x004F: "backup end", 0x006F: "backup start",
              0x0056: "system catalog migration begin", 0x0057: "system catalog migration end",
              0x0092: "database migration begin", 0x0093: "database migration end",
              0x0089: "table space rollforward to PIT starts", 0x0071: "table space rollforward to PIT ends"}
R_LOAD = "LOAD on a captured table: loaded rows are not logged (IBM: resynchronize the table)"

R_NOT_CAPTURED = "not captured"
R_NOT_CAPTURED_TBSP = "not captured (table space without captured tables)"
R_INTERNAL = "internal metadata record (record type 0 per IBM docs)"
R_OVERFLOW_INSERT = "overflow record carrying the after image of the following update (IBM: x04 bit)"
R_POINTER_PAIRED = "overflow pointer record; the row image is in the adjacent overflow record"
R_OVERFLOW_UNDO = "undo of an overflow record placement (the paired undo-update restores the row)"
R_OUT_OF_ROW = "value stored out of row (LOB / varchar / LONG / DECFLOAT / XML): re-read the row by key (X-121)"
R_ORDER = "log records not in LRI order (db2ReadLog merges streams in LRI order): batch not decodable"


@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    tbspaceid: int
    tableid: int
    columns: tuple[ColumnSpec, ...]
    compression: str | None = None
    key: tuple[str, ...] = ()                  # primary key (or first unique index) columns, key order


@dataclass
class RawSignal:
    """A alelt_common.completeness.SourceSignal in stdlib form (cls = RecordClass value)."""

    cls: str
    reason: str
    keys: list[dict[str, Any]] | None = None
    record: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class RawEvent:
    lri: str
    txid: str
    kind: str                                  # ChangeKind value
    offset: int
    length: int
    schema: str | None = None
    table: str | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    commit_ts: datetime | None = None
    note: str | None = None
    signal: RawSignal | None = None            # set: this event is a signal (kind OTHER, DDL for catalog refresh)


@dataclass
class RawQuarantine:
    lri: str | None
    reason: str
    offset: int
    length: int
    schema: str | None = None
    table: str | None = None
    raw_hex: str | None = None


@dataclass
class RawDecode:
    events: list[RawEvent] = field(default_factory=list)
    quarantined: list[RawQuarantine] = field(default_factory=list)
    skipped: int = 0                           # records classified `ignored`
    census: Counter[tuple[str, str, str]] = field(default_factory=Counter)   # (record kind, class, reason) -> n
    foreign_ids: set[tuple[int, int]] = field(default_factory=set)   # ids proven out of scope (not in a later catalog)
    created_ids: set[tuple[int, int]] = field(default_factory=set)   # ids (re)created in this buffer

    @property
    def signals(self) -> list[RawEvent]:
        return [e for e in self.events if e.signal is not None]


def _join(notes: list[str]) -> str | None:
    return ";".join(notes) if notes else None


@dataclass
class _Info:
    rec: F.LogRecord
    h: F.DmsHeader | None = None
    imgs: F.DmsImages | None = None
    layout_err: str | None = None
    multi: list[tuple[bytes, bytes | None]] | None = None

    @property
    def outer(self) -> tuple[int, ...]:
        if self.imgs is None:
            return ()
        return tuple(x[0] for x in (self.imgs.before, self.imgs.after) if x is not None)

    def oc(self, side: str) -> str | None:
        img = getattr(self.imgs, side) if self.imgs is not None else None
        return outer_class(img[0]) if img else None

    def label(self) -> str:
        r = self.rec
        s = f"0x{r.rtype:04X}"
        if self.h is not None:
            s += f"/{self.h.component}/{self.h.function}"
            if self.outer:
                s += "/" + ">".join(str(o) for o in self.outer)
        return s


def decode_buffer(raw: bytes, tables: Mapping[tuple[int, int], TableSpec], hex_bytes: int = 256,
                  catalog_lri: str | None = None,
                  foreign: MutableSet[tuple[int, int]] | None = None) -> RawDecode:
    """catalog_lri: position the catalog snapshot is valid at; foreign: ids known to be out of scope (read only)."""
    return _Decoder(raw, tables, hex_bytes, catalog_lri, foreign).run()


class _Decoder:
    def __init__(self, raw: bytes, tables: Mapping[tuple[int, int], TableSpec], hex_bytes: int,
                 catalog_lri: str | None, foreign: MutableSet[tuple[int, int]] | None) -> None:
        self.raw, self.tables, self.hex_bytes = raw, tables, hex_bytes
        self.cat_lri = Lri.parse(catalog_lri) if catalog_lri else None
        self.foreign = foreign if foreign is not None else set()
        self.tbspaces = {ids[0] for ids in tables}
        self.unmapped = sorted({f"{t.schema}.{t.name}" for ids, t in tables.items() if ids[0] < 0 or ids[1] < 0})
        self.out = RawDecode()
        self.refreshed: set[tuple[int, int]] = set()

    # ------------------------------------------------------------------ pass 1: frame + pair
    def run(self) -> RawDecode:
        infos: list[_Info] = []
        it = F.iter_records(self.raw)
        while True:
            try:
                rec = next(it)
            except StopIteration:
                break
            except F.FramingError as e:
                self.out.quarantined.append(RawQuarantine(None, f"framing: {e}", e.offset, len(self.raw) - e.offset,
                                                          raw_hex=self.raw[e.offset:e.offset + self.hex_bytes].hex()))
                self.out.census[("framing", UNSUPPORTED, "buffer cannot be framed")] += 1
                break
            infos.append(self._info(rec))
        self.infos = infos
        for a, b in zip(infos, infos[1:]):
            if not a.rec.lri < b.rec.lri:      # multiple log streams are merged by LRI: anything else is corrupt
                self.out.quarantined.append(RawQuarantine(b.rec.lri.text, R_ORDER, b.rec.offset, b.rec.length,
                                                          raw_hex=b.rec.record[:self.hex_bytes].hex()))
                self.out.census[("lri-order", UNSUPPORTED, R_ORDER)] += 1
                self.out.events.append(RawEvent(b.rec.lri.text, b.rec.txid, "OTHER", b.rec.offset, b.rec.length,
                                                signal=RawSignal(UNSUPPORTED, R_ORDER, None, "db2:lri-order")))
                return self.out
        self._pair()
        for i, inf in enumerate(infos):
            try:
                self._one(i, inf)
            except Exception as e:  # noqa: BLE001 - R7: a bad record never stops the batch
                r = inf.rec
                self.out.quarantined.append(RawQuarantine(r.lri.text, f"internal: {type(e).__name__}: {e}",
                                                          r.offset, r.length, raw_hex=r.record[:self.hex_bytes].hex()))
                self._count(inf, UNSUPPORTED, "internal decoder error")
        return self.out

    @staticmethod
    def _info(rec: F.LogRecord) -> _Info:
        inf = _Info(rec)
        if rec.rtype in (F.RT_NORMAL, F.RT_CLR, F.RT_REDO, F.RT_UNDO, 0x0069):
            inf.h = F.dms_header(rec.body)
        if (inf.h is not None and inf.h.component == F.COMP_DMS and rec.rtype in (F.RT_NORMAL, F.RT_CLR)
                and inf.h.function in (F.F_INSERT_MULTI, F.F_UNDO_INSERT_MULTI)):
            try:
                inf.multi = F.dms_multi(rec.body, inf.h.function)
            except F.DmsLayoutError as e:
                inf.layout_err = str(e)
        elif (inf.h is not None and inf.h.component == F.COMP_DMS and rec.rtype in (F.RT_NORMAL, F.RT_CLR)
                and inf.h.function in DML_FUNCS):
            try:
                inf.imgs = F.dms_images(rec.body, inf.h)
            except F.DmsLayoutError as e:
                inf.layout_err = str(e)
        return inf

    def _pair(self) -> None:
        """Overflow (x04) / pointer (x02) pairing inside one transaction (IBM docs, Update record notes)."""
        by_tx: dict[str, list[int]] = {}
        for i, inf in enumerate(self.infos):
            by_tx.setdefault(inf.rec.txid, []).append(i)
        self.after_from: dict[int, int] = {}       # update idx -> overflow insert idx carrying its after image
        self.consumed: set[int] = set()            # overflow inserts used by an update
        self.pointer_paired: set[int] = set()
        self.overflow_undo: set[int] = set()       # undo-insert CLRs of consumed overflow inserts
        self.tx_end: dict[str, int] = {}
        self.tx_of: dict[int, tuple[str, int]] = {}
        for tid, idx in by_tx.items():
            for pos, i in enumerate(idx):
                self.tx_of[i] = (tid, pos)
                if self.infos[i].rec.rtype in (F.RT_COMMIT, F.RT_PENDING_COMMIT, F.RT_ABORT):
                    self.tx_end[tid] = i
        self.by_tx = by_tx
        for tid, idx in by_tx.items():
            rids: set[tuple[tuple[int, int], bytes]] = set()
            for pos, i in enumerate(idx):
                u = self.infos[i]
                if u.imgs is None or u.h is None:
                    continue
                f, clr, ids = u.h.function, u.rec.rtype == F.RT_CLR, (u.h.tbspaceid, u.h.tableid)
                if f == F.F_UPDATE and not clr and u.oc("after") == "pointer":
                    for j in reversed(idx[:pos]):
                        v = self.infos[j]
                        if (j not in self.consumed and v.h is not None and v.rec.rtype == F.RT_NORMAL
                                and v.h.function in INSERTS and (v.h.tbspaceid, v.h.tableid) == ids
                                and v.oc("after") == "overflow"):
                            self.after_from[i] = j
                            self.consumed.add(j)
                            r = F.rid(v.rec.body)
                            if r is not None:
                                rids.add((ids, r))
                            break
                elif f != F.F_UPDATE and "pointer" in (u.oc("before"), u.oc("after")):
                    for j in (idx[pos - 1] if pos else None, idx[pos + 1] if pos + 1 < len(idx) else None):
                        v = self.infos[j] if j is not None else None
                        if (v is not None and v.h is not None and v.h.function == f and v.rec.rtype == u.rec.rtype
                                and (v.h.tbspaceid, v.h.tableid) == ids
                                and "overflow" in (v.oc("before"), v.oc("after"))):
                            self.pointer_paired.add(i)
                            break
            for i in idx:
                u = self.infos[i]
                if (u.h is not None and u.rec.rtype == F.RT_CLR and u.h.component == F.COMP_DMS
                        and u.h.function in (F.F_UNDO_INSERT, F.F_UNDO_INSERT_EMPTY)):
                    r = F.rid(u.rec.body)
                    if r is not None and ((u.h.tbspaceid, u.h.tableid), r) in rids:
                        self.overflow_undo.add(i)

    # ------------------------------------------------------------------ pass 2: classify + emit
    def _count(self, inf: _Info, cls: str, reason: str = "") -> None:
        self.out.census[(inf.label(), cls, "" if cls == DECODED else reason)] += 1
        if cls == IGNORED:
            self.out.skipped += 1

    def _quarantine(self, inf: _Info, reason: str, t: TableSpec | None = None) -> None:
        r = inf.rec
        self.out.quarantined.append(RawQuarantine(r.lri.text, reason, r.offset, r.length, t.schema if t else None,
                                                  t.name if t else None, r.record[:self.hex_bytes].hex()))
        self._count(inf, UNSUPPORTED, reason.split(":")[0][:80])

    def _signal(self, inf: _Info, cls: str, reason: str, t: TableSpec | None, keys: list[dict[str, Any]] | None = None,
                **detail: Any) -> None:
        r = inf.rec
        detail.setdefault("lri", r.lri.text)
        kind = "DDL" if cls == REFRESH else "OTHER"
        self.out.events.append(RawEvent(r.lri.text, r.txid, kind, r.offset, r.length, t.schema if t else None,
                                        t.name if t else None,
                                        signal=RawSignal(cls, reason, keys, f"db2:{inf.label()}", detail)))

    def _unexplained(self, inf: _Info, t: TableSpec | None = None) -> None:
        reason = f"no rule for db2 record {inf.label()}"
        self._count(inf, UNEXPLAINED, reason)
        self._signal(inf, UNSUPPORTED, reason, t)

    def _unknown_table(self, inf: _Info, ids: tuple[int, int], create: bool = False) -> None:
        """Record of a table id that is not in the catalog (F-07)."""
        if self.unmapped:
            # partitioned tables in the catalog without data-partition object ids: the id may be one of their partitions
            reason = "catalog lacks data-partition ids of partitioned table(s): " + ", ".join(self.unmapped[:5])
            self._count(inf, UNSUPPORTED, "catalog lacks data-partition ids of partitioned table(s)")
            if ids not in self.refreshed:
                self.refreshed.add(ids)
                self._signal(inf, UNSUPPORTED, reason, None, tbspaceid=ids[0], tableid=ids[1])
            return
        if ids[0] not in self.tbspaces:
            self._count(inf, IGNORED, R_NOT_CAPTURED_TBSP)
            return
        if not create and ids in self.foreign:
            self._count(inf, IGNORED, R_NOT_CAPTURED)
            return
        if self.cat_lri is not None and inf.rec.lri <= self.cat_lri:
            # a catalog taken later does not contain the id: out of scope, or dropped before the snapshot
            self.out.foreign_ids.add(ids)
            self._count(inf, IGNORED, R_NOT_CAPTURED)
            return
        reason = "unknown table id after the catalog position (new table, possibly captured)"
        self._count(inf, REFRESH, reason)
        if ids not in self.refreshed:
            self.refreshed.add(ids)
            self._signal(inf, REFRESH, reason, None, tbspaceid=ids[0], tableid=ids[1])

    def _one(self, i: int, inf: _Info) -> None:
        rec = inf.rec
        lri, tid = rec.lri.text, rec.txid
        if rec.sqlcode != 0:
            h = F.dms_header(rec.body)
            t = self.tables.get((h.tbspaceid, h.tableid)) if h else None
            self._quarantine(inf, f"db2ReadLog could not decompress record (sqlcode {rec.sqlcode})", t)
            return
        if rec.rtype in COMMITS:
            what, toff = COMMITS[rec.rtype]
            secs = F.commit_time(rec.body[toff:]) if toff is not None else None
            ts = datetime.fromtimestamp(secs, timezone.utc) if secs is not None else None
            self.out.events.append(RawEvent(lri, tid, "COMMIT", rec.offset, rec.length, commit_ts=ts,
                                            note=None if rec.rtype == F.RT_COMMIT else what))
            self._count(inf, DECODED)
            return
        if rec.rtype in ABORTS:
            self.out.events.append(RawEvent(lri, tid, "ROLLBACK", rec.offset, rec.length,
                                            note=None if rec.rtype == F.RT_ABORT else ABORTS[rec.rtype]))
            self._count(inf, DECODED)
            return
        if rec.rtype == F.RT_LOAD_START:
            self._load(inf)
            return
        h = inf.h
        if rec.rtype in (F.RT_NORMAL, F.RT_CLR, F.RT_REDO, F.RT_UNDO, 0x0069) and h is not None and \
                h.component in (F.COMP_DMS, F.COMP_DOM, F.COMP_LOB, F.COMP_LF, F.COMP_RDS):
            pass
        elif rec.rtype in RT_IGNORED:
            self._count(inf, IGNORED, RT_IGNORED[rec.rtype])
            return
        if rec.rtype not in (F.RT_NORMAL, F.RT_CLR, F.RT_REDO, F.RT_UNDO, 0x0069) or h is None:
            self._unexplained(inf)
            return
        if h.component == F.COMP_DMS:
            self._dms(i, inf, h)
        elif h.component == F.COMP_DOM:
            self._dom(inf, h)
        elif h.component in (F.COMP_LOB, F.COMP_LF):
            self._lob(i, inf, h)
        elif h.component == F.COMP_RDS:
            self._rds(inf, h)
        else:
            self._unexplained(inf)

    # ---- utility manager: LOAD start
    def _load(self, inf: _Info) -> None:
        ids = F.load_target(inf.rec.body)
        t = self.tables.get(ids) if ids else None
        if t is not None:
            self._count(inf, UNSUPPORTED, R_LOAD)
            self._signal(inf, UNSUPPORTED, R_LOAD, t)
        elif ids is None:
            self._count(inf, UNSUPPORTED, "load start record too short")
            self._signal(inf, UNSUPPORTED, "load start record too short: LOAD target unknown", None)
        else:
            self._unknown_table(inf, ids)

    # ---- relation manager (component 11): DDL statement / partition information
    def _rds(self, inf: _Info, h: F.DmsHeader) -> None:
        f = h.function
        if f not in RDS_FUNCS:
            self._unexplained(inf)
            return
        if f == 5:
            self._count(inf, IGNORED, "undo DDL statement")
            return
        t = self.tables.get((h.tbspaceid, h.tableid))
        if t is None:
            self._unknown_table(inf, (h.tbspaceid, h.tableid))
            return
        self._count(inf, REFRESH, RDS_FUNCS[f])
        self._signal(inf, REFRESH, RDS_FUNCS[f], t)

    # ---- DOM (component 4): table / index DDL, truncate, reorg
    def _dom(self, inf: _Info, h: F.DmsHeader) -> None:
        ids = (h.tbspaceid, h.tableid)
        f = h.function
        if f in DOM_IGNORED:
            self._count(inf, IGNORED, DOM_IGNORED[f])
            return
        t = self.tables.get(ids)
        if f == 101:                                    # create table
            self.out.created_ids.add(ids)
            if t is None:
                self._unknown_table(inf, ids, create=True)
            elif self.cat_lri is not None and inf.rec.lri <= self.cat_lri:
                self._count(inf, IGNORED, "create table before the catalog snapshot")
            else:
                self._count(inf, REFRESH, "table id (re)created after the catalog position")
                self._signal(inf, REFRESH, "table id (re)created after the catalog position", t)
            return
        if t is None:
            self._unknown_table(inf, ids)
            return
        if f == 4:
            self._count(inf, REFRESH, "drop table of a captured table")
            self._signal(inf, REFRESH, "drop table of a captured table", t)
        elif f == 11:
            reason = "truncate / import replace: rows removed without row images"
            self._count(inf, UNSUPPORTED, reason)
            self._signal(inf, UNSUPPORTED, reason, t)
        elif f == 12:
            reason = "activate not logged initially: changes of this unit of work are not logged"
            self._count(inf, UNSUPPORTED, reason)
            self._signal(inf, UNSUPPORTED, reason, t)
        else:
            self._unexplained(inf, t)

    # ---- LOB manager (component 5)
    def _lob(self, i: int, inf: _Info, h: F.DmsHeader) -> None:
        f = h.function
        lf = h.component == F.COMP_LF
        info_only, data = (LF_IGNORED, LF_DATA) if lf else (LOB_IGNORED, LOB_DATA)
        if f in info_only:
            self._count(inf, IGNORED, info_only[f])
            return
        if f not in data or inf.rec.rtype == 0x0069:
            self._unexplained(inf)
            return
        what = "long field" if lf else "LOB"
        ids = F.lob_parent(inf.rec.body)
        t = self.tables.get(ids) if ids else None
        if t is None:
            if ids is None:
                self._quarantine(inf, "LOB manager record shorter than its 12-byte header")
            else:
                self._unknown_table(inf, ids)
            return
        # the row record (insert / update with an out-of-row LOB descriptor) follows in the same transaction and
        # triggers the keyed re-read; a completed transaction without such a record would lose the LOB change
        tid, pos = self.tx_of[i]
        later = self.by_tx[tid][pos + 1:]
        has_row = any(self.infos[j].h is not None and self.infos[j].h.component == F.COMP_DMS
                      and self.infos[j].imgs is not None
                      and (self.infos[j].h.tbspaceid, self.infos[j].h.tableid) == ids for j in later)
        if not has_row and tid in self.tx_end and self.tx_end[tid] > i:
            reason = f"{what} change without a row record in its transaction"
            self._count(inf, UNSUPPORTED, reason)
            self._signal(inf, UNSUPPORTED, reason, t)
            return
        self._count(inf, IGNORED, f"{what} manager data: the value is re-read by key with its row (X-121)")

    # ---- DMS (component 1)
    def _dms(self, i: int, inf: _Info, h: F.DmsHeader) -> None:
        f = h.function
        if f in DMS_IGNORED:
            self._count(inf, IGNORED, DMS_IGNORED[f])
            return
        ids = (h.tbspaceid, h.tableid)
        if f == F.F_PART_STATE:                     # header = partition object, master table ids at body 6 / 8
            body = inf.rec.body
            master = (int.from_bytes(body[6:8], "little"), int.from_bytes(body[8:10], "little"))
            t = self.tables.get(master) or self.tables.get(ids)
            action = int.from_bytes(body[22:24], "little") if len(body) >= 24 else -1
            if t is None:
                self._unknown_table(inf, master)
            elif action == PART_ADD:
                self._count(inf, REFRESH, "add data partition")
                self._signal(inf, REFRESH, "add data partition", t)
            else:
                reason = f"data partition attach / detach (action {action}): rows change without row images"
                self._count(inf, UNSUPPORTED, "data partition attach / detach")
                self._signal(inf, UNSUPPORTED, reason, t)
            return
        t = self.tables.get(ids)
        if t is None:
            if f in DML_FUNCS or f in DMS_DDL or f in (F.F_INSERT_MULTI, F.F_UNDO_INSERT_MULTI):
                self._unknown_table(inf, ids)
            else:
                self._unexplained(inf)
            return
        if f in DMS_DDL:
            self._count(inf, REFRESH, DMS_DDL[f])
            self._signal(inf, REFRESH, DMS_DDL[f], t)
            return
        if f in (F.F_INSERT_MULTI, F.F_UNDO_INSERT_MULTI) and inf.rec.rtype in (F.RT_NORMAL, F.RT_CLR):
            self._multi(inf, t)
            return
        if f not in DML_FUNCS or inf.rec.rtype not in (F.RT_NORMAL, F.RT_CLR):
            self._unexplained(inf, t)
            return
        self._dml(i, inf, h, t)

    def _multi(self, inf: _Info, t: TableSpec) -> None:
        """Insert multiple records (167): one INSERT per record; undo (168): one undo-INSERT per RID."""
        rec = inf.rec
        if t.compression in VALUE_COMPRESSION:
            self._quarantine(inf, f"value compression ({t.compression}) row format not supported", t)
            return
        if inf.layout_err is not None or inf.multi is None:
            self._quarantine(inf, f"row image: {inf.layout_err}", t)
            return
        clr = rec.rtype == F.RT_CLR
        out: list[RawEvent] = []
        rereads: list[tuple[list[dict[str, Any]] | None, list[str]]] = []
        try:
            for rid, sub in inf.multi:
                if clr:
                    out.append(RawEvent(rec.lri.text, rec.txid, "COMPENSATION", rec.offset, rec.length, t.schema,
                                        t.name, note=f"undo INSERT;target_ref:{rid.hex()}"))
                    continue
                assert sub is not None
                if outer_class(sub[0]) != "user":
                    raise RowImageError(Err.SUBREC_TYPE, f"record type 0x{sub[0]:02X} in insert multiple records")
                row = decode_row(sub, t.columns)
                out.append(RawEvent(rec.lri.text, rec.txid, "INSERT", rec.offset, rec.length, t.schema, t.name,
                                    None, row.values, note=_join(row.notes + [f"row_ref:{rid.hex()}"])))
                gaps = [n.split(":", 1)[1] for n in row.notes if n.startswith(("lob_unavailable:", "value_unavailable:"))]
                if gaps:
                    rereads.append((self._keys(t, row.values), gaps))
        except RowImageError as e:
            self._quarantine(inf, f"row image: {e}", t)
            return
        self.out.events.extend(out)
        for keys, gaps in rereads:
            if keys is None:
                self._signal(inf, UNSUPPORTED, R_OUT_OF_ROW + " - table has no key", t, columns=gaps)
            else:
                self._signal(inf, REREAD, R_OUT_OF_ROW, t, keys, columns=gaps)
        self._count(inf, REREAD if rereads else DECODED, R_OUT_OF_ROW if rereads else "")

    def _keys(self, t: TableSpec, vals: dict[str, Any] | None) -> list[dict[str, Any]] | None:
        if not t.key or vals is None or any(k not in vals for k in t.key):
            return None
        return [{k: vals[k] for k in t.key}]

    def _dml(self, i: int, inf: _Info, h: F.DmsHeader, t: TableSpec) -> None:
        rec = inf.rec
        lri, tid, f = rec.lri.text, rec.txid, h.function
        clr = rec.rtype == F.RT_CLR
        if t.compression in VALUE_COMPRESSION:
            self._quarantine(inf, f"value compression ({t.compression}) row format not supported", t)
            return
        if inf.layout_err is not None:
            self._quarantine(inf, f"row image: {inf.layout_err}", t)
            return
        if i in self.consumed:
            self._count(inf, IGNORED, R_OVERFLOW_INSERT)
            return
        if i in self.pointer_paired:
            self._count(inf, IGNORED, R_POINTER_PAIRED)
            return
        if i in self.overflow_undo:
            self._count(inf, IGNORED, R_OVERFLOW_UNDO)
            return
        if not clr and f in UNDO_NAME:
            self._quarantine(inf, f"undo function {f} outside a CLR", t)
            return
        assert inf.imgs is not None
        b_img, a_img = inf.imgs.before, inf.imgs.after
        if i in self.after_from:
            a_img = self.infos[self.after_from[i]].imgs.after  # type: ignore[union-attr]
        before = after = None
        try:
            if b_img is not None and outer_class(b_img[0]) != "pointer":
                before = decode_row(b_img, t.columns)
            if a_img is not None and outer_class(a_img[0]) != "pointer":
                after = decode_row(a_img, t.columns)
        except RowImageError as e:
            if e.code == Err.NOT_USER_DATA:   # Db2 internal control record (table/page bookkeeping), not a row
                self.out.events.append(RawEvent(lri, tid, "OTHER", rec.offset, rec.length, t.schema, t.name,
                                                note=f"internal control record (dms function {f})"))
                self._count(inf, IGNORED, R_INTERNAL)
            else:
                self._quarantine(inf, f"row image: {e}", t)
            return
        if (b_img is not None and before is None) or (a_img is not None and after is None):
            # unpaired pointer image (its overflow partner is outside this buffer)
            keys = self._keys(t, after.values if after else None) or self._keys(t, before.values if before else None)
            if f == F.F_UPDATE or f in INSERTS:
                reason = "overflow pointer whose overflow record is outside this batch"
                if keys is None:
                    self._count(inf, UNSUPPORTED, reason)
                    self._signal(inf, UNSUPPORTED, reason + " (no key)", t)
                else:
                    self._count(inf, REREAD, reason)
                    self._signal(inf, REREAD, reason, t, keys)
            else:
                reason = "overflow pointer delete / undo without its overflow record"
                self._count(inf, UNSUPPORTED, reason)
                self._signal(inf, UNSUPPORTED, reason, t)
            return
        if f in INSERTS and not clr and a_img is not None and outer_class(a_img[0]) == "overflow":
            # x04 insert not consumed by an update in this buffer: the update is in the next batch
            reason = "overflow record whose update is outside this batch"
            keys = self._keys(t, after.values if after else None)
            self._count(inf, REREAD if keys else UNSUPPORTED, reason)
            self._signal(inf, REREAD if keys else UNSUPPORTED, reason, t, keys)
            return
        notes = (before.notes if before else []) + [n for n in (after.notes if after else [])
                                                     if not before or n not in before.notes]
        if clr:
            kind = "COMPENSATION"
            notes.insert(0, f"undo {UNDO_NAME.get(f, f)}")
        elif f in INSERTS:
            kind = "INSERT"
        elif f in (F.F_DELETE, F.F_DELETE_EMPTY):
            kind = "DELETE"
        else:
            kind = "UPDATE"
        rid = F.rid(rec.body)          # REQ-25 / X-133: CLR target = RID of the undone record
        if rid is not None:
            if clr:
                notes.insert(1, f"target_ref:{rid.hex()}")
            else:
                notes.append(f"row_ref:{rid.hex()}")
        self.out.events.append(RawEvent(lri, tid, kind, rec.offset, rec.length, t.schema, t.name,
                                        before.values if before else None, after.values if after else None,
                                        note=_join(notes)))
        gaps = [n.split(":", 1)[1] for n in (after.notes if after else [])
                if n.startswith(("lob_unavailable:", "value_unavailable:"))]
        if gaps:
            keys = self._keys(t, after.values if after else None)
            self._count(inf, REREAD, R_OUT_OF_ROW)
            if keys is None:
                self._signal(inf, UNSUPPORTED, R_OUT_OF_ROW + " - table has no key", t, columns=gaps)
            else:
                self._signal(inf, REREAD, R_OUT_OF_ROW, t, keys, columns=gaps)
            return
        self._count(inf, DECODED)

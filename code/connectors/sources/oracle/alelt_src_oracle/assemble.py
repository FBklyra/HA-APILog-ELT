"""V$LOGMNR_CONTENTS rows -> whole committed transactions as canonical ChangeEvents.

Design choice (documented in README): the miner reads **uncommitted** redo (no ``COMMITTED_DATA_ONLY``) and this
module assembles transactions itself by XID:

- ``COMMITTED_DATA_ONLY`` makes LogMiner buffer every open transaction inside the database session (PGA) and
  hides the start SCN of open transactions, so a resume position could not be computed safely.
- Reading uncommitted rows gives ``START`` / DML / ``ROLLBACK=1`` undo rows / ``COMMIT`` / ``ROLLBACK`` and lets us
  emit a **commit token** that carries the restart SCN (oldest open transaction), REMOTE.md S3.
- The remote-capture contract (REMOTE.md S2) wants whole transactions, contiguous, in commit order: DML is
  buffered here per XID and emitted at COMMIT; rolled-back transactions are dropped here; savepoint rollbacks
  (``ROLLBACK = 1`` undo rows inside a committed transaction) cancel the newest live change of the same ROW_ID.

Row keys used (upper case, as selected by ``logminer.CONTENTS_SQL``): SCN, XID (hex), OPERATION_CODE, OPERATION,
SEG_OWNER, TABLE_NAME, ROW_ID, CSF, ROLLBACK, RS_ID, SSN, SQL_REDO, SQL_UNDO, TIMESTAMP, INFO.

Completeness (alelt_common/completeness.py, X-132): every row is counted in ``census`` (record ``logminer:<code>``):
- decoded: START / COMMIT / ROLLBACK / INSERT / UPDATE / DELETE / DDL / LOB rows whose result is determined;
  ``TRUNCATE TABLE`` (a DDL row) becomes a TRUNCATE event (XC-33);
- ignored (documented, no row change): INTERNAL, SELECT_FOR_UPDATE, rows of schemas outside the capture, objects no
  longer in the dictionary (dropped: not captured at mining time), undo rows of changes made before the mining start;
- needs_keyed_reread: LOB_WRITE / LOB_TRIM / LOB_ERASE on a LOB whose prior value is not in this transaction (the
  result would be a guess), XMLType (binary/object storage) DOC rows, columns not in the catalog (DDL inside the
  mining window; + needs_catalog_refresh) - the row is re-read by key from the locator / redo WHERE clause;
- unsupported: UNSUPPORTED rows, interrupted CSF continuations, missing dictionary for a captured table
  (``OBJ# n``, + needs_catalog_refresh), a committed transaction whose start was not mined (connection-wide), any
  unknown OPERATION_CODE (also counted as unexplained).
"""
from __future__ import annotations

import itertools

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import Any

from alelt_common import completeness as FC
from alelt_common.canonical_jsonl import Undecodable
from alelt_common.models import ChangeEvent, ChangeKind, ColumnInfo, Position, TableInfo, TableRef

from . import types as T
from .position import Scn
from .redosql import Lit, LobLocator, LobOp, RedoSyntaxError, Stmt, parse_lob_locator, parse_lob_ops, parse_stmt

log = logging.getLogger("alelt_src_oracle")
ENGINE = "oracle"

# V$LOGMNR_CONTENTS.OPERATION_CODE (Oracle Database Reference)
OP_INSERT, OP_DELETE, OP_UPDATE, OP_DDL, OP_START, OP_COMMIT = 1, 2, 3, 5, 6, 7
OP_SEL_LOB_LOCATOR, OP_LOB_WRITE, OP_LOB_TRIM, OP_SELECT_FOR_UPDATE, OP_LOB_ERASE = 9, 10, 11, 25, 28
OP_INTERNAL, OP_MISSING_SCN, OP_ROLLBACK = 0, 34, 36
OP_XML_BEGIN, OP_XML_WRITE, OP_XML_END = 68, 70, 71
OP_UNSUPPORTED = 255
DML_OPS = {OP_INSERT: "INSERT", OP_DELETE: "DELETE", OP_UPDATE: "UPDATE"}
LOB_OPS = {OP_LOB_WRITE, OP_LOB_TRIM, OP_LOB_ERASE}


class LogMinerGap(RuntimeError):
    """MISSING_SCN or a log sequence gap: changes may be lost - stop, never skip (R7)."""


@dataclass
class _Ev:
    ev: ChangeEvent
    rowid: str | None
    table: tuple[str, str] | None
    lob_absent: set[str] = field(default_factory=set)   # INSERT LOB columns not in the redo (SRC-ora-03)


@dataclass
class _Lob:
    loc: LobLocator
    ops: list[LobOp] = field(default_factory=list)
    scn: int = 0


@dataclass
class _Txn:
    xid: str
    start_scn: int
    seen_start: bool
    events: list[_Ev] = field(default_factory=list)
    lob: _Lob | None = None
    undone: int = 0
    nbytes: int = 0
    cold: SpillBuffer | None = None          # HC-08: oldest events of a large transaction (on disk)
    seen_at: float = 0.0
    cold_dml: bool = False


from alelt_common.spill import SpillBuffer, SpillLimitExceeded, mem_budget_bytes  # noqa: E402

# HC-08: an open transaction spills its oldest events to disk (alelt_common.spill) instead of stopping capture;
# only the disk cap (ALELT_CAPTURE_MAX_SPILL_MB) stops it (runner stop + incident).
TransactionTooLarge = SpillLimitExceeded


@dataclass
class AssemblerStats:
    rows: int = 0
    committed: int = 0
    skipped: int = 0
    rolled_back: int = 0
    compensations: int = 0
    unmatched_compensations: int = 0
    quarantined: int = 0
    empty_commits_suppressed: int = 0


def _upper(x: Any) -> str:
    return "" if x is None else str(x)


class Assembler:
    """Feed rows in V$LOGMNR_CONTENTS order (SCN ascending); get committed transactions back.

    ``skip``: commit token already delivered (resume): transactions whose ``(commit_scn, xid) <= skip.key`` are dropped.
    ``db_tz``: time zone of V$LOGMNR_CONTENTS.TIMESTAMP (database server time); commit_ts is converted to UTC.
    ``empty_commit_every_s``: transactions without captured changes are emitted as a bare COMMIT (resume token) at
    most this often, so the durable position keeps moving on an idle captured schema.
    """

    def __init__(self, tables: dict[tuple[str, str], TableInfo], schemas: Iterable[str], skip: Scn | None = None,
                 db_tz: tzinfo = timezone.utc, empty_commit_every_s: float = 5.0,
                 clock: Callable[[], float] = time.monotonic, census: FC.Census | None = None,
                 resolve_obj: Callable[[int], tuple[str, str] | None] | None = None,
                 max_lookback_scns: int = 0) -> None:
        self.census = census if census is not None else FC.Census(ENGINE)
        self.resolve_obj = resolve_obj
        self._cls: tuple[str, str] | None = None
        self.tables = tables
        self.schemas = {s.upper() for s in schemas} | {k[0] for k in tables}
        self.skip = skip
        self.db_tz = db_tz
        self.empty_every = empty_commit_every_s
        self.clock = clock
        self._last_emit = clock()
        self.open: dict[str, _Txn] = {}
        self.max_txn_bytes = mem_budget_bytes()      # in-memory budget per open transaction (then spill)
        self.max_lookback_scns = int(max_lookback_scns or 0)
        self._lookback_capped = False
        self._pending: list[tuple[str, _Txn, datetime | None]] = []
        self._pending_scn: int | None = None
        self._frag: dict[str, Any] | None = None
        self.stats = AssemblerStats()
        self.last_scn: int | None = None

    # ------------------------------------------------------------------ public
    def feed(self, row: dict[str, Any]) -> list[ChangeEvent]:
        self.stats.rows += 1
        scn = int(row["SCN"])
        out: list[ChangeEvent] = []
        if self._pending_scn is not None and scn > self._pending_scn:
            out = self._emit_pending()
        self.last_scn = scn if self.last_scn is None else max(self.last_scn, scn)
        self._cls = None
        rec = f"logminer:{int(row.get('OPERATION_CODE') or 0)}"
        merged = self._continuation(row)
        if merged is not None:
            self._process(merged)
        elif self._cls is None:
            self._cls = (FC.RecordClass.DECODED.value, "")      # CSF fragment: decoded with its continuation
        cls, why = self._cls or (FC.RecordClass.DECODED.value, "")
        if cls == "unexplained":
            self.census.unexplained(rec, why)
        else:
            self.census.count(rec, cls, why)
        return out

    def _mark(self, cls: FC.RecordClass | str, reason: str = "") -> None:
        self._cls = ("unexplained" if cls == "unexplained" else FC.RecordClass(cls).value, reason)

    def _signal(self, t: _Txn, row: dict[str, Any], sig: FC.SourceSignal) -> None:
        sig.record = sig.record or f"logminer:{int(row.get('OPERATION_CODE') or 0)}"
        t.events.append(_Ev(sig.event(self._pos(row, t), t.xid), None, None))
        if self._cls is None or self._cls[0] != FC.RecordClass.UNSUPPORTED.value:
            self._mark(sig.cls, sig.reason.split(":", 1)[0][:100])

    def _key(self, owner: str, name: str, vals: dict[str, Lit]) -> dict[str, Any] | None:
        """Primary-key values from a redo / locator WHERE clause (None when a key column is missing)."""
        info = self.tables.get((owner, name))
        keys = info.key_columns() if info else []
        if not keys or any(k.name not in vals for k in keys):
            return None
        try:
            return {k.name: T.convert_lit(vals[k.name], k.canonical_type.value, k.engine_type, k.scale) for k in keys}
        except T.ConvertError:
            return None

    def _reread_or_hold(self, t: _Txn, row: dict[str, Any], owner: str, name: str, vals: dict[str, Lit] | None,
                        why: str) -> None:
        ref = TableRef(schema=owner, name=name)
        key = self._key(owner, name, vals) if vals is not None else None
        self._signal(t, row, FC.keyed_reread(ref, [key], why) if key is not None
                     else FC.unsupported(ref, why + "; no primary key in the redo to re-read by"))

    def flush(self, force_token: bool = False) -> list[ChangeEvent]:
        """Window end: all rows up to the window's end SCN were fed. Emits commits waiting at the last SCN."""
        return self._emit_pending(force_token)

    def restart_scn(self, upto: int) -> int:
        """SCN where mining must restart to rebuild every transaction still open (or ``upto``)."""
        starts = [t.start_scn for t in self.open.values()] + [t.start_scn for _, t, _ in self._pending]
        return self._capped(min(starts + [upto]), upto)

    def _capped(self, restart: int, at: int) -> int:
        """SRC-ora-06: a long-open transaction would pull every restart (and the logs mined after it) hours back.
        With ``max_lookback_scns`` the restart point never lies more than that many SCNs behind ``at``; a
        transaction that started before is then 'start not mined' after a restart = connection-wide hold + resync
        (bounded source impact, never a silent loss). 0 = unlimited (default)."""
        if self.max_lookback_scns and at - restart > self.max_lookback_scns:
            if not self._lookback_capped:
                log.error("oracle: open transaction older than max_lookback_scns (%s): restart capped at SCN %s",
                          self.max_lookback_scns, at - self.max_lookback_scns, extra={"component": "alelt_src_oracle"})
            self._lookback_capped = True
            return at - self.max_lookback_scns
        return restart

    def oldest_open(self, now: float | None = None) -> tuple[str, float] | None:
        """(xid, age seconds) of the oldest open transaction (alerting, SRC-ora-06)."""
        if not self.open:
            return None
        t = min(self.open.values(), key=lambda x: x.start_scn)
        return t.xid, (now if now is not None else self.clock()) - t.seen_at

    # ------------------------------------------------------------------ internals
    def _continuation(self, row: dict[str, Any]) -> dict[str, Any] | None:
        """CSF = 1: SQL_REDO/SQL_UNDO continue in the next row(s) with the same RS_ID/SSN."""
        csf = int(row.get("CSF") or 0)
        if self._frag is not None:
            same = (_upper(row.get("RS_ID")) == _upper(self._frag.get("RS_ID"))
                    and _upper(row.get("SSN")) == _upper(self._frag.get("SSN")))
            if same:
                self._frag["SQL_REDO"] = (self._frag.get("SQL_REDO") or "") + (row.get("SQL_REDO") or "")
                self._frag["SQL_UNDO"] = (self._frag.get("SQL_UNDO") or "") + (row.get("SQL_UNDO") or "")
                if csf:
                    return None
                merged, self._frag = self._frag, None
                return merged
            broken, self._frag = self._frag, None
            self._quarantine(broken, "CSF continuation interrupted (incomplete SQL_REDO)")
            bo, bn = _upper(broken.get("SEG_OWNER")).upper(), _upper(broken.get("TABLE_NAME"))
            bt = self.open.get(_upper(broken.get("XID")).upper()) or self._txn(broken)
            self._signal(bt, broken, FC.unsupported(TableRef(schema=bo, name=bn) if bo and bn else None,
                                                    "CSF continuation interrupted: SQL_REDO incomplete"))
        if csf:
            self._frag = dict(row)
            return None
        return row

    def _txn(self, row: dict[str, Any], start: bool = False) -> _Txn:
        xid = _upper(row.get("XID")).upper()
        t = self.open.get(xid)
        if t is None:
            t = _Txn(xid, int(row["SCN"]), seen_start=start)
            t.seen_at = self.clock()
            self.open[xid] = t
        return t

    def _process(self, row: dict[str, Any]) -> None:
        op = int(row.get("OPERATION_CODE") or 0)
        if op == OP_START:
            self._txn(row, start=True)
            return
        if op == OP_COMMIT:
            self._commit(row)
            return
        if op == OP_ROLLBACK:
            t = self.open.pop(_upper(row.get("XID")).upper(), None)
            if t is not None:
                self.stats.rolled_back += 1
            return
        xt = self.open.get(_upper(row.get("XID")).upper())
        if xt is not None:
            xt.nbytes += len(row.get("SQL_REDO") or "") + len(row.get("SQL_UNDO") or "") + 64
            if xt.nbytes > self.max_txn_bytes:
                self._spill(xt)
        if op == OP_MISSING_SCN:
            raise LogMinerGap(f"LogMiner reported MISSING_SCN at SCN {row.get('SCN')} - redo not available")
        if op == OP_INTERNAL:
            # INTERNAL rows (seen live inside SEL_LOB_LOCATOR / LOB_WRITE / LOB_TRIM sequences) carry no change and
            # must not close the open LOB sequence
            self._mark(FC.RecordClass.IGNORED, "INTERNAL (no row change)")
            return
        if op == OP_SELECT_FOR_UPDATE:
            self._mark(FC.RecordClass.IGNORED, "SELECT_FOR_UPDATE (lock only)")
            return
        owner = _upper(row.get("SEG_OWNER")).upper()
        if owner not in self.schemas and owner != "UNKNOWN":
            self._mark(FC.RecordClass.IGNORED, "schema not captured")
            return
        t = self._txn(row)
        if op not in LOB_OPS and t.lob is not None:
            self._finish_lob(t)
        if op in DML_OPS:
            self._dml(t, row, op)
        elif op == OP_SEL_LOB_LOCATOR:
            try:
                t.lob = _Lob(parse_lob_locator(row.get("SQL_REDO") or ""), scn=int(row["SCN"]))
            except RedoSyntaxError as e:
                self._quarantine(row, f"SELECT_LOB_LOCATOR not parseable: {e}", t)
        elif op in LOB_OPS:
            if t.lob is None:
                self._quarantine(row, "LOB operation without a preceding SELECT_LOB_LOCATOR in this window", t)
                return
            try:
                t.lob.ops.extend(parse_lob_ops(row.get("SQL_REDO") or ""))
            except RedoSyntaxError as e:
                lob, t.lob = t.lob, None
                self._quarantine(row, f"LOB operation not parseable: {e}", t)
                self._reread_or_hold(t, row, lob.loc.schema.upper(), lob.loc.table, lob.loc.where,
                                     f"LOB operation not parseable: {e}")
        elif op == OP_DDL:
            tab = _upper(row.get("TABLE_NAME"))
            redo = (row.get("SQL_REDO") or "").strip()
            trunc = _truncate_target(redo, owner)
            if trunc is not None:                     # TRUNCATE TABLE is logged as DDL: a row change (XC-33)
                t.events.append(_Ev(ChangeEvent(position=self._pos(row, t), txid=t.xid, kind=ChangeKind.TRUNCATE,
                                                table=TableRef(schema=trunc[0], name=tab or trunc[1]),
                                                note="truncate table"), None, (trunc[0], tab or trunc[1])))
                return
            t.events.append(_Ev(ChangeEvent(
                position=self._pos(row, t), txid=t.xid, kind=ChangeKind.DDL,
                table=TableRef(schema=owner, name=tab) if tab else None,
                note=redo[:4000]), None, None))
        elif op in (OP_XML_BEGIN, OP_XML_WRITE, OP_XML_END):
            tab = _upper(row.get("TABLE_NAME"))
            if op == OP_XML_BEGIN:
                try:
                    loc = parse_lob_locator(row.get("SQL_REDO") or "")
                    where: dict[str, Lit] | None = loc.where
                except RedoSyntaxError:
                    where = None
                self._reread_or_hold(t, row, owner, tab, where, "XMLType (binary/object storage) change: XML DOC "
                                                                "rows are not SQL")
            else:
                self._mark(FC.RecordClass.NEEDS_KEYED_REREAD, "XMLType DOC continuation (row re-read at DOC BEGIN)")
        elif op == OP_UNSUPPORTED:
            self._quarantine(row, f"LogMiner UNSUPPORTED operation ({row.get('INFO') or 'no info'})", t)
            tab = _upper(row.get("TABLE_NAME"))
            self._signal(t, row, FC.unsupported(TableRef(schema=owner, name=tab) if tab else None,
                                                f"LogMiner UNSUPPORTED operation ({row.get('INFO') or 'no info'})"))
        else:
            self._quarantine(row, f"unknown LogMiner OPERATION_CODE {op} ({row.get('OPERATION')})", t)
            tab = _upper(row.get("TABLE_NAME"))
            self._signal(t, row, FC.unsupported(TableRef(schema=owner, name=tab) if tab else None,
                                                f"unknown LogMiner OPERATION_CODE {op}"))
            self._mark("unexplained", f"OPERATION_CODE {op} ({row.get('OPERATION')})")

    def _pos(self, row: dict[str, Any], t: _Txn) -> Position:
        return Position(engine=ENGINE, text=Scn(int(row["SCN"]), t.xid).text)

    # ---- DML
    def _dml(self, t: _Txn, row: dict[str, Any], op: int) -> None:
        owner, name = _upper(row.get("SEG_OWNER")).upper(), _upper(row.get("TABLE_NAME"))
        if owner == "UNKNOWN" or name.startswith("OBJ# "):
            self._missing_dictionary(t, row, owner, name)
            return
        info = self.tables.get((owner, name))
        if info is not None and not info.capture_enabled and info.columns:
            self._mark(FC.RecordClass.IGNORED, "table not captured")    # the connector never delivers its rows
            return
        rowid = row.get("ROW_ID")
        if int(row.get("ROLLBACK") or 0):
            self._compensate(t, (owner, name), rowid, op)
            return
        try:
            redo = parse_stmt(row.get("SQL_REDO") or "")
            undo = None
            if op == OP_UPDATE and row.get("SQL_UNDO"):
                try:
                    undo = parse_stmt(row["SQL_UNDO"])
                except RedoSyntaxError:
                    undo = None          # the redo WHERE clause still carries the old values
            self._uncat: list[str] = []
            self._lob_absent: set[str] = set()
            ev = self._event(t, row, op, owner, name, info, redo, undo)
        except (RedoSyntaxError, T.ConvertError) as e:
            self._quarantine(row, f"{DML_OPS[op]} not decodable: {e}", t)
            return
        t.events.append(_Ev(ev, rowid, (owner, name), set(self._lob_absent)))
        if self._uncat:                # columns newer than the catalog (DDL in the window): never best-effort
            why = f"column(s) {','.join(self._uncat)} not in the catalog (DDL inside the mining window)"
            self._signal(t, row, FC.catalog_refresh(TableRef(schema=owner, name=name), why))
            img = {**redo.where, **redo.values}
            self._reread_or_hold(t, row, owner, name, img, why)

    def _missing_dictionary(self, t: _Txn, row: dict[str, Any], owner: str, name: str) -> None:
        """``OBJ# n`` / owner UNKNOWN: LogMiner has no dictionary entry for the object (dropped, or altered after this
        redo). Values are hex RAW without column names - never decoded."""
        obj = None
        if name.startswith("OBJ# "):
            try:
                obj = int(name[5:].strip())
            except ValueError:
                obj = None
        res = self.resolve_obj(obj) if (obj is not None and self.resolve_obj is not None) else None
        if res is None:
            self._mark(FC.RecordClass.IGNORED, "object not in the dictionary (dropped): not captured at mining time")
            self.stats.skipped += 1
            return
        ref = TableRef(schema=res[0], name=res[1])
        info = self.tables.get(res)
        if res[0] not in self.schemas or (info is not None and not info.capture_enabled):
            self._mark(FC.RecordClass.IGNORED, "object outside the capture (resolved by object id)")
            return
        why = f"no LogMiner dictionary for object {obj} ({res[0]}.{res[1]}): values not decodable"
        self._quarantine(row, why, t)
        self._signal(t, row, FC.catalog_refresh(ref, why))
        self._signal(t, row, FC.unsupported(ref, why))

    def _cols(self, info: TableInfo | None) -> dict[str, ColumnInfo]:
        return {c.name: c for c in info.columns} if info else {}

    def _conv(self, cols: dict[str, ColumnInfo], vals: dict[str, Lit], notes: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, lit in vals.items():
            c = cols.get(k)
            if c is None and not cols:        # table not in the catalog at all: the pipeline decides (not captured)
                out[k] = _best_effort(lit)
                if "uncataloged" not in notes:
                    notes.append("uncataloged")
                continue
            if c is None:                     # column newer than the catalog: never a best-effort value
                if k not in getattr(self, "_uncat", []):
                    getattr(self, "_uncat", []).append(k)
                continue
            out[k] = T.convert_lit(lit, c.canonical_type.value, c.engine_type, c.scale)
            if lit.lost:
                notes.append(f"precision_lost:{k}")
        return out

    def _event(self, t: _Txn, row: dict[str, Any], op: int, owner: str, name: str, info: TableInfo | None,
               redo: Stmt, undo: Stmt | None) -> ChangeEvent:
        cols = self._cols(info)
        notes: list[str] = []
        before: dict[str, Any] | None = None
        after: dict[str, Any] | None = None
        if op == OP_INSERT:
            if redo.op != "INSERT":
                raise RedoSyntaxError(f"INSERT row carries a {redo.op} statement")
            after = self._conv(cols, redo.values, notes)
            self._lob_absent = {c for c, ci in cols.items() if c not in after
                                and T.base_type(ci.engine_type) in T.LOB_TYPES}
            for c in cols:
                after.setdefault(c, None)
        elif op == OP_DELETE:
            if redo.op != "DELETE":
                raise RedoSyntaxError(f"DELETE row carries a {redo.op} statement")
            before = self._conv(cols, redo.where, notes)
        else:
            if redo.op != "UPDATE":
                raise RedoSyntaxError(f"UPDATE row carries a {redo.op} statement")
            before = self._conv(cols, redo.where, notes)
            if undo is not None and undo.op == "UPDATE":
                before.update(self._conv(cols, undo.values, notes))
            after = dict(before)
            after.update(self._conv(cols, redo.values, notes))
            missing = [c for c in cols if c not in after]
            if missing:
                notes.append("unchanged:" + ",".join(missing))
        if info is not None and op != OP_INSERT:
            keys = [c.name for c in info.key_columns()]
            if keys and before is not None and any(k not in before for k in keys):
                raise RedoSyntaxError("old image lacks key column(s) - supplemental logging missing?")
        return ChangeEvent(position=self._pos(row, t), txid=t.xid, kind=ChangeKind(DML_OPS[op]),
                           table=TableRef(schema=owner, name=name), before=before, after=after,
                           note=";".join(notes) or None)

    def _spill(self, t: _Txn) -> None:
        """Move the oldest half of the in-memory events of `t` to its spill file (bounded memory, HC-08). Later
        searches (savepoint undo, LOB pieces) see only the in-memory tail; a miss there with spilled events is a
        hold / keyed re-read, never a guess."""
        n = max(1, len(t.events) // 2)
        if t.cold is None:
            t.cold = SpillBuffer(label=f"oracle:{t.xid}")
        moved = t.events[:n]
        del t.events[:n]
        for e in moved:
            if e.lob_absent and e.ev.after is not None and e.table is not None:
                # a LOB write can no longer reach this row image: apply the unwritten-LOB rule now (re-read by key);
                # a later write becomes its own UPDATE row
                cols = sorted(e.lob_absent)
                for c in cols:
                    e.ev.after.pop(c, None)
                e.ev.note = ";".join(x for x in (e.ev.note, "unchanged:" + ",".join(cols)) if x)
                t.cold.write_cold(e.ev)
                t.cold_dml = True
                owner, name = e.table
                info = self.tables.get((owner, name))
                keys = [c.name for c in info.key_columns()] if info else []
                key = {k: e.ev.after[k] for k in keys if k in e.ev.after} if keys else None
                why = f"INSERT LOB column(s) {','.join(cols)} not in the redo (large transaction spilled)"
                ref = TableRef(schema=owner, name=name)
                sig = (FC.keyed_reread(ref, [key], why, "logminer:lob", columns=cols)
                       if key and len(key) == len(keys) else FC.unsupported(ref, why + "; no primary key",
                                                                            "logminer:lob"))
                t.cold.write_cold(sig.event(e.ev.position, t.xid))
                continue
            if e.ev.kind in (ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE):
                t.cold_dml = True
            t.cold.write_cold(e.ev)
        t.nbytes //= 2

    def _compensate(self, t: _Txn, table: tuple[str, str], rowid: Any, op: int | None = None) -> None:
        idx = -1
        for i in range(len(t.events) - 1, -1, -1):
            e = t.events[i]
            if e.table == table and (rowid is None or e.rowid == rowid) and e.ev.kind in (
                    ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE):
                idx = i
                break
        if idx < 0 and t.cold is not None and t.cold.spilled:
            # the undone change is in the spilled part of a large transaction: hold + resync the table
            self.stats.unmatched_compensations += 1
            sig = FC.unsupported(TableRef(schema=table[0], name=table[1]),
                                 "savepoint undo of a change in the spilled part of a large transaction: hold + "
                                 "resync", "logminer:undo")
            t.events.append(_Ev(sig.event(Position(engine=ENGINE, text=Scn(int(self.last_scn or 0), t.xid).text),
                                          t.xid), None, None))
            self._mark(sig.cls, "undo of a spilled change")
            return
        if idx < 0:
            self.stats.unmatched_compensations += 1   # undo of a change made before the mining start
            self._mark(FC.RecordClass.IGNORED, "undo of a change made before the mining start")
            return
        e = t.events.pop(idx)
        self.stats.compensations += 1
        if op != OP_DELETE or e.ev.kind is ChangeKind.INSERT:
            return
        # A DELETE compensation undoes an INSERT. On tables with LOB columns LogMiner writes the INSERT with a zero
        # ROW_ID followed by an UPDATE (LOB columns) carrying the real ROW_ID, and only ONE compensation (seen live
        # on 23ai): drop the row's whole history in this transaction back to (and including) its INSERT.
        key = self._row_key(table, e.ev)
        # SRC-ora-09: only with the LOB pattern itself (an INSERT of this row with a zero ROW_ID earlier in the
        # transaction); a version that writes one compensation per change never reaches this branch for an older
        # UPDATE, so a legitimate earlier UPDATE is never removed by the heuristic
        if key is None or not any(x.table == table and x.ev.kind is ChangeKind.INSERT and _no_rowid(x.rowid)
                                  and self._row_key(table, x.ev) == key for x in t.events[:idx]):
            return
        for i in range(idx - 1, -1, -1):
            x = t.events[i]
            if x.table != table or x.ev.kind not in (ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE):
                continue
            if x.rowid == rowid or (_no_rowid(x.rowid) and key is not None and self._row_key(table, x.ev) == key):
                kind = x.ev.kind
                del t.events[i]
                if kind is ChangeKind.INSERT:
                    break

    def _row_key(self, table: tuple[str, str], ev: ChangeEvent) -> tuple[Any, ...] | None:
        info = self.tables.get(table)
        keys = [c.name for c in info.key_columns()] if info else []
        img = ev.after or ev.before or {}
        if not keys or any(k not in img for k in keys):
            return None
        return tuple(img[k] for k in keys)

    # ---- LOBs
    def _finish_lob(self, t: _Txn) -> None:
        lob, t.lob = t.lob, None
        if lob is None or not lob.ops:
            return
        owner, name, col = lob.loc.schema.upper(), lob.loc.table, lob.loc.column
        info = self.tables.get((owner, name))
        binary = lob.loc.is_binary
        target: _Ev | None = None
        for e in reversed(t.events):
            if e.table == (owner, name) and lob.loc.rowid and e.rowid == lob.loc.rowid and e.ev.after is not None:
                target = e
                break
        base: Any = None
        known = target is not None and col in (target.ev.after or {})
        if known:
            base = target.ev.after[col]                      # type: ignore[index]
        # without the prior value the result is used only when it does not depend on it (e.g. write + trim, or trim to
        # 0 first): applied to two different prior values, the operations must give the same result
        determined = known or _independent_of_base(lob.ops, binary)
        if not determined:
            ref = TableRef(schema=owner, name=name)
            key = self._key(owner, name, lob.loc.where)
            why = (f"LOB {'/'.join(sorted({o.kind for o in lob.ops}))} on {col} whose prior value is not in this "
                   "transaction")
            sig = FC.keyed_reread(ref, [key], why, "logminer:lob") if key is not None else FC.unsupported(
                ref, why + "; no primary key in the locator", "logminer:lob")
            t.events.append(_Ev(sig.event(Position(engine=ENGINE, text=Scn(lob.scn, t.xid).text), t.xid),
                                None, None))
            self.census.count("logminer:lob", sig.cls, "LOB change on an unknown prior value")
            if target is not None and target.ev.after is not None:
                target.ev.after.pop(col, None)
            return
        assumed = base is None
        v: Any = (b"" if binary else "") if base is None else base
        for o in lob.ops:
            v = _apply_lob(v, o, binary)
        if target is not None:
            target.lob_absent.discard(col)
            target.ev.after[col] = v                         # type: ignore[index]
            if target.ev.note and f"unchanged:" in target.ev.note:
                target.ev.note = _drop_unchanged(target.ev.note, col)
            if assumed:
                target.ev.note = ";".join(x for x in (target.ev.note, f"lob_from_writes:{col}") if x)
            return
        cols = self._cols(info)
        notes: list[str] = []
        try:
            key = self._conv(cols, lob.loc.where, notes)
        except T.ConvertError as e:
            self.stats.quarantined += 1
            t.events.append(_Ev(ChangeEvent(position=Position(engine=ENGINE, text=Scn(lob.scn, t.xid).text),
                                            txid=t.xid, kind=ChangeKind.OTHER, table=TableRef(schema=owner, name=name),
                                            after={"__alelt_undecodable": Undecodable(
                                                f"LOB row key not decodable: {e} | {lob.loc}"[:4000])},
                                            note=f"quarantine:LOB row key not decodable: {e}"), None, None))
            return
        after = dict(key)
        after[col] = v
        missing = [c for c in cols if c not in after]
        notes += ["unchanged:" + ",".join(missing)] if missing else []
        notes.append(f"lob_from_writes:{col}")
        t.events.append(_Ev(ChangeEvent(position=Position(engine=ENGINE, text=Scn(lob.scn, t.xid).text),
                                        txid=t.xid, kind=ChangeKind.UPDATE, table=TableRef(schema=owner, name=name),
                                        before=key, after=after, note=";".join(notes)), lob.loc.rowid, (owner, name)))

    def _unwritten_lobs(self, t: _Txn) -> None:
        """SRC-ora-03: an INSERT whose redo lacks a LOB column and no LOB write filled it before COMMIT (NOLOGGING LOB
        segment, direct-path DBMS_LOB): the value is unknown - never NULL. Omit it and re-read the row by key."""
        for e in list(t.events):
            if not e.lob_absent or e.ev.after is None or e.table is None:
                continue
            cols = sorted(e.lob_absent)
            for c in cols:
                e.ev.after.pop(c, None)
            e.ev.note = ";".join(x for x in (e.ev.note, "unchanged:" + ",".join(cols)) if x)
            owner, name = e.table
            ref = TableRef(schema=owner, name=name)
            info = self.tables.get((owner, name))
            keys = [c.name for c in info.key_columns()] if info else []
            key = {k: e.ev.after[k] for k in keys if k in e.ev.after} if keys else None
            why = f"INSERT without the value of LOB column(s) {','.join(cols)} and no LOB write in the transaction"
            sig = (FC.keyed_reread(ref, [key], why, "logminer:lob", columns=cols)
                   if key and len(key) == len(keys) else FC.unsupported(ref, why + "; no primary key", "logminer:lob"))
            t.events.insert(t.events.index(e) + 1, _Ev(sig.event(e.ev.position, t.xid), None, None))
            self.census.count("logminer:lob", sig.cls, "INSERT LOB value not in the redo")
            e.lob_absent = set()

    # ---- commit / emit
    def _commit(self, row: dict[str, Any]) -> None:
        xid = _upper(row.get("XID")).upper()
        t = self.open.pop(xid, None)
        if t is None:
            t = _Txn(xid, int(row["SCN"]), seen_start=False)
        if t.lob is not None:
            self._finish_lob(t)
        self._unwritten_lobs(t)
        ts = row.get("TIMESTAMP") or row.get("COMMIT_TIMESTAMP")
        commit_ts = None
        if isinstance(ts, datetime):
            commit_ts = (ts.replace(tzinfo=self.db_tz) if ts.tzinfo is None else ts).astimezone(timezone.utc)
        scn = int(row["SCN"])
        if self.skip is not None and (scn, xid) <= self.skip.key:
            self.stats.skipped += 1
            return
        self._pending_scn = scn
        self._pending.append((xid, t, commit_ts))

    def _emit_pending(self, force_token: bool = False) -> list[ChangeEvent]:
        if not self._pending:
            return []
        scn = self._pending_scn or 0
        pend = sorted(self._pending, key=lambda p: p[0])
        out: list[ChangeEvent] = []
        parts: list[Any] = []
        now = self.clock()
        for i, (xid, t, ts) in enumerate(pend):
            rest = pend[i + 1:]
            restart = self._capped(min([x.start_scn for x in self.open.values()] + [x[1].start_scn for x in rest]
                                       + [scn]), scn)
            if not t.events and not t.cold:
                last = i == len(pend) - 1
                if not (force_token and last) and now - self._last_emit < self.empty_every:
                    self.stats.empty_commits_suppressed += 1
                    continue
            note = None if t.seen_start else "partial_txn:start_not_mined"
            if t.cold is not None and t.cold.spilled:          # HC-08: large transaction, delivered lazily
                parts.append(out)
                parts.append(t.cold.drain())
                out = []
            for e in t.events:
                out.append(e.ev)
            if not t.seen_start and (t.cold_dml or any(e.ev.kind in (ChangeKind.INSERT, ChangeKind.UPDATE,
                                                                     ChangeKind.DELETE) for e in t.events)):
                out.append(FC.unsupported(None, f"transaction {xid} committed but its start was not mined: changes "
                                                "made before the mining start are unknown", "logminer:7")
                           .event(Position(engine=ENGINE, text=Scn(scn, xid).text), xid))
                self.census.count("logminer:7", FC.RecordClass.UNSUPPORTED, "transaction start not mined")
            out.append(ChangeEvent(position=Position(engine=ENGINE, text=Scn(scn, xid, restart).text),
                                   txid=xid, kind=ChangeKind.COMMIT, commit_ts=ts,
                                   note=note if (t.events or t.cold) else None))
            self.stats.committed += 1
            self._last_emit = now
        self._pending = []
        self._pending_scn = None
        if parts:
            parts.append(out)
            return itertools.chain.from_iterable(parts)  # type: ignore[return-value]
        return out

    def _quarantine(self, row: dict[str, Any], reason: str, t: _Txn | None = None) -> None:
        self.stats.quarantined += 1
        if self._cls is None:
            self._mark(FC.RecordClass.UNSUPPORTED, "quarantined: " + reason.split(":", 1)[0][:80])
        log.warning("oracle row quarantined: %s (scn=%s xid=%s)", reason, row.get("SCN"), row.get("XID"),
                    extra={"component": "alelt_src_oracle"})
        if t is None:
            xid = _upper(row.get("XID")).upper()
            t = self.open.get(xid) or self._txn(row)
        owner, name = _upper(row.get("SEG_OWNER")).upper(), _upper(row.get("TABLE_NAME"))
        sql = (row.get("SQL_REDO") or "")[:2000]
        t.events.append(_Ev(ChangeEvent(
            position=self._pos(row, t), txid=t.xid, kind=ChangeKind.OTHER,
            table=TableRef(schema=owner, name=name) if owner and name else None,
            after={"__alelt_undecodable": Undecodable(f"{reason} | {sql}"[:4000])},   # -> quarantined at decode (R7)
            note=f"quarantine:{reason} | rs_id={_upper(row.get('RS_ID')).strip()} ssn={row.get('SSN')} | {sql}"),
            None, None))


def _independent_of_base(ops: list[LobOp], binary: bool) -> bool:
    if not ops:
        return False
    span = max((o.offset or 1) + (o.length or 0) for o in ops) + 64
    a: Any = b"" if binary else ""
    b: Any = (b"\x01" if binary else "\x01") * span
    for o in ops:
        a, b = _apply_lob(a, o, binary), _apply_lob(b, o, binary)
    return bool(a == b)


def _truncate_target(redo: str, owner: str) -> tuple[str, str] | None:
    """``truncate table "S"."T"`` / ``TRUNCATE TABLE t ...`` -> (schema, table); None for any other DDL."""
    import re

    m = re.match(r'^\s*truncate\s+table\s+(?:"([^"]+)"|(\w+))(?:\s*\.\s*(?:"([^"]+)"|(\w+)))?', redo, re.I)
    if not m:
        return None
    a, b = m.group(1) or (m.group(2) or "").upper(), m.group(3) or (m.group(4) or "").upper()
    return (a, b) if b else (owner, a)


def _drop_unchanged(note: str, col: str) -> str:
    parts = []
    for p in note.split(";"):
        if p.startswith("unchanged:"):
            cols = [c for c in p[len("unchanged:"):].split(",") if c and c != col]
            if cols:
                parts.append("unchanged:" + ",".join(cols))
        elif p:
            parts.append(p)
    return ";".join(parts)


def _no_rowid(rowid: Any) -> bool:
    """LogMiner prints an all-'A' (zero) ROW_ID when the row address is not known yet (e.g. INSERT into a LOB table)."""
    return not rowid or set(str(rowid)) <= {"A"}


def _apply_lob(v: Any, o: LobOp, binary: bool) -> Any:
    pad = b"\x00" if binary else " "
    if o.kind == "write":
        data = o.data if o.data is not None else (b"" if binary else "")
        if binary and isinstance(data, str):
            data = bytes.fromhex(data)
        if not binary and isinstance(data, bytes):
            data = data.decode("utf-8", "replace")
        data = data[:o.length] if o.length else data
        start = max(o.offset - 1, 0)
        if len(v) < start:
            v = v + pad * (start - len(v))
        return v[:start] + data + v[start + len(data):]
    if o.kind == "trim":
        return v[:o.length]
    if o.kind == "erase":
        start = max(o.offset - 1, 0)
        end = min(start + o.length, len(v))
        return v[:start] + pad * (end - start) + v[end:]
    return v


def _best_effort(lit: Lit) -> Any:
    if lit.kind in ("null",):
        return None
    if lit.kind == "num":
        from decimal import Decimal
        return Decimal(str(lit.value))
    if lit.kind == "tstz":
        return lit.value.astimezone(timezone.utc)
    if lit.kind == "empty_lob":
        return ""
    return lit.value

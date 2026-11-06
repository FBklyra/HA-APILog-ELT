"""CDC change-function rows -> whole transactions as canonical ChangeEvents.

Rows come from ``cdc.fn_cdc_get_all_changes_<capture_instance>(from_lsn, to_lsn, N'all update old')`` of every
capture instance for the same LSN window. CDC only contains committed changes and ``__$start_lsn`` is the commit
LSN of the transaction, so a transaction = all rows (of all instances) with the same ``__$start_lsn``; inside it
``__$seqval`` (then ``__$operation``) gives the change order.

``__$operation``: 1 delete, 2 insert, 3 update (before image), 4 update (after image). 3+4 of one row change share
``__$seqval`` and are merged into one UPDATE (full before + after image). Large-object columns (``(n)varchar(max)``,
``varbinary(max)``, ``text``, ``ntext``, ``image``, ``xml``) whose bit is not set in ``__$update_mask`` are reported
as ``unchanged:<col>`` (CDC does not carry their value in that case, documented). Columns of the table that the
capture instance does not capture, and computed columns (CDC records them as NULL - verified on SQL Server / Azure SQL
Edge), are omitted from the images; an INSERT or UPDATE of such a table is ALSO re-read by key
(needs_keyed_reread): their values are never guessed and never NULL by assumption (X-132).

Two capture instances of one table (schema change): the older is read up to the newer one's ``start_lsn``
(``CaptureInstance.valid_to``), the newer from there; rows of the older instance at or after that LSN are counted as
ignored (superseded, the same change is in the newer instance) - XC-32. ``pre_signals`` (e.g. a capture instance that
starts after the resume position) are emitted inside the first transaction of the window.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import Any

from alelt_common import completeness as FC
from alelt_common.canonical_jsonl import Undecodable
from alelt_common.models import ChangeEvent, ChangeKind, ColumnInfo, Position, TableInfo, TableRef

from . import types as T
from .position import MAX, Lsn, lsn_text

ENGINE = "sqlserver"
OPS = {1: "DELETE", 2: "INSERT", 3: "UPDATE_OLD", 4: "UPDATE_NEW"}


@dataclass
class CaptureInstance:
    name: str
    table: TableInfo
    captured: dict[str, int] = field(default_factory=dict)     # column -> column_ordinal in the instance (1-based)
    lob_cols: set[str] = field(default_factory=set)
    start_lsn: bytes | None = None
    create_date: datetime | None = None
    computed: set[str] = field(default_factory=set)            # computed columns (CDC captures them as NULL)
    valid_to: bytes | None = None                               # newer instance of the same table starts here


@dataclass
class TxnMeta:
    tran_id: bytes | None = None
    begin: datetime | None = None
    end: datetime | None = None


def bit_set(mask: bytes | None, ordinal: int) -> bool:
    """sys.fn_cdc_is_bit_set(ordinal, mask): ordinal 1 = lowest bit of the LAST byte."""
    if not mask or ordinal < 1:
        return False
    idx = len(mask) - 1 - (ordinal - 1) // 8
    return idx >= 0 and bool(mask[idx] >> ((ordinal - 1) % 8) & 1)


class DecodeError(ValueError):
    pass


def _image(row: dict[str, Any], ci: CaptureInstance, cols: dict[str, ColumnInfo]) -> dict[str, Any]:
    """Captured columns -> canonical values. Unconvertible value -> Undecodable (the line is quarantined at decode
    with its raw bytes, R7). UNKNOWN-typed columns pass through as text/bytes (the column is quarantined by type)."""
    out: dict[str, Any] = {}
    for name in ci.captured:
        c = cols.get(name)
        if c is None or name in ci.computed or c.engine_type.lower() in T.TEXT_FORM_TYPES:
            continue
        v = row.get(name)
        if c.canonical_type.value == "UNKNOWN":
            out[name] = None if v is None else bytes(v) if isinstance(v, (bytes, bytearray)) else str(v)
            continue
        try:
            out[name] = T.convert(v, c.canonical_type.value, c.scale)
        except (T.ConvertError, ValueError, TypeError) as e:
            out[name] = Undecodable(f"{name}:{c.engine_type}:{e}:{repr(v)[:200]}")
    return out


def _undecodable(pos: Position, tref: TableRef | None, reason: str, row: dict[str, Any]) -> ChangeEvent:
    """A whole change row ALELT cannot interpret: carried as an Undecodable value so decode_batch quarantines
    the line (raw bytes + position + reason, R7). Never emitted as a silently ignored OTHER event."""
    raw = {k: (bytes(v).hex() if isinstance(v, (bytes, bytearray)) else str(v)) for k, v in row.items()}
    return ChangeEvent(position=pos, txid="", kind=ChangeKind.OTHER, table=tref,
                       after={"__alelt_undecodable": Undecodable(f"{reason}: {raw!r}"[:4000])},
                       note=f"quarantine:{reason}")


def assemble(rows: Iterable[dict[str, Any]], instances: dict[str, CaptureInstance], meta: dict[bytes, TxnMeta],
             server_tz: tzinfo = timezone.utc, census: FC.Census | None = None,
             pre_signals: list[FC.SourceSignal] | None = None) -> list[ChangeEvent]:
    """Rows of one complete LSN window -> [DML..., COMMIT] per transaction, in commit (start_lsn) order."""
    cen = census if census is not None else FC.Census(ENGINE)
    pending_sigs = list(pre_signals or [])
    def key(r: dict[str, Any]) -> tuple[Any, ...]:
        return (bytes(r["__$start_lsn"]), bytes(r["__$seqval"]), r.get("__$command_id") or 0,
                r["ci"], 0 if r["__$operation"] in (1, 3) else 1)

    srt = sorted(rows, key=key)
    out: list[ChangeEvent] = []
    cur_lsn: bytes | None = None
    pend_old: dict[tuple[bytes, bytes, str], list[dict[str, Any]]] = {}

    def commit(lsn: bytes) -> None:
        m = meta.get(lsn) or TxnMeta()
        ts = m.end
        if ts is not None and ts.tzinfo is None:
            ts = ts.replace(tzinfo=server_tz)
        txid = lsn_text(m.tran_id) if m.tran_id and any(m.tran_id) else lsn_text(lsn)
        for e in out_txn:
            e.txid = txid
        out.extend(out_txn)
        out.append(ChangeEvent(position=Position(engine=ENGINE, text=Lsn(lsn, MAX).text), txid=txid,
                               kind=ChangeKind.COMMIT, commit_ts=ts.astimezone(timezone.utc) if ts else None))

    out_txn: list[ChangeEvent] = []
    for r in srt:
        lsn = bytes(r["__$start_lsn"])
        if cur_lsn is not None and lsn != cur_lsn:
            commit(cur_lsn)
            out_txn = []
        cur_lsn = lsn
        ci = instances.get(r["ci"])
        op = int(r["__$operation"])
        seq = bytes(r["__$seqval"])
        pos = Position(engine=ENGINE, text=Lsn(lsn, seq).text)
        rec = f"cdc:{op}"
        if pending_sigs:
            out_txn.extend(s.event(pos, "") for s in pending_sigs)
            pending_sigs = []
        if ci is None:
            out_txn.append(_undecodable(pos, None, f"unknown capture instance {r['ci']}", r))
            cen.count(rec, FC.RecordClass.UNSUPPORTED, "unknown capture instance")
            continue
        if ci.valid_to is not None and lsn >= ci.valid_to:
            cen.count(rec, FC.RecordClass.IGNORED, "superseded capture instance (change read from the newer one)")
            continue
        tref = ci.table.table
        cols = {c.name: c for c in ci.table.columns}
        # not captured / computed (CDC writes NULL) / CLR + sql_variant (CDC row values are binary serializations,
        # the documented form is their text: SRC-ms-05) -> omitted and re-read by key, never guessed
        uncaptured = [c for c in cols if c not in ci.captured or c in ci.computed
                      or cols[c].engine_type.lower() in T.TEXT_FORM_TYPES]
        try:
            if op == 3:
                pend_old.setdefault((lsn, seq, ci.name), []).append(r)
                cen.count(rec, FC.RecordClass.DECODED)
                continue
            if op == 1:
                ev = ChangeEvent(position=pos, txid="", kind=ChangeKind.DELETE, table=tref,
                                 before=_image(r, ci, cols),
                                 note=("unchanged:" + ",".join(uncaptured)) if uncaptured else None)
            elif op == 2:
                ev = ChangeEvent(position=pos, txid="", kind=ChangeKind.INSERT, table=tref,
                                 after=_image(r, ci, cols),
                                 note=("unchanged:" + ",".join(uncaptured)) if uncaptured else None)
            elif op == 4:
                olds = pend_old.get((lsn, seq, ci.name)) or []
                old = olds.pop(0) if olds else None
                after = _image(r, ci, cols)
                before = _image(old, ci, cols) if old is not None else None
                mask = r.get("__$update_mask")
                unchanged = list(uncaptured)
                for lc in sorted(ci.lob_cols):
                    if lc in ci.captured and not bit_set(mask, ci.captured[lc]):
                        after.pop(lc, None)
                        if before is not None:
                            before.pop(lc, None)
                        unchanged.append(lc)
                notes = []
                if unchanged:
                    notes.append("unchanged:" + ",".join(unchanged))
                if old is None:
                    notes.append("no_before_image")
                ev = ChangeEvent(position=pos, txid="", kind=ChangeKind.UPDATE, table=tref, before=before,
                                 after=after, note=";".join(notes) or None)
            else:
                ev = _undecodable(pos, tref, f"unknown __$operation {op}", r)
        except DecodeError as e:
            ev = _undecodable(pos, tref, f"{OPS.get(op, op)} not decodable: {e}", r)
        out_txn.append(ev)
        if ev.kind is ChangeKind.OTHER:
            cen.count(rec, FC.RecordClass.UNSUPPORTED, "undecodable change row (quarantined)")
        elif ev.kind in (ChangeKind.INSERT, ChangeKind.UPDATE) and uncaptured:
            keys = [c.name for c in ci.table.key_columns()]
            img = ev.after or {}
            why = (f"{ev.kind.value}: column(s) {','.join(uncaptured)} not captured by {ci.name} "
                   "(captured column list / computed)")
            if keys and all(k in img and not isinstance(img[k], Undecodable) for k in keys):
                sig = FC.keyed_reread(tref, [{k: img[k] for k in keys}], why, rec, columns=uncaptured)
            else:
                sig = FC.unsupported(tref, why + "; no key to re-read", rec)
            out_txn.append(sig.event(pos, ""))
            cen.count(rec, sig.cls, "columns not captured")
        else:
            cen.count(rec, FC.RecordClass.DECODED)
    if cur_lsn is not None:
        commit(cur_lsn)
    leftovers = [k for k, v in pend_old.items() if v]
    if leftovers:
        # an update before-image without after-image cannot happen in a consistent window: never drop silently
        raise DecodeError(f"{len(leftovers)} update before-image(s) without after-image, e.g. {leftovers[0]}")
    return out

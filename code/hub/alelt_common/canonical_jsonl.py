"""Canonical JSON Lines batch body for remote-captured sources (connectors/sources/_spi/REMOTE.md §4).

One ChangeEvent per line: {"pos","txid","kind","schema","table","before","after","commit_ts","note"}.
Values: JSON null/bool/int/str as is; everything else as a one-key tagged object:
  {"$dec": "12.30"} Decimal · {"$f": "1.5"|"NaN"|"Infinity"|"-Infinity"} float · {"$bin": base64} bytes ·
  {"$date": iso} date · {"$time": iso} time · {"$ldt": iso} naive datetime · {"$ts": iso} tz-aware datetime (UTC).
  {"$undecodable": text} = connector could not convert the value -> the line is quarantined at decode (R7).
Round trip is exact for every canonical value type.
"""
from __future__ import annotations

import base64
import json
import math
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from .models import ChangeEvent, ChangeKind, Position, QuarantineRecord, RawRef, TableRef
from .spi import DecodeResult


class Undecodable(str):
    """A source value the connector could not convert (text kept). Encoded as {"$undecodable": text}: decode_batch
    quarantines the whole line with its raw bytes (R7) instead of guessing a value."""


def encode_value(v: Any) -> Any:
    if isinstance(v, Undecodable):
        return {"$undecodable": str(v)}
    if v is None or isinstance(v, (bool, str)):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if math.isfinite(v):
            return {"$f": repr(v)}
        return {"$f": "NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")}
    if isinstance(v, Decimal):
        return {"$dec": str(v)}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$bin": base64.b64encode(bytes(v)).decode("ascii")}
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return {"$ldt": v.isoformat()}
        return {"$ts": v.astimezone(timezone.utc).isoformat()}
    if isinstance(v, date):
        return {"$date": v.isoformat()}
    if isinstance(v, time):
        return {"$time": v.isoformat()}
    if isinstance(v, uuid.UUID):
        return str(v)
    raise TypeError(f"not a canonical value: {type(v).__name__}")


_DEC = {
    "$dec": Decimal,
    "$f": float,
    "$bin": lambda s: base64.b64decode(s, validate=True),
    "$date": date.fromisoformat,
    "$time": time.fromisoformat,
    "$ldt": datetime.fromisoformat,
    "$ts": datetime.fromisoformat,
}


def decode_value(v: Any) -> Any:
    if isinstance(v, dict):
        if len(v) != 1:
            raise ValueError(f"bad tagged value {v!r}")
        (k, s), = v.items()
        fn = _DEC.get(k)
        if fn is None or not isinstance(s, str):
            raise ValueError(f"unknown value tag {k!r}")
        out = fn(s)
        if k == "$ts" and out.tzinfo is None:
            raise ValueError("$ts without offset")
        return out
    if isinstance(v, float):                      # producers always tag floats; tolerate plain JSON numbers
        return v
    if isinstance(v, list):
        raise ValueError("arrays must be encoded as JSON text")
    return v


def _row(r: dict[str, Any] | None, enc: bool) -> dict[str, Any] | None:
    if r is None:
        return None
    f = encode_value if enc else decode_value
    return {k: f(v) for k, v in r.items()}


def encode_event(ev: ChangeEvent) -> bytes:
    d = {"pos": ev.position.text, "txid": ev.txid, "kind": ev.kind.value,
         "schema": ev.table.schema_ if ev.table else None, "table": ev.table.name if ev.table else None,
         "before": _row(ev.before, True), "after": _row(ev.after, True),
         "commit_ts": encode_value(ev.commit_ts) if ev.commit_ts else None, "note": ev.note}
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"


def encode_events(events: list[ChangeEvent]) -> bytes:
    return b"".join(encode_event(e) for e in events)


def decode_batch(raw: bytes, engine: str, batch_id: Any = None) -> DecodeResult:
    """Parse a canonical JSONL body. Never raises: bad lines are quarantined with their raw bytes (R7)."""
    events: list[ChangeEvent] = []
    quarantined: list[QuarantineRecord] = []
    bid: uuid.UUID | None
    try:
        bid = batch_id if isinstance(batch_id, uuid.UUID) else uuid.UUID(str(batch_id)) if batch_id else None
    except ValueError:
        bid = None
    off = 0
    for line in raw.splitlines(keepends=True):
        ref = RawRef(batch_id=bid, offset=off, length=len(line)) if bid else None
        off += len(line)
        if not line.strip():
            continue
        d: dict[str, Any] = {}
        try:
            d = json.loads(line)
            ts = decode_value(d.get("commit_ts")) if d.get("commit_ts") else None
            events.append(ChangeEvent(
                position=Position(engine=engine, text=d["pos"]), txid=str(d["txid"]), kind=ChangeKind(d["kind"]),
                table=TableRef(schema=d["schema"], name=d["table"]) if d.get("table") else None,
                before=_row(d.get("before"), False), after=_row(d.get("after"), False),
                commit_ts=ts, raw_ref=ref, note=d.get("note")))
        except Exception as e:  # noqa: BLE001 - quarantine, never drop (R7)
            tbl = None
            if isinstance(d, dict) and d.get("schema") and d.get("table"):
                tbl = TableRef(schema=str(d["schema"]), name=str(d["table"]))
            pos = Position(engine=engine, text=str(d["pos"])) if isinstance(d, dict) and d.get("pos") else None
            quarantined.append(QuarantineRecord(position=pos, table=tbl, reason=f"undecodable canonical line: {e}"[:500],
                                                raw_ref=ref, raw_hex=line[:256].hex()))
    return DecodeResult(events=events, quarantined=quarantined)


# ------------------------------------------------------------------ snapshot rows (same value forms as the CDC path)
_TEXT_TYPES = ("JSON", "XML")


def json_text(v: Any) -> Any:
    """Canonical JSON value = JSON text (REMOTE.md S7). dict / list / numbers from a connector or a plain JSON chunk
    become compact JSON text; text stays as is; None stays None."""
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", "replace")
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def canonical_snapshot_row(row: dict[str, Any], types: dict[str, Any], decode_tags: bool = True,
                           strict: bool = False) -> dict[str, Any]:
    """One snapshot row in the CDC value forms. `types`: column -> CanonicalType (or its value string).

    - JSON / XML columns: dict / list / scalars -> JSON text (the CDC path always carries text), never tag-decoded.
    - other columns: a one-key tagged object ({"$dec": ...}, {"$ts": ...}) is decoded to its typed value when
      `decode_tags`; any other object or array cannot be a value of that column -> `Undecodable` (quarantined, R7),
    or ValueError when `strict` (destinations: never write a guessed value). Columns not in `types` pass through."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        t = types.get(k)
        name = getattr(t, "value", t)
        if name in _TEXT_TYPES:
            out[k] = json_text(v)
        elif t is not None and isinstance(v, (dict, list)):
            try:
                if not decode_tags or isinstance(v, list):
                    raise ValueError("object/array for a non-JSON column")
                out[k] = decode_value(v)
            except (ValueError, TypeError, ArithmeticError) as e:
                if strict:
                    raise ValueError(f"column {k}: {type(v).__name__} value is not a {name} value") from e
                out[k] = Undecodable(json.dumps(v, ensure_ascii=False, default=str))
        elif t is not None:
            out[k] = normalize_scalar(v, name)
        else:
            out[k] = v
    return out


_INTS = ("SHORT", "INT", "LONG")


def normalize_scalar(v: Any, name: Any) -> Any:
    """Snapshot scalar -> the CDC value form of its canonical type (XC-21: a keyless surrogate id or a verify
    checksum must not depend on whether a value came as text "4.70", int 4 or Decimal 4.7, or with +02:00 instead of
    UTC). Only unambiguous conversions; anything else is returned unchanged (never guessed)."""
    if v is None or isinstance(v, bool):
        return v
    try:
        if name == "DECIMAL":
            if isinstance(v, str) and v.strip():
                return Decimal(v.strip())
            if isinstance(v, int):
                return Decimal(v)
            if isinstance(v, float) and math.isfinite(v):
                return Decimal(repr(v))
        elif name in _INTS:
            if isinstance(v, str) and v.strip().lstrip("+-").isdigit():
                return int(v.strip())
            if isinstance(v, Decimal) and v.is_finite() and v == v.to_integral_value():
                return int(v)
        elif name == "LOCALDATE" and isinstance(v, str) and len(v.strip()) == 10:
            return date.fromisoformat(v.strip())
        elif name in ("LOCALDATETIME", "INSTANT"):
            d = datetime.fromisoformat(v.strip()) if isinstance(v, str) and len(v.strip()) >= 19 else v
            if isinstance(d, datetime) and d.tzinfo is not None:
                d = d.astimezone(timezone.utc)
            return d
    except (ValueError, ArithmeticError):
        return v
    return v


def snapshot_types(info: Any) -> dict[str, Any]:
    """column -> CanonicalType for canonical_snapshot_row (from a TableInfo)."""
    return {c.name: c.canonical_type for c in info.columns}

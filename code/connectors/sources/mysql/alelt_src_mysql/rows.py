"""Row image -> canonical Python values (stdlib only).

A ColumnPlan joins the binlog TABLE_MAP column (type + metadata) with the catalog column (name, signedness,
character set, enum labels, TEXT vs BLOB, MariaDB JSON-as-text). The binlog alone is enough to *size* every
value, so a catalog problem never desynchronises the row; it only makes that value undecodable (=> quarantine).
"""
from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any

from . import types as T
from . import values as V
from .events import ColumnDef, bit_set


class _Reread:
    """Marker: the value was sized and skipped; its form is not decoded by the core -> the row is re-read by key."""

    def __init__(self, why: str = "") -> None:
        self.why = why


REREAD = _Reread()


class RowError(ValueError):
    """Undecodable row; message is the quarantine reason."""


class Unconvertible(str):
    """A value that was read correctly but has no canonical form (e.g. TIME '-838:59:59' for LOCALTIME).
    The connector turns it into alelt_common.canonical_jsonl.Undecodable => the event is quarantined (R7)."""


def float32(v: float) -> float:
    """Shortest decimal that round-trips the FLOAT (binary32) value - what SELECT returns for a FLOAT column."""
    bits = struct.pack("<f", v)
    if v != v or v in (float("inf"), float("-inf")):
        return v
    for p in range(1, 10):
        x = float(f"{v:.{p}g}")
        if struct.pack("<f", x) == bits:
            return x
    return v


def json_text(v: Any) -> str:
    """Canonical JSON text (REMOTE.md S7): compact separators, key order as stored by the server."""
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


@dataclass
class ColumnPlan:
    name: str
    type: int
    meta: int
    meta2: int
    canonical: str            # canonical type name
    unsigned: bool = False
    codec: str | None = None  # Python codec for character data; None = bytes
    is_char: bool = False     # CHAR(n): trailing spaces removed (as SELECT returns them)
    labels: list[str] | None = None   # ENUM / SET labels
    json_text: bool = False   # MariaDB JSON (LONGTEXT holding JSON text)
    engine_type: str = ""
    pad_to: int | None = None  # BINARY(n): right-pad with 0x00 to n bytes (as stored / returned by SELECT)
    inet: int = 0              # MariaDB INET4 (4) / INET6 (6): fixed binary in network byte order -> text
    json_decoder: Any = None   # optional JsonBinaryDecoder plugin for MySQL JSON values (jsonhook.py, X-133)
    reread: bool = False       # value form not documented for row images (MariaDB UUID): omitted, row re-read


def inet6_text(b: bytes) -> str:
    """MariaDB INET6 text form (inet_ntop): longest run (>= 2) of zero groups as '::' (first on a tie), lower-case
    hex, embedded IPv4 for ::a.b.c.d (run of 6) and ::ffff:a.b.c.d. Verified against SELECT on MariaDB 13."""
    w = [int.from_bytes(b[i:i + 2], "big") for i in range(0, 16, 2)]
    best, blen, cur, clen = -1, 0, -1, 0
    for i, x in enumerate(w + [1]):
        if x == 0 and i < 8:
            if cur < 0:
                cur, clen = i, 0
            clen += 1
        elif cur >= 0:
            if clen > blen:
                best, blen = cur, clen
            cur = -1
    if blen < 2:
        best, blen = -1, 0
    if best == 0 and (blen == 6 or (blen == 5 and w[5] == 0xFFFF)):
        v4 = ".".join(str(x) for x in b[12:16])
        return ("::ffff:" if blen == 5 else "::") + v4
    parts: list[str] = []
    i = 0
    while i < 8:
        if i == best:
            parts.append("" if i else ":")
            parts.append("" if i + blen < 8 else "")
            i += blen
            if i == 8:
                parts.append("")
            continue
        parts.append(f"{w[i]:x}")
        i += 1
    s = ":".join(parts)
    return s.replace(":::", "::")


def _size_prefixed(buf: memoryview, off: int, nbytes: int) -> tuple[bytes, int]:
    n = int.from_bytes(buf[off:off + nbytes], "little")
    end = off + nbytes + n
    if end > len(buf):
        raise RowError("value length beyond event")
    return bytes(buf[off + nbytes:end]), end


def _text(raw: bytes, c: ColumnPlan) -> str | bytes:
    if c.codec is None:
        return raw
    try:
        s = raw.decode(c.codec)
    except UnicodeDecodeError as e:
        raise RowError(f"column {c.name}: bytes not valid {c.codec}: {e.reason}") from None
    return s.rstrip(" ") if c.is_char else s


def read_value(buf: memoryview, off: int, c: ColumnPlan) -> tuple[Any, int, str | None]:
    """(canonical value, next offset, note). Raises RowError / ValueError on undecodable data."""
    t = c.type
    note: str | None = None
    if t in (T.TINY, T.SHORT, T.INT24, T.LONG, T.LONGLONG):
        n = {T.TINY: 1, T.SHORT: 2, T.INT24: 3, T.LONG: 4, T.LONGLONG: 8}[t]
        if off + n > len(buf):
            raise RowError("truncated integer")
        v = V.le_uint(buf, off, n)
        if not c.unsigned:
            v = V.signed(v, n)
        if c.canonical == "DECIMAL":
            from decimal import Decimal
            return Decimal(v), off + n, None
        return v, off + n, None
    if t == T.FLOAT:
        return float32(V.decode_float(buf, off)), off + 4, None
    if t == T.DOUBLE:
        return V.decode_double(buf, off), off + 8, None
    if t == T.YEAR:
        y = buf[off]
        return (1900 + y if y else 0), off + 1, None
    if t in (T.NEWDECIMAL,):
        d, n = V.decode_decimal(buf, off, c.meta, c.meta2)
        return d, off + n, None
    if t in (T.DATE, T.NEWDATE):
        d, n, note = V.decode_date(buf, off)
        return d, off + n, note
    if t == T.DATETIME2:
        d, n, note = V.decode_datetime2(buf, off, c.meta)
        return d, off + n, note
    if t == T.TIMESTAMP2:
        d, n, note = V.decode_timestamp2(buf, off, c.meta)
        return d, off + n, note
    if t == T.TIMESTAMP:
        sec = V.le_uint(buf, off, 4)
        from datetime import datetime, timezone
        return (datetime.fromtimestamp(sec, tz=timezone.utc) if sec else None), off + 4, \
            (None if sec else "zero timestamp -> NULL")
    if t == T.DATETIME:
        d, n, note = V.decode_datetime_old(buf, off)
        return d, off + n, note
    if t in (T.TIME2, T.TIME):
        td, n = V.decode_time2(buf, off, c.meta) if t == T.TIME2 else V.decode_time_old(buf, off)
        if c.canonical == "STRING":
            return _fmt_duration(td), off + n, None
        tod = V.time_of_day(td)
        if tod is None:
            return Unconvertible(f"TIME {_fmt_duration(td)} outside 00:00:00..23:59:59 (LOCALTIME)"), off + n, None
        return tod, off + n, None
    if t == T.BIT:
        nbytes = c.meta + (1 if c.meta2 else 0)
        raw = bytes(buf[off:off + nbytes])
        if c.canonical == "BOOLEAN":
            return int.from_bytes(raw, "big") != 0, off + nbytes, None
        return raw, off + nbytes, None
    if t in (T.VARCHAR, T.VAR_STRING):
        raw, end = _size_prefixed(buf, off, 1 if c.meta < 256 else 2)
        return _text(raw, c), end, None
    if t == T.STRING:
        raw, end = _size_prefixed(buf, off, 1 if c.meta < 256 else 2)
        if c.codec is None and c.pad_to and len(raw) < c.pad_to:
            raw = raw + b"\0" * (c.pad_to - len(raw))
        if c.inet:                              # binlog strips trailing zero bytes of fixed binary values
            n = 4 if c.inet == 4 else 16
            if len(raw) > n:
                raise RowError(f"column {c.name}: {len(raw)} bytes for INET{c.inet}")
            raw = raw + b"\0" * (n - len(raw))
            return (".".join(str(x) for x in raw) if n == 4 else inet6_text(raw)), end, None
        return _text(raw, c), end, None
    if t == T.ENUM:
        idx = V.le_uint(buf, off, c.meta)
        labels = c.labels
        if idx == 0:
            return "", off + c.meta, None
        if not labels or idx > len(labels):
            raise RowError(f"column {c.name}: ENUM index {idx} without catalog label")
        return labels[idx - 1], off + c.meta, None
    if t == T.SET:
        bits = V.le_uint(buf, off, c.meta)
        labels = c.labels or []
        if bits >> len(labels):
            raise RowError(f"column {c.name}: SET bits 0x{bits:x} beyond catalog labels")
        return ",".join(lb for i, lb in enumerate(labels) if bits & (1 << i)), off + c.meta, None
    if t in (T.BLOB, T.TINY_BLOB, T.MEDIUM_BLOB, T.LONG_BLOB, T.GEOMETRY, T.VECTOR):
        raw, end = _size_prefixed(buf, off, c.meta)
        if c.json_text:
            if not raw:
                return None, end, None
            try:
                return json_text(json.loads(raw.decode("utf-8"))), end, None
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise RowError(f"column {c.name}: invalid JSON text: {e}") from None
        return (_text(raw, c) if t in (T.BLOB, T.TINY_BLOB, T.MEDIUM_BLOB, T.LONG_BLOB) else raw), end, None
    if t == T.JSON:
        raw, end = _size_prefixed(buf, off, c.meta)
        if not raw:
            return None, end, None
        if c.json_decoder is None:              # default (X-133): not decoded by the core, re-read by key
            return REREAD, end, None
        try:
            txt = c.json_decoder.decode(raw)
            return json_text(json.loads(txt)), end, None
        except Exception as e:  # noqa: BLE001 - plugin failure: fall back to the keyed re-read, counted
            return _Reread(f"{type(e).__name__}: {e}"[:200]), end, None
    raise RowError(f"column {c.name}: unsupported binlog column type {T.TYPE_NAMES.get(t, t)}")


def _fmt_duration(td: Any) -> str:
    neg = td.total_seconds() < 0
    td = -td if neg else td
    total = td.days * 86400 + td.seconds
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    frac = f".{td.microseconds:06d}" if td.microseconds else ""
    return f"{'-' if neg else ''}{h:02d}:{m:02d}:{s:02d}{frac}"


def read_image(buf: memoryview, off: int, plan: list[ColumnPlan], present: bytes
               ) -> tuple[dict[str, Any], int, list[str]]:
    """One row image (null bitmap + values of the present columns)."""
    npresent = sum(1 for i in range(len(plan)) if bit_set(present, i))
    nb = (npresent + 7) // 8
    if off + nb > len(buf):
        raise RowError("truncated null bitmap")
    nulls = bytes(buf[off:off + nb])
    p = off + nb
    out: dict[str, Any] = {}
    notes: list[str] = []
    j = 0
    for i, c in enumerate(plan):
        if not bit_set(present, i):
            continue
        is_null = bool(nulls[j // 8] & (1 << (j % 8)))
        j += 1
        if is_null:
            out[c.name] = None
            continue
        try:
            v, p, note = read_value(buf, p, c)
        except (V.ValueError_, struct.error, IndexError) as e:
            raise RowError(f"column {c.name}: {e}") from None
        if c.reread or isinstance(v, _Reread):
            notes.append(f"reread:{c.name}")
            if isinstance(v, _Reread) and v.why:
                notes.append(f"json_decoder_failed:{c.name}")
            continue
        out[c.name] = v
        if note:
            notes.append(f"{c.name}: {note}")
    return out, p, notes


def plan_from_defs(defs: list[ColumnDef]) -> list[ColumnPlan]:
    """Plan from binlog metadata only (no catalog): names from binlog_row_metadata=FULL or 'col<N>'."""
    out = []
    for i, d in enumerate(defs):
        canon = {T.TINY: "SHORT", T.SHORT: "SHORT", T.INT24: "INT", T.LONG: "INT", T.LONGLONG: "LONG",
                 T.FLOAT: "FLOAT", T.DOUBLE: "DOUBLE", T.NEWDECIMAL: "DECIMAL", T.DATE: "LOCALDATE",
                 T.DATETIME2: "LOCALDATETIME", T.TIMESTAMP2: "INSTANT", T.TIME2: "LOCALTIME", T.YEAR: "SHORT",
                 T.JSON: "JSON", T.BIT: "BINARY", T.VARCHAR: "STRING", T.STRING: "STRING", T.ENUM: "STRING",
                 T.SET: "STRING", T.BLOB: "BINARY", T.GEOMETRY: "BINARY"}.get(d.type, "UNKNOWN")
        if d.unsigned and d.type in T.INTEGER_CODES:
            canon = {T.TINY: "SHORT", T.SHORT: "INT", T.INT24: "INT", T.LONG: "LONG", T.LONGLONG: "DECIMAL"}[d.type]
        out.append(ColumnPlan(d.name or f"col{i + 1}", d.type, d.meta, d.meta2, canon, bool(d.unsigned),
                              "utf-8" if d.type in (T.VARCHAR, T.STRING) else None, d.type == T.STRING,
                              d.enum_values or d.set_values))
    return out

"""Decode Db2 formatted user data records (row images in DMS insert/delete/update records). Stdlib only.

Two stages, mirrored 1:1 by the C library (decoder/):
  decode_cells(): bytes -> list[Cell]   (layout work; C port = aleltd_decode_row)
  cells_to_values(): Cells -> canonical Python values (codepage, Decimal, date/time; Python only)
Layout reference: ../DESIGN.md §5.
"""
from __future__ import annotations

import codecs
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import IntEnum
from typing import Any

from . import types as T


class Err(IntEnum):
    """Error codes, identical to ALELTD_E_* in decoder/include/aleltdecode.h."""

    OK = 0
    SHORT = 1             # buffer too short for header / fixed section
    SUBREC_TYPE = 2       # sub-record type not 0/16 (special control etc.)
    NOT_USER_DATA = 3     # updatable record but not formatted user data (internal control / value compression)
    BAD_FIXED_LEN = 4
    BAD_NULL_FLAG = 5
    BAD_BCD = 6
    BAD_VAR_REF = 7       # var-length (offset,len) outside the record
    UNSUPPORTED_TYPE = 8
    BAD_BOOL = 9
    ARG = 10
    BAD_CODEPAGE = 11     # Python stage only
    VALUE_COMPRESSED = 12  # user record with inner type 0x02 (VALUE COMPRESSION offset-array format)
    POINTER = 13          # record type 0x02 bit without 0x04: row data is in another record (overflow pointer)
    LAYOUT = 14           # fixed section not exactly consumed by the catalog columns (wrong catalog version)


class Kind(IntEnum):
    """Cell kinds, identical to ALELTD_K_* in aleltdecode.h."""

    NULL = 0
    ABSENT = 1            # column not present in image (row older than ADD COLUMN)
    INT = 2
    FLOAT = 3
    TEXT = 4              # ASCII text: DECIMAL '-12.30', DATE 'YYYY-MM-DD', TIME 'HH:MM:SS', TIMESTAMP 'YYYY-MM-DDTHH:MM:SS[.f]'
    BYTES = 5             # raw character / binary data (codepage applied later)
    BOOL = 6
    LOB_UNAVAILABLE = 7   # value not in this image: out-of-row LOB / varchar, DECFLOAT, XML (value = reason code)


class RowImageError(ValueError):
    def __init__(self, code: Err, msg: str) -> None:
        super().__init__(f"{code.name}: {msg}")
        self.code = code


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str             # SYSCAT TYPENAME (built-in, normalized)
    length: int
    scale: int = 0
    nullable: bool = True
    codepage: int = 0


Cell = tuple[Kind, Any]

SUBREC_HDR = 4            # record header: u8 (outer) record type, u8, u16 length
USERDATA_HDR = 4          # u8 (inner) record type (0x01 = fixed section + formatted record), u8, u16 fixed length
LOB_INLINE = 0x69
LOB_OUT_OF_ROW = 0x49
LOB_EMPTY = 0x80
VAR_OUT_OF_ROW = 0x8000   # high bit of a varying-length field offset: data stored out of row (IBM docs)
# LOB_UNAVAILABLE reason codes for columns that are not LOBs (LOB cells carry the descriptor's first byte)
UNAVAIL_VAR_OUT_OF_ROW = -2
UNAVAIL_DECFLOAT = -3
UNAVAIL_XML = -4
UNAVAIL_LONG = -5         # LONG VARCHAR / LONG VARGRAPHIC: long field descriptor only (data: long field manager)

# IBM "Data manager log records": a user data record has outer record type 0 or 0x10, or the 0x04 bit set with
# an inner record type having bit 0x01 (formatted) or 0x02 (value-compressed). In an update, a new image with the
# 0x02 bit means the after image is in the preceding insert record of the transaction (which has the 0x04 bit).
OUTER_OVERFLOW = 0x04
OUTER_POINTER = 0x02


def outer_class(outer: int) -> str:
    """'user' | 'overflow' (0x04 bit: user record, e.g. 0xC4 / 0xD4) | 'pointer' (0x02 bit, e.g. 0x42 / 0x52) | 'other'."""
    if outer in (0, 0x10):
        return "user"
    if outer & OUTER_OVERFLOW:
        return "overflow"
    if outer & OUTER_POINTER:
        return "pointer"
    return "other"


def _bcd_digits(b: bytes, n_nibbles: int) -> str:
    out = []
    for i in range(n_nibbles):
        v = (b[i >> 1] >> (0 if i & 1 else 4)) & 0xF
        if v > 9:
            raise RowImageError(Err.BAD_BCD, f"nibble {v:X}")
        out.append(chr(48 + v))
    return "".join(out)


def _decimal_text(b: bytes, scale: int) -> str:
    sign = b[-1] & 0xF
    if sign < 0xA:
        raise RowImageError(Err.BAD_BCD, f"sign nibble {sign:X}")
    d = _bcd_digits(b, 2 * len(b) - 1)
    if scale > len(d):
        raise RowImageError(Err.BAD_BCD, "scale exceeds digits")
    ip, fp = d[:len(d) - scale].lstrip("0") or "0", d[len(d) - scale:]
    return ("-" if sign in (0xB, 0xD) else "") + ip + ("." + fp if scale else "")


def _ts_text(b: bytes, scale: int) -> str:
    d = _bcd_digits(b, 14 + scale)
    s = f"{d[0:4]}-{d[4:6]}-{d[6:8]}T{d[8:10]}:{d[10:12]}:{d[12:14]}"
    return s + ("." + d[14:] if scale else "")


def decode_cells(sub: bytes, cols: Sequence[ColumnSpec]) -> list[Cell]:
    """sub = whole sub-record (4 B header, 4 B user-data header, fixed section, variable section)."""
    if len(sub) < SUBREC_HDR + USERDATA_HDR:
        raise RowImageError(Err.SHORT, f"{len(sub)} bytes")
    oc = outer_class(sub[0])
    if oc == "pointer":
        raise RowImageError(Err.POINTER, f"record type 0x{sub[0]:02X}")
    if oc == "other":
        raise RowImageError(Err.SUBREC_TYPE, f"sub-record type {sub[0]}")
    if not sub[4] & 0x01:
        if sub[4] & 0x02:
            raise RowImageError(Err.VALUE_COMPRESSED, f"inner record type 0x{sub[4]:02X}")
        raise RowImageError(Err.NOT_USER_DATA, f"user record type {sub[4]}")
    fixed_len = int.from_bytes(sub[6:8], "little")
    data = sub[8:]                                   # var offsets are relative to here
    if fixed_len > len(data):
        raise RowImageError(Err.BAD_FIXED_LEN, f"fixed {fixed_len} > {len(data)}")
    cells: list[Cell] = []
    pos = 0
    absent = False
    for c in cols:
        t = T.norm(c.type)
        w = T.fixed_width(t, c.length, c.scale)
        if (w is None or w < 0 or t in T.UNSUPPORTED or (t == "DECIMAL" and c.length > 31)
                or (t == "TIMESTAMP" and c.scale > 12) or c.length < 0 or c.scale < 0):
            raise RowImageError(Err.UNSUPPORTED_TYPE, f"{c.name} {c.type}")
        need = w + (1 if c.nullable else 0)
        if absent or pos + need > fixed_len:
            absent = True
            cells.append((Kind.ABSENT, None))
            continue
        f = data[pos:pos + w]
        if c.nullable:
            nf = data[pos + w]
            if nf not in (0, 1):
                raise RowImageError(Err.BAD_NULL_FLAG, f"{c.name} flag {nf}")
            if nf == 1:
                cells.append((Kind.NULL, None))
                pos += need
                continue
        pos += need
        cells.append(_cell(t, c, f, data, fixed_len))
    if not absent and pos != fixed_len:
        raise RowImageError(Err.LAYOUT, f"columns use {pos} of {fixed_len} fixed bytes")
    return cells


def _cell(t: str, c: ColumnSpec, f: bytes, data: bytes, fixed_len: int) -> Cell:
    if t in ("SMALLINT", "INTEGER", "BIGINT"):
        return Kind.INT, int.from_bytes(f, "little", signed=True)
    if t == "REAL":
        return Kind.FLOAT, struct.unpack("<f", f)[0]
    if t == "DOUBLE":
        return Kind.FLOAT, struct.unpack("<d", f)[0]
    if t == "BOOLEAN":
        if f[0] > 1:
            raise RowImageError(Err.BAD_BOOL, f"{c.name} {f[0]}")
        return Kind.BOOL, f[0] == 1
    if t == "DECIMAL":
        return Kind.TEXT, _decimal_text(f, c.scale)
    if t == "DATE":
        d = _bcd_digits(f, 8)
        return Kind.TEXT, f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    if t == "TIME":
        d = _bcd_digits(f, 6)
        return Kind.TEXT, f"{d[0:2]}:{d[2:4]}:{d[4:6]}"
    if t == "TIMESTAMP":
        return Kind.TEXT, _ts_text(f, c.scale)
    if t in ("CHARACTER", "GRAPHIC", "BINARY"):
        return Kind.BYTES, bytes(f)
    if t == "DECFLOAT":
        return Kind.LOB_UNAVAILABLE, UNAVAIL_DECFLOAT
    off, ln = int.from_bytes(f[0:2], "little"), int.from_bytes(f[2:4], "little")
    if t == "XML":
        return Kind.LOB_UNAVAILABLE, UNAVAIL_XML
    if t in T.LONG_TYPES:
        return Kind.LOB_UNAVAILABLE, UNAVAIL_LONG
    if t in T.VAR_TYPES and off & VAR_OUT_OF_ROW:
        return Kind.LOB_UNAVAILABLE, UNAVAIL_VAR_OUT_OF_ROW
    if ln and (off < fixed_len or off + ln > len(data)):
        raise RowImageError(Err.BAD_VAR_REF, f"{c.name} off={off} len={ln} rec={len(data)} fixed={fixed_len}")
    v = bytes(data[off:off + ln]) if ln else b""
    if t in T.VAR_TYPES:
        return Kind.BYTES, v
    # LOB descriptor (verified on 11.5.9: 0x69 000000 + data inline; 0x49 = out of row; 0x80 = empty)
    if ln == 1 and v[0] == LOB_EMPTY:
        return Kind.BYTES, b""
    if ln >= 4 and v[0] == LOB_INLINE and v[1:4] == b"\0\0\0":
        return Kind.BYTES, v[4:]
    return Kind.LOB_UNAVAILABLE, (v[0] if v else -1)


# ------------------------------------------------------------------ stage 2: canonical values
# Db2 CCSID -> Python codec for CCSIDs without a `cp<N>` codec (SRC-db2-06; unverified on a live database except 1208)
CCSID_CODECS = {1208: "utf-8", 1200: "utf-16-be", 13488: "utf-16-be", 819: "latin-1", 912: "iso8859-2",
                915: "iso8859-5", 1089: "iso8859-6", 813: "iso8859-7", 916: "iso8859-8", 920: "iso8859-9",
                923: "iso8859-15", 943: "cp932", 932: "cp932", 5039: "shift_jis", 954: "euc_jp", 5050: "euc_jp",
                970: "euc_kr", 1363: "cp949", 1383: "gb2312", 1386: "gbk", 5488: "gb18030", 1392: "gb18030",
                950: "cp950", 1381: "gb2312", 874: "cp874", 1252: "cp1252"}


def _codec(cp: int) -> str | None:
    if cp in CCSID_CODECS:
        return CCSID_CODECS[cp]
    try:
        return codecs.lookup(f"cp{cp}").name
    except LookupError:
        return None


def _text_value(t: str, scale: int, s: str) -> Any:
    if t == "DECIMAL":
        return Decimal(s)
    if t == "DATE":
        return date.fromisoformat(s)
    if t == "TIME":
        if s.startswith("24:"):     # valid Db2 end of day: the largest LOCALTIME (docs/TYPE-MAP.md, SRC-db2-05)
            return time.max
        return time.fromisoformat(s)
    if t == "TIMESTAMP":
        base, _, frac = s.partition(".")
        # canonical LOCALDATETIME keeps microseconds; TIMESTAMP(7..12) digits below 1 us are truncated and reported
        # by cells_to_values as `precision_lost:<COL>` (docs/TYPE-MAP.md, XC-36 / SRC-db2-05)
        return datetime.fromisoformat(base).replace(microsecond=int(frac[:6].ljust(6, "0")) if frac else 0)
    raise RowImageError(Err.ARG, f"text cell for {t}")


def _lost(t: str, scale: int, s: str) -> bool:
    """True when the text value carries information the canonical value cannot hold."""
    if t == "TIMESTAMP" and scale > 6:
        return bool(s.partition(".")[2][6:].strip("0"))
    return t == "TIME" and s.startswith("24:")


def cells_to_values(cells: Sequence[Cell], cols: Sequence[ColumnSpec]) -> tuple[dict[str, Any], list[str]]:
    """Return (values, notes). ABSENT / LOB_UNAVAILABLE columns are omitted and listed in notes
    (`absent:`, `lob_unavailable:` for out-of-row LOB / varchar, `value_unavailable:` for DECFLOAT / XML)."""
    vals: dict[str, Any] = {}
    notes: list[str] = []
    for (k, v), c in zip(cells, cols):
        t = T.norm(c.type)
        if k == Kind.NULL:
            vals[c.name] = None
        elif k == Kind.ABSENT:
            notes.append(f"absent:{c.name}")
        elif k == Kind.LOB_UNAVAILABLE:
            notes.append(f"{'value_unavailable' if t in ('DECFLOAT', 'XML') else 'lob_unavailable'}:{c.name}")
        elif k in (Kind.INT, Kind.FLOAT, Kind.BOOL):
            vals[c.name] = v
        elif k == Kind.TEXT:
            try:
                vals[c.name] = _text_value(t, c.scale, v)
            except ValueError as e:
                raise RowImageError(Err.BAD_BCD, f"{c.name}: {e}") from None
            if _lost(t, c.scale, v):
                notes.append(f"precision_lost:{c.name}")
        elif t in ("BINARY", "VARBINARY", "BLOB") or c.codepage == 0:
            vals[c.name] = v
        else:
            cp = 1200 if t in ("GRAPHIC", "VARGRAPHIC", "DBCLOB") and c.codepage in (0, 1200) else c.codepage
            name = _codec(cp)
            if name is None:
                raise RowImageError(Err.BAD_CODEPAGE, f"{c.name} codepage {c.codepage}")
            try:
                vals[c.name] = v.decode(name)
            except UnicodeDecodeError as e:
                raise RowImageError(Err.BAD_CODEPAGE, f"{c.name}: {e}") from None
    return vals, notes


@dataclass
class RowResult:
    values: dict[str, Any]
    notes: list[str]


# Optional native backend (decoder/python/aleltdecode_cffi.py); set by alelt_src_db2.native.enable().
_native_cells: Any = None


def decode_row(sub: bytes, cols: Sequence[ColumnSpec]) -> RowResult:
    """Full decode; raises RowImageError (caller quarantines)."""
    cells = _native_cells(sub, cols) if _native_cells is not None else decode_cells(sub, cols)
    v, n = cells_to_values(cells, cols)
    return RowResult(v, n)

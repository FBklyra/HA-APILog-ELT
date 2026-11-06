"""pgoutput logical replication message decoder (stdlib only).

Implements the message formats documented in the PostgreSQL manual, chapter "Logical Replication Message
Formats" (protocol versions 1-4): Begin, Message, Commit, Origin, Relation, Type, Insert, Update, Delete,
Truncate, and the streaming messages Stream Start / Stop / Commit / Abort (v2+), Begin Prepare / Prepare /
Commit Prepared / Rollback Prepared / Stream Prepare (v3) are recognised and reported as `Other`.

All integers are network byte order. Strings are NUL-terminated. Timestamps are microseconds since 2000-01-01 UTC.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

_I8 = struct.Struct("!b")
_U8 = struct.Struct("!B")
_I16 = struct.Struct("!h")
_I32 = struct.Struct("!i")
_U32 = struct.Struct("!I")
_I64 = struct.Struct("!q")
_U64 = struct.Struct("!Q")


class ProtocolError(ValueError):
    pass


def pg_ts(us: int) -> datetime:
    return PG_EPOCH + timedelta(microseconds=us)


class _R:
    """Bounds-checked big-endian reader."""

    __slots__ = ("b", "o")

    def __init__(self, b: bytes) -> None:
        self.b = b
        self.o = 0

    def _take(self, s: struct.Struct) -> int:
        if self.o + s.size > len(self.b):
            raise ProtocolError(f"truncated message at offset {self.o}")
        (v,) = s.unpack_from(self.b, self.o)
        self.o += s.size
        return int(v)

    def i8(self) -> int:
        return self._take(_I8)

    def u8(self) -> int:
        return self._take(_U8)

    def i16(self) -> int:
        return self._take(_I16)

    def i32(self) -> int:
        return self._take(_I32)

    def u32(self) -> int:
        return self._take(_U32)

    def i64(self) -> int:
        return self._take(_I64)

    def u64(self) -> int:
        return self._take(_U64)

    def byte(self) -> str:
        return chr(self.u8())

    def cstr(self) -> str:
        end = self.b.find(b"\x00", self.o)
        if end < 0:
            raise ProtocolError("unterminated string")
        try:
            s = self.b[self.o:end].decode("utf-8")
        except UnicodeDecodeError as e:     # identifiers are converted to client_encoding=UTF8 by the walsender
            raise ProtocolError(f"identifier is not UTF-8 (SQL_ASCII database?): {e}") from None
        self.o = end + 1
        return s

    def raw(self, n: int) -> bytes:
        if n < 0 or self.o + n > len(self.b):
            raise ProtocolError(f"bad length {n} at offset {self.o}")
        v = self.b[self.o:self.o + n]
        self.o += n
        return v

    def done(self) -> None:
        if self.o != len(self.b):
            raise ProtocolError(f"{len(self.b) - self.o} trailing byte(s)")


# ------------------------------------------------------------------ messages
@dataclass(frozen=True)
class Begin:
    final_lsn: int
    commit_ts: datetime
    xid: int


@dataclass(frozen=True)
class Commit:
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime


@dataclass(frozen=True)
class Origin:
    commit_lsn: int
    name: str


@dataclass(frozen=True)
class RelColumn:
    key: bool
    name: str
    type_oid: int
    typmod: int


@dataclass(frozen=True)
class Relation:
    xid: int | None
    relid: int
    namespace: str
    name: str
    replident: str                 # d default, n nothing, f full, i index
    columns: tuple[RelColumn, ...]


@dataclass(frozen=True)
class TypeMsg:
    xid: int | None
    oid: int
    namespace: str
    name: str


@dataclass(frozen=True)
class Cell:
    kind: str                      # n null, u unchanged toast, t text, b binary
    data: bytes | None = None


@dataclass(frozen=True)
class Insert:
    xid: int | None
    relid: int
    new: tuple[Cell, ...]


@dataclass(frozen=True)
class Update:
    xid: int | None
    relid: int
    old_kind: str | None           # 'K' key, 'O' old full row, None
    old: tuple[Cell, ...] | None
    new: tuple[Cell, ...]


@dataclass(frozen=True)
class Delete:
    xid: int | None
    relid: int
    old_kind: str                  # 'K' or 'O'
    old: tuple[Cell, ...]


@dataclass(frozen=True)
class Truncate:
    xid: int | None
    options: int                   # 1 CASCADE, 2 RESTART IDENTITY
    relids: tuple[int, ...]


@dataclass(frozen=True)
class LogicalMessage:
    xid: int | None
    transactional: bool
    lsn: int
    prefix: str
    content: bytes


@dataclass(frozen=True)
class StreamStart:
    xid: int
    first_segment: bool


@dataclass(frozen=True)
class StreamStop:
    pass


@dataclass(frozen=True)
class StreamCommit:
    xid: int
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime


@dataclass(frozen=True)
class StreamAbort:
    xid: int
    subxid: int
    abort_lsn: int | None = None
    abort_ts: datetime | None = None


@dataclass(frozen=True)
class Other:
    tag: str
    body: bytes = field(repr=False, default=b"")


Message = (Begin | Commit | Origin | Relation | TypeMsg | Insert | Update | Delete | Truncate | LogicalMessage
           | StreamStart | StreamStop | StreamCommit | StreamAbort | Other)


def _tuple(r: _R) -> tuple[Cell, ...]:
    n = r.i16()
    if n < 0:
        raise ProtocolError("negative column count")
    out = []
    for _ in range(n):
        k = r.byte()
        if k in ("n", "u"):
            out.append(Cell(k))
        elif k in ("t", "b"):
            out.append(Cell(k, r.raw(r.i32())))
        else:
            raise ProtocolError(f"unknown tuple column kind {k!r}")
    return tuple(out)


def decode(buf: bytes, in_stream: bool = False, proto_version: int = 1) -> Message:
    """Decode one pgoutput message. `in_stream`: between Stream Start and Stream Stop (DML carry an xid)."""
    if not buf:
        raise ProtocolError("empty message")
    r = _R(bytes(buf))
    tag = r.byte()
    sx = in_stream
    m: Message
    if tag == "B":
        m = Begin(r.i64(), pg_ts(r.i64()), r.u32())
    elif tag == "C":
        m = Commit(r.i8(), r.i64(), r.i64(), pg_ts(r.i64()))
    elif tag == "O":
        m = Origin(r.i64(), r.cstr())
    elif tag == "R":
        xid = r.u32() if sx else None
        relid, ns, name, ident = r.u32(), r.cstr(), r.cstr(), r.byte()
        cols = []
        for _ in range(r.i16()):
            flags = r.i8()
            cols.append(RelColumn(bool(flags & 1), r.cstr(), r.u32(), r.i32()))
        m = Relation(xid, relid, ns, name, ident, tuple(cols))
    elif tag == "Y":
        xid = r.u32() if sx else None
        m = TypeMsg(xid, r.u32(), r.cstr(), r.cstr())
    elif tag == "I":
        xid = r.u32() if sx else None
        relid = r.u32()
        if r.byte() != "N":
            raise ProtocolError("Insert without 'N' tuple")
        m = Insert(xid, relid, _tuple(r))
    elif tag == "U":
        xid = r.u32() if sx else None
        relid = r.u32()
        k = r.byte()
        old_kind, old = None, None
        if k in ("K", "O"):
            old_kind, old = k, _tuple(r)
            k = r.byte()
        if k != "N":
            raise ProtocolError("Update without 'N' tuple")
        m = Update(xid, relid, old_kind, old, _tuple(r))
    elif tag == "D":
        xid = r.u32() if sx else None
        relid = r.u32()
        k = r.byte()
        if k not in ("K", "O"):
            raise ProtocolError("Delete without 'K'/'O' tuple")
        m = Delete(xid, relid, k, _tuple(r))
    elif tag == "T":
        xid = r.u32() if sx else None
        n = r.i32()
        opts = r.i8()
        m = Truncate(xid, opts, tuple(r.u32() for _ in range(n)))
    elif tag == "M":
        xid = r.u32() if sx else None
        flags = r.i8()
        lsn, prefix = r.i64(), r.cstr()
        m = LogicalMessage(xid, bool(flags & 1), lsn, prefix, r.raw(r.i32()))
    elif tag == "S":
        m = StreamStart(r.u32(), bool(r.i8()))
    elif tag == "E":
        m = StreamStop()
    elif tag == "c":
        m = StreamCommit(r.u32(), r.i8(), r.i64(), r.i64(), pg_ts(r.i64()))
    elif tag == "A":
        xid, sub = r.u32(), r.u32()
        if r.o < len(r.b):                         # v4 (parallel streaming): abort lsn + timestamp
            m = StreamAbort(xid, sub, r.i64(), pg_ts(r.i64()))
        else:
            m = StreamAbort(xid, sub)
    elif tag in ("b", "P", "K", "r", "p"):         # two-phase (v3): not requested by ALELT
        return Other(tag, bytes(buf[1:]))
    else:
        raise ProtocolError(f"unknown pgoutput message tag {tag!r}")
    r.done()
    return m

"""Framing of a db2ReadLog (filter ON) buffer (stdlib only).

Buffer = repeated [db2ReadLogFilterData 56 B][log record, recordSize B]. See ../DESIGN.md §2-§4.
All integers little-endian (Db2 LUW on x86-64).
"""
from __future__ import annotations

import struct
from collections.abc import Iterator
from dataclasses import dataclass

from .position import Lri

FILTER_HDR = struct.Struct("<QQQQQQIi")      # LRI type1 (24), LRI type2 (24), realLogRecLen, sqlcode
LOG_HDR = struct.Struct("<IHHQQQ6sH")        # 40 B log manager header
DMS_HDR = struct.Struct("<BBHH")             # component, function, tbspaceid, tableid
FILTER_SIZE = FILTER_HDR.size                 # 56
LOG_HDR_SIZE = LOG_HDR.size                   # 40
DMS_HDR_SIZE = DMS_HDR.size                   # 6

# log manager record types
RT_NORMAL = 0x004E
RT_CLR = 0x0043
RT_COMMIT = 0x0084
RT_PENDING_COMMIT = 0x008A     # local pending list (commit variant)
RT_ABORT = 0x0041
RT_REDO = 0x0052               # e.g. LOB manager data, initialize table
RT_UNDO = 0x0055               # e.g. DMS "start of out-of-row data"
CLR_FLAG_EXTRA = 0x0002        # CLR header has one more u64

COMP_DMS = 1
COMP_DOM = 4                   # data object manager: create/drop table, index, truncate, reorg
COMP_LOB = 5                   # LOB manager (12 B header: comp, op, tbspace, object, parent tbspace, parent object)
COMP_LF = 3                    # long field manager (10 B header, same id fields as the LOB manager)
COMP_RDS = 11                  # relation manager (DDL statement / partition information)
RT_LOAD_START = 0x004A         # utility manager: u32 id, u16 pool (table space) id, u16 object (table) id, u32 flag
# DMS function ids
F_DELETE, F_INSERT, F_UPDATE = 161, 162, 163
F_DELETE_EMPTY, F_INSERT_EMPTY, F_UNDO_DELETE_EMPTY = 164, 165, 166
F_UNDO_INSERT, F_UNDO_DELETE, F_UNDO_UPDATE = 110, 111, 112
F_UNDO_INSERT_EMPTY = 131
F_INSERT_MULTI, F_UNDO_INSERT_MULTI = 167, 168
F_PART_STATE, F_UNDO_PART_STATE = 137, 108
F_ALTER_ATTR = 124
RID_OFF = DMS_HDR_SIZE + 6     # RID (6 B) in insert/delete/update/undo-insert bodies


class FramingError(ValueError):
    """Buffer cannot be framed (truncated/corrupt); offset points at the bad item."""

    def __init__(self, msg: str, offset: int) -> None:
        super().__init__(f"{msg} at offset {offset}")
        self.offset = offset


@dataclass(frozen=True)
class LogRecord:
    offset: int            # offset of the filter header in the buffer
    length: int            # 56 + record size
    lri: Lri
    real_len: int
    sqlcode: int           # non-zero: Db2 could not decompress this record
    size: int
    rtype: int
    flag: int
    lsn: int
    lfs: int
    back_lso: int
    tid: bytes
    record: bytes          # whole log record incl. 40 B header

    @property
    def txid(self) -> str:
        return self.tid.hex().upper()

    @property
    def header_size(self) -> int:
        """Log manager header size: 40, CLR +16 (+8 if flag & 0x0002)."""
        if self.rtype == RT_CLR:
            return LOG_HDR_SIZE + 16 + (8 if self.flag & CLR_FLAG_EXTRA else 0)
        return LOG_HDR_SIZE

    @property
    def body(self) -> bytes:
        return self.record[self.header_size:]


def iter_records(buf: bytes) -> Iterator[LogRecord]:
    """Yield records; raises FramingError on the first item that does not fit."""
    off, n = 0, len(buf)
    while off < n:
        if n - off < FILTER_SIZE + LOG_HDR_SIZE:
            raise FramingError("truncated record prefix", off)
        _t1, p1, p2, _t2, _q1, _q2, real_len, sqlcode = FILTER_HDR.unpack_from(buf, off)
        if _t1 != 1:
            raise FramingError(f"bad LRI type {_t1}", off)
        rec_off = off + FILTER_SIZE
        size, rtype, flag, lsn, lfs, back, tid, _stream = LOG_HDR.unpack_from(buf, rec_off)
        if size < LOG_HDR_SIZE or size > n - rec_off:
            raise FramingError(f"bad record size {size}", off)
        rec = LogRecord(off, FILTER_SIZE + size, Lri(p1, p2), real_len, sqlcode, size, rtype, flag,
                        lsn, lfs, back, tid, bytes(buf[rec_off:rec_off + size]))
        if rec.header_size > size:
            raise FramingError("record shorter than its header", off)
        yield rec
        off = rec_off + size


@dataclass(frozen=True)
class DmsHeader:
    component: int
    function: int
    tbspaceid: int
    tableid: int


def dms_header(body: bytes) -> DmsHeader | None:
    if len(body) < DMS_HDR_SIZE:
        return None
    return DmsHeader(*DMS_HDR.unpack_from(body, 0))


@dataclass(frozen=True)
class DmsImages:
    """Row images carried by a DMS record. Bytes are whole sub-records (4 B record header first)."""

    before: bytes | None
    after: bytes | None


class DmsLayoutError(ValueError):
    pass


def _u16(b: bytes, off: int) -> int:
    if off + 2 > len(b):
        raise DmsLayoutError(f"read past end ({off}+2 > {len(b)})")
    return int.from_bytes(b[off:off + 2], "little")


def _subrecord(b: bytes, off: int) -> tuple[bytes, int]:
    """Sub-record at off; its own 4-byte header carries the length (bytes 2..3)."""
    ln = _u16(b, off + 2)
    if ln < 4 or off + ln > len(b):
        raise DmsLayoutError(f"sub-record length {ln} at {off} exceeds {len(b)}")
    return b[off:off + ln], off + ln


def dms_images(body: bytes, hdr: DmsHeader) -> DmsImages:
    """Extract images. Layout after the 6 B DMS header:
    insert/delete/undo-delete/undo-update: u16 internal, u16 len, u16 free, RID 6, u16 offset, sub-record
    *-empty-page variants:                 3 more u16 before the offset (sub-record at +20)
    update: [old part as above][DMS hdr 6][new part as above]
    undo-insert: no image.
    """
    f = hdr.function
    base = DMS_HDR_SIZE
    if f in (F_INSERT, F_DELETE, F_UNDO_DELETE, F_UNDO_UPDATE):
        img, _ = _subrecord(body, base + 14)
        return DmsImages(img if f == F_DELETE else None, None if f == F_DELETE else img)
    if f in (F_INSERT_EMPTY, F_DELETE_EMPTY, F_UNDO_DELETE_EMPTY):
        img, _ = _subrecord(body, base + 20)
        return DmsImages(img if f == F_DELETE_EMPTY else None, None if f == F_DELETE_EMPTY else img)
    if f == F_UPDATE:
        old, nxt = _subrecord(body, base + 14)
        if nxt + DMS_HDR_SIZE > len(body) or body[nxt + 1] != F_UPDATE:
            raise DmsLayoutError("second DMS header of update not found")
        new, _ = _subrecord(body, nxt + DMS_HDR_SIZE + 14)
        return DmsImages(old, new)
    if f in (F_UNDO_INSERT, F_UNDO_INSERT_EMPTY):
        return DmsImages(None, None)
    raise DmsLayoutError(f"no image layout for DMS function {f}")


def dms_multi(body: bytes, function: int) -> list[tuple[bytes, bytes | None]]:
    """Insert multiple records (167) / undo (168), IBM "Data manager log records": 20-byte fixed part (number of
    records at 8), then per record RID 6, record offset 2 and (167 only) the record header + data."""
    n = _u16(body, 8)
    off, out = 20, []
    for _ in range(n):
        if off + 8 > len(body):
            raise DmsLayoutError(f"multi-record description at {off} exceeds {len(body)}")
        r = bytes(body[off:off + 6])
        if function == F_INSERT_MULTI:
            sub, off = _subrecord(body, off + 8)
            out.append((r, sub))
        else:
            out.append((r, None))
            off += 8
    return out


def load_target(body: bytes) -> tuple[int, int] | None:
    """Load start: (pool id, object id) at body offsets 4 and 6 (= TBSPACEID, TABLEID; verified on 11.5.9)."""
    if len(body) < 8:
        return None
    return int.from_bytes(body[4:6], "little"), int.from_bytes(body[6:8], "little")


def rid(body: bytes) -> bytes | None:
    """RID of an insert / delete / update / undo-insert DMS record (body offset 12, 6 bytes)."""
    return bytes(body[RID_OFF:RID_OFF + 6]) if len(body) >= RID_OFF + 6 else None


def lob_parent(body: bytes) -> tuple[int, int] | None:
    """LOB manager record: (parent tbspaceid, parent tableid) = the base table (IBM 'LOB manager log records')."""
    if len(body) < 12:
        return None
    return int.from_bytes(body[6:8], "little"), int.from_bytes(body[8:10], "little")


def commit_time(body: bytes) -> int | None:
    """Commit / pending-list commit: u32 UTC seconds at body offset 0."""
    return int.from_bytes(body[0:4], "little") if len(body) >= 4 else None


def auth_id(rec: LogRecord) -> str | None:
    """Commit: u32 time, u32, u16 len, authid. Abort: u16 len, authid."""
    b = rec.body
    off = 8 if rec.rtype in (RT_COMMIT, RT_PENDING_COMMIT) else 0
    if len(b) < off + 2:
        return None
    ln = _u16(b, off)
    return b[off + 2:off + 2 + ln].decode("utf-8", "replace").rstrip() if off + 2 + ln <= len(b) else None

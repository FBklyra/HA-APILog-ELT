"""Binlog v4 event parsing (stdlib only). Public formats: MySQL "Binary Log" / replication protocol documentation and
the MariaDB knowledge base (binlog event types 160+).

Every function takes the full event bytes (19-byte common header + post-header + body [+ 4-byte CRC32]) and the
current FormatDescription. Nothing here knows the catalog; column values are decoded in rows.py.
"""
from __future__ import annotations

import struct
import uuid
import zlib
from dataclasses import dataclass, field

from .position import GtidSet, MariaGtidList

HEADER_LEN = 19

# event type codes (MySQL)
UNKNOWN_EVENT = 0
START_EVENT_V3 = 1
QUERY_EVENT = 2
STOP_EVENT = 3
ROTATE_EVENT = 4
INTVAR_EVENT = 5
RAND_EVENT = 13
USER_VAR_EVENT = 14
FORMAT_DESCRIPTION_EVENT = 15
XID_EVENT = 16
TABLE_MAP_EVENT = 19
WRITE_ROWS_EVENT_V1 = 23
UPDATE_ROWS_EVENT_V1 = 24
DELETE_ROWS_EVENT_V1 = 25
INCIDENT_EVENT = 26
HEARTBEAT_LOG_EVENT = 27
ROWS_QUERY_LOG_EVENT = 29
WRITE_ROWS_EVENT = 30
UPDATE_ROWS_EVENT = 31
DELETE_ROWS_EVENT = 32
GTID_LOG_EVENT = 33
ANONYMOUS_GTID_LOG_EVENT = 34
PREVIOUS_GTIDS_LOG_EVENT = 35
TRANSACTION_CONTEXT_EVENT = 36
VIEW_CHANGE_EVENT = 37
XA_PREPARE_LOG_EVENT = 38
PARTIAL_UPDATE_ROWS_EVENT = 39
TRANSACTION_PAYLOAD_EVENT = 40
HEARTBEAT_LOG_EVENT_V2 = 41
GTID_TAGGED_LOG_EVENT = 42
# MariaDB
ANNOTATE_ROWS_EVENT = 160
BINLOG_CHECKPOINT_EVENT = 161
MARIA_GTID_EVENT = 162
GTID_LIST_EVENT = 163
START_ENCRYPTION_EVENT = 164
QUERY_COMPRESSED_EVENT = 165
WRITE_ROWS_COMPRESSED_EVENT_V1 = 166
UPDATE_ROWS_COMPRESSED_EVENT_V1 = 167
DELETE_ROWS_COMPRESSED_EVENT_V1 = 168
WRITE_ROWS_COMPRESSED_EVENT = 169
UPDATE_ROWS_COMPRESSED_EVENT = 170
DELETE_ROWS_COMPRESSED_EVENT = 171

WRITE_TYPES = frozenset({WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT, WRITE_ROWS_COMPRESSED_EVENT_V1,
                         WRITE_ROWS_COMPRESSED_EVENT})
UPDATE_TYPES = frozenset({UPDATE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT, UPDATE_ROWS_COMPRESSED_EVENT_V1,
                          UPDATE_ROWS_COMPRESSED_EVENT, PARTIAL_UPDATE_ROWS_EVENT})
DELETE_TYPES = frozenset({DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT, DELETE_ROWS_COMPRESSED_EVENT_V1,
                          DELETE_ROWS_COMPRESSED_EVENT})
ROWS_TYPES = WRITE_TYPES | UPDATE_TYPES | DELETE_TYPES
ROWS_V2 = frozenset({WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT, DELETE_ROWS_EVENT, PARTIAL_UPDATE_ROWS_EVENT,
                     WRITE_ROWS_COMPRESSED_EVENT, UPDATE_ROWS_COMPRESSED_EVENT, DELETE_ROWS_COMPRESSED_EVENT})
COMPRESSED_ROWS = frozenset({WRITE_ROWS_COMPRESSED_EVENT_V1, UPDATE_ROWS_COMPRESSED_EVENT_V1,
                             DELETE_ROWS_COMPRESSED_EVENT_V1, WRITE_ROWS_COMPRESSED_EVENT,
                             UPDATE_ROWS_COMPRESSED_EVENT, DELETE_ROWS_COMPRESSED_EVENT})

LOG_EVENT_ARTIFICIAL_F = 0x20
CHECKSUM_OFF, CHECKSUM_CRC32 = 0, 1


class EventError(ValueError):
    """Malformed event; message is the quarantine reason."""


@dataclass
class Header:
    timestamp: int
    type: int
    server_id: int
    size: int
    log_pos: int
    flags: int


def parse_header(ev: bytes | memoryview) -> Header:
    if len(ev) < HEADER_LEN:
        raise EventError("event shorter than the 19-byte header")
    ts, typ, sid, size, pos, flags = struct.unpack_from("<IBIIIH", ev, 0)
    return Header(ts, typ, sid, size, pos, flags)


@dataclass
class FormatDescription:
    binlog_version: int = 4
    server_version: str = ""
    header_len: int = HEADER_LEN
    post_header: bytes = b""
    checksum: int = CHECKSUM_OFF

    def post_len(self, typ: int, default: int) -> int:
        if 1 <= typ <= len(self.post_header):
            return self.post_header[typ - 1]
        return default

    @property
    def mariadb(self) -> bool:
        return "mariadb" in self.server_version.lower()


def parse_fde(ev: bytes) -> FormatDescription:
    """FORMAT_DESCRIPTION_EVENT. Checksum algorithm = byte before the trailing CRC (servers >= 5.6.1)."""
    b = ev[HEADER_LEN:]
    if len(b) < 2 + 50 + 4 + 1:
        raise EventError("short FORMAT_DESCRIPTION_EVENT")
    ver = struct.unpack_from("<H", b, 0)[0]
    sv = bytes(b[2:52]).split(b"\0", 1)[0].decode("ascii", "replace")
    hlen = b[56]
    rest = bytes(b[57:])
    checksum = CHECKSUM_OFF
    phl = rest
    if _version_tuple(sv) >= (5, 6, 1) and len(rest) >= 5:
        checksum = rest[-5]
        phl = rest[:-5]
    return FormatDescription(ver, sv, hlen, phl, checksum)


def _version_tuple(v: str) -> tuple[int, int, int]:
    nums = []
    for part in v.split("-")[0].split(".")[:3]:
        digits = "".join(ch for ch in part if ch.isdigit())
        nums.append(int(digits or 0))
    while len(nums) < 3:
        nums.append(0)
    return nums[0], nums[1], nums[2]


def body(ev: bytes, fde: FormatDescription, typ: int | None = None) -> memoryview:
    """Event bytes after the common header, without the CRC32. Verifies the CRC when enabled."""
    t = ev[4] if typ is None else typ
    mv = memoryview(ev)
    if fde.checksum == CHECKSUM_CRC32 and t != FORMAT_DESCRIPTION_EVENT:
        if len(ev) < HEADER_LEN + 4:
            raise EventError("event too short for CRC32")
        want = struct.unpack_from("<I", ev, len(ev) - 4)[0]
        if zlib.crc32(mv[:-4]) & 0xFFFFFFFF != want:
            raise EventError("CRC32 mismatch")
        return mv[HEADER_LEN:-4]
    if t == FORMAT_DESCRIPTION_EVENT and fde.checksum == CHECKSUM_CRC32:
        return mv[HEADER_LEN:-4]
    return mv[HEADER_LEN:]


def lenenc(b: memoryview | bytes, off: int) -> tuple[int | None, int]:
    x = b[off]
    if x < 0xFB:
        return x, off + 1
    if x == 0xFB:
        return None, off + 1
    if x == 0xFC:
        return struct.unpack_from("<H", b, off + 1)[0], off + 3
    if x == 0xFD:
        return int.from_bytes(b[off + 1:off + 4], "little"), off + 4
    return struct.unpack_from("<Q", b, off + 1)[0], off + 9


# ------------------------------------------------------------------------------------------------ simple events
@dataclass
class Rotate:
    position: int
    next_file: str


def has_valid_crc(ev: bytes) -> bool:
    return len(ev) >= HEADER_LEN + 4 and zlib.crc32(ev[:-4]) & 0xFFFFFFFF == struct.unpack_from("<I", ev, len(ev) - 4)[0]


def parse_rotate(ev: bytes, fde: FormatDescription, fde_known: bool = True) -> Rotate:
    """The first (artificial) ROTATE arrives before any FORMAT_DESCRIPTION: detect its CRC32 by value."""
    if not fde_known:
        fde = FormatDescription(checksum=CHECKSUM_CRC32 if has_valid_crc(ev) else CHECKSUM_OFF)
    b = body(ev, fde)
    pos = struct.unpack_from("<Q", b, 0)[0]
    return Rotate(pos, bytes(b[8:]).decode("utf-8", "replace"))


@dataclass
class Query:
    thread_id: int
    exec_time: int
    error_code: int
    schema: str
    sql: str


def parse_query(ev: bytes, fde: FormatDescription) -> Query:
    typ = ev[4]
    b = body(ev, fde)
    phl = fde.post_len(QUERY_EVENT, 13) if typ != QUERY_COMPRESSED_EVENT else fde.post_len(typ, 13)
    if len(b) < phl:
        raise EventError("short QUERY_EVENT")
    thread_id, exec_time, db_len, err, sv_len = struct.unpack_from("<IIBHH", b, 0)
    p = phl + sv_len
    schema = bytes(b[p:p + db_len]).decode("utf-8", "replace")
    p += db_len + 1
    raw = bytes(b[p:])
    if typ == QUERY_COMPRESSED_EVENT:            # MariaDB log_bin_compress: the statement text is compressed
        raw = maria_uncompress(raw)
    sql = raw.decode("utf-8", "replace")
    return Query(thread_id, exec_time, err, schema, sql)


def maria_uncompress(buf: bytes) -> bytes:
    """MariaDB compressed binlog payload (KB "Compressed Binary Log"; log_event.cc binlog_buf_uncompress):
    byte 0 = 0x80 | algorithm << 4 (0 = zlib) | n, then the uncompressed length in n big-endian bytes, then zlib data."""
    if not buf or buf[0] & 0x80 == 0:
        raise EventError("compressed event without the 0x80 header byte")
    algo, n = (buf[0] >> 4) & 0x07, buf[0] & 0x07
    if algo != 0 or not 1 <= n <= 4 or len(buf) < 1 + n:
        raise EventError(f"unsupported MariaDB compression header 0x{buf[0]:02x}")
    want = int.from_bytes(buf[1:1 + n], "big")
    out = zlib.decompress(buf[1 + n:])
    if len(out) != want:
        raise EventError(f"MariaDB compressed event: {len(out)} bytes, header says {want}")
    return out


# ------------------------------------------------------------------ MySQL serialization (Gtid_tagged_log_event)
def ser_uint(b: bytes | memoryview, p: int) -> tuple[int, int]:
    """mysql::serialization variable-length unsigned integer: the number of trailing 1-bits of the first byte gives
    the number of extra bytes (8 = a full 8-byte value follows); value = little-endian bytes >> (extra + 1)."""
    if p >= len(b):
        raise EventError("truncated serialized integer")
    first = b[p]
    extra = 0
    while extra < 8 and first & (1 << extra):
        extra += 1
    if extra == 8:
        if p + 9 > len(b):
            raise EventError("truncated serialized integer")
        return int.from_bytes(bytes(b[p + 1:p + 9]), "little"), p + 9
    n = extra + 1
    if p + n > len(b):
        raise EventError("truncated serialized integer")
    return int.from_bytes(bytes(b[p:p + n]), "little") >> n, p + n


def ser_int(b: bytes | memoryview, p: int) -> tuple[int, int]:
    """Signed: zig-zag over ser_uint."""
    u, p = ser_uint(b, p)
    return (u >> 1) ^ -(u & 1), p


@dataclass
class TaggedGtid:
    sid: str
    tag: str
    gno: int
    commit_us: int | None


def parse_tagged_gtid(ev: bytes, fde: FormatDescription) -> TaggedGtid:
    """GTID_TAGGED_LOG_EVENT (MySQL 8.3+, gtid_next='AUTOMATIC:<tag>'): a serialized message
    (format version, size, last non-ignorable field id, then (field id, value) pairs). Fields used: 1 uuid (16 bytes,
    each a serialized integer), 2 gno (signed), 3 tag (length-prefixed string), 6 immediate commit timestamp,
    7 original commit timestamp (optional, defaults to 6). Unknown ignorable fields after the last non-ignorable one
    are skipped only when they are not needed; an unknown field id <= the last non-ignorable one is an error."""
    b = bytes(body(ev, fde))
    _, p = ser_uint(b, 0)                       # format version
    size, p = ser_uint(b, p)
    end = min(len(b), size)
    last_needed, p = ser_uint(b, p)
    sid_bytes: bytes | None = None
    gno: int | None = None
    tag = ""
    imm: int | None = None
    orig: int | None = None
    while p < end:
        fid, p = ser_uint(b, p)
        if fid == 0:                             # gtid flags
            _, p = ser_uint(b, p)
        elif fid == 1:
            out = bytearray()
            for _ in range(16):
                v, p = ser_uint(b, p)
                out.append(v & 0xFF)
            sid_bytes = bytes(out)
        elif fid == 2:
            gno, p = ser_int(b, p)
        elif fid == 3:
            n, p = ser_uint(b, p)
            tag = b[p:p + n].decode("ascii")
            p += n
        elif fid in (4, 5):                      # last_committed, sequence_number
            _, p = ser_int(b, p)
        elif fid == 6:
            imm, p = ser_uint(b, p)
        elif fid == 7:
            orig, p = ser_uint(b, p)
        elif fid in (8, 9, 10, 11):              # transaction length, server versions, commit group ticket
            _, p = ser_uint(b, p)
        else:
            if fid <= last_needed:
                raise EventError(f"GTID_TAGGED_LOG_EVENT: unknown required field {fid}")
            break                                # ignorable fields of a newer format: not needed here
    if sid_bytes is None or gno is None or gno <= 0:
        raise EventError("GTID_TAGGED_LOG_EVENT without uuid / gno")
    return TaggedGtid(str(uuid.UUID(bytes=sid_bytes)), tag.lower(), gno, orig if orig is not None else imm)


# ------------------------------------------------------------------ MySQL transaction payload (compression)
def parse_transaction_payload(ev: bytes, fde: FormatDescription) -> list[bytes]:
    """TRANSACTION_PAYLOAD_EVENT (binlog_transaction_compression=ON, MySQL 8.0.20+): header fields
    (type, length, value as net_field_length integers): 0 end mark, 1 payload size, 2 compression type
    (0 ZSTD, 255 NONE), 3 uncompressed size; then the payload = the transaction's events back to back."""
    b = bytes(body(ev, fde))
    p = 0
    ctype, size, usize = None, None, None
    while p < len(b):
        ft, p = lenenc(b, p)
        if ft == 0:
            break
        ln, p = lenenc(b, p)
        val, _ = lenenc(b, p)
        p += int(ln or 0)
        if ft == 1:
            size = val
        elif ft == 2:
            ctype = val
        elif ft == 3:
            usize = val
    data = b[p:p + size] if size is not None else b[p:]
    if ctype == 255:
        raw = data
    elif ctype == 0:
        try:
            import zstandard
        except ImportError:
            raise EventError("compressed transaction payload needs the zstandard package") from None
        raw = zstandard.ZstdDecompressor().decompress(data, max_output_size=int(usize or 0) or (1 << 31))
    else:
        raise EventError(f"transaction payload compression type {ctype} not documented")
    if usize is not None and len(raw) != usize:
        raise EventError(f"transaction payload: {len(raw)} bytes, header says {usize}")
    out, q = [], 0
    while q < len(raw):
        if q + HEADER_LEN > len(raw):
            raise EventError("truncated event inside the transaction payload")
        n = struct.unpack_from("<I", raw, q + 9)[0]
        if n < HEADER_LEN or q + n > len(raw):
            raise EventError("bad event size inside the transaction payload")
        out.append(raw[q:q + n])
        q += n
    return out


def parse_xid(ev: bytes, fde: FormatDescription) -> int:
    return int(struct.unpack_from("<Q", body(ev, fde), 0)[0])


@dataclass
class MysqlGtid:
    sid: str                      # uuid text ('00000000-...' for ANONYMOUS_GTID)
    gno: int
    commit_us: int | None         # original commit timestamp (us since epoch), else immediate
    anonymous: bool = False


def parse_mysql_gtid(ev: bytes, fde: FormatDescription, anonymous: bool = False) -> MysqlGtid:
    b = body(ev, fde)
    if len(b) < 25:
        raise EventError("short GTID_LOG_EVENT")
    sid = str(uuid.UUID(bytes=bytes(b[1:17])))
    gno = struct.unpack_from("<q", b, 17)[0]
    commit_us = None
    p = 25
    if len(b) >= p + 17:            # lt_type(1) last_committed(8) sequence_number(8)
        p += 17
        if len(b) >= p + 7:
            imm = int.from_bytes(b[p:p + 7], "little")
            p += 7
            if imm & (1 << 55):
                imm &= (1 << 55) - 1
                orig = int.from_bytes(b[p:p + 7], "little") if len(b) >= p + 7 else imm
                commit_us = orig
            else:
                commit_us = imm
    return MysqlGtid(sid, gno, commit_us or None, anonymous)


def parse_previous_gtids(ev: bytes, fde: FormatDescription) -> GtidSet:
    return GtidSet.decode(bytes(body(ev, fde)))


@dataclass
class MariaGtid:
    domain: int
    server: int
    seq: int
    standalone: bool              # FL_STANDALONE: no terminating COMMIT/XID (DDL, non-transactional statement)
    flags: int


FL_STANDALONE = 1
FL_GROUP_COMMIT_ID = 2
FL_TRANSACTIONAL = 4


def parse_maria_gtid(ev: bytes, fde: FormatDescription) -> MariaGtid:
    h = parse_header(ev)
    b = body(ev, fde)
    if len(b) < 13:
        raise EventError("short MariaDB GTID_EVENT")
    seq, domain, flags = struct.unpack_from("<QIB", b, 0)
    return MariaGtid(domain, h.server_id, seq, bool(flags & FL_STANDALONE), flags)


def parse_gtid_list(ev: bytes, fde: FormatDescription) -> MariaGtidList:
    b = body(ev, fde)
    n = struct.unpack_from("<I", b, 0)[0] & 0x0FFFFFFF
    out = MariaGtidList()
    for i in range(n):
        d, s, q = struct.unpack_from("<IIQ", b, 4 + 16 * i)
        out.set(d, s, q)
    return out


def parse_annotate(ev: bytes, fde: FormatDescription) -> str:
    return bytes(body(ev, fde)).decode("utf-8", "replace")


# ------------------------------------------------------------------------------------------------ TABLE_MAP
# optional metadata field types (binlog_row_metadata)
OM_SIGNEDNESS, OM_DEFAULT_CHARSET, OM_COLUMN_CHARSET, OM_COLUMN_NAME = 1, 2, 3, 4
OM_SET_STR_VALUE, OM_ENUM_STR_VALUE, OM_GEOMETRY_TYPE, OM_SIMPLE_PRIMARY_KEY = 5, 6, 7, 8
OM_PRIMARY_KEY_WITH_PREFIX, OM_ENUM_AND_SET_DEFAULT_CHARSET, OM_ENUM_AND_SET_COLUMN_CHARSET = 9, 10, 11
OM_COLUMN_VISIBILITY = 12

from . import types as T  # noqa: E402

_META_1 = frozenset({T.FLOAT, T.DOUBLE, T.BLOB, T.TINY_BLOB, T.MEDIUM_BLOB, T.LONG_BLOB, T.GEOMETRY, T.JSON,
                     T.TIMESTAMP2, T.DATETIME2, T.TIME2, T.VECTOR})
_META_2 = frozenset({T.VARCHAR, T.VAR_STRING, T.BIT, T.NEWDECIMAL, T.STRING, T.ENUM, T.SET, T.DECIMAL})


@dataclass
class ColumnDef:
    type: int                 # real type (STRING metadata resolved to ENUM / SET / STRING)
    meta: int                 # type-specific (see rows.py)
    nullable: bool
    meta2: int = 0            # second metadata byte (NEWDECIMAL scale, BIT bits%8)
    name: str | None = None
    unsigned: bool | None = None
    enum_values: list[str] | None = None
    set_values: list[str] | None = None


@dataclass
class TableMap:
    table_id: int
    schema: str
    table: str
    columns: list[ColumnDef]
    pk: list[int] = field(default_factory=list)


def parse_table_map(ev: bytes, fde: FormatDescription) -> TableMap:
    b = body(ev, fde)
    phl = fde.post_len(TABLE_MAP_EVENT, 8)
    if phl == 6:
        table_id = struct.unpack_from("<I", b, 0)[0]
    else:
        table_id = int.from_bytes(b[0:6], "little")
    p = phl
    dl = b[p]
    schema = bytes(b[p + 1:p + 1 + dl]).decode("utf-8", "replace")
    p += 1 + dl + 1
    tl = b[p]
    table = bytes(b[p + 1:p + 1 + tl]).decode("utf-8", "replace")
    p += 1 + tl + 1
    n, p = lenenc(b, p)
    assert n is not None
    ctypes = list(b[p:p + n])
    p += n
    mlen, p = lenenc(b, p)
    mend = p + int(mlen or 0)
    cols: list[ColumnDef] = []
    for t in ctypes:
        meta = meta2 = 0
        real = t
        if t in _META_1:
            meta = b[p]
            p += 1
        elif t in _META_2:
            b0, b1 = b[p], b[p + 1]
            p += 2
            if t in (T.VARCHAR, T.VAR_STRING):
                meta = b0 | (b1 << 8)
            elif t == T.BIT:
                meta, meta2 = b1, b0            # bytes, bits % 8
            elif t in (T.NEWDECIMAL, T.DECIMAL):
                meta, meta2 = b0, b1            # precision, scale
            else:                                # STRING / ENUM / SET: real type + length
                if (b0 & 0x30) != 0x30:
                    real = b0 | 0x30
                    meta = b1 | (((b0 & 0x30) ^ 0x30) << 4)
                else:
                    real = b0
                    meta = b1
                if real not in (T.ENUM, T.SET, T.STRING):
                    real = T.STRING
        cols.append(ColumnDef(real, meta, True, meta2))
    if p != mend:
        raise EventError(f"TABLE_MAP metadata length mismatch ({p} != {mend}) for {schema}.{table}")
    p = mend
    nb = (n + 7) // 8
    nullbits = b[p:p + nb]
    p += nb
    for i, c in enumerate(cols):
        c.nullable = bool(nullbits[i // 8] & (1 << (i % 8)))
    tm = TableMap(table_id, schema, table, cols)
    _parse_optional_metadata(b, p, tm)
    return tm


def _parse_optional_metadata(b: memoryview, p: int, tm: TableMap) -> None:
    cols = tm.columns
    while p < len(b):
        ft = b[p]
        ln, p = lenenc(b, p + 1)
        end = p + int(ln or 0)
        if end > len(b):
            raise EventError("TABLE_MAP optional metadata beyond event")
        data = b[p:end]
        if ft == OM_SIGNEDNESS:
            numeric = [c for c in cols if c.type in T.NUMERIC_CODES]
            for i, c in enumerate(numeric):
                c.unsigned = bool(data[i // 8] & (0x80 >> (i % 8)))
        elif ft == OM_COLUMN_NAME:
            q = 0
            for c in cols:
                if q >= len(data):
                    break
                nl, q = lenenc(data, q)
                c.name = bytes(data[q:q + int(nl or 0)]).decode("utf-8", "replace")
                q += int(nl or 0)
        elif ft in (OM_ENUM_STR_VALUE, OM_SET_STR_VALUE):
            want = T.ENUM if ft == OM_ENUM_STR_VALUE else T.SET
            targets = [c for c in cols if c.type == want]
            q = 0
            for c in targets:
                if q >= len(data):
                    break
                cnt, q = lenenc(data, q)
                vals = []
                for _ in range(int(cnt or 0)):
                    sl, q = lenenc(data, q)
                    vals.append(bytes(data[q:q + int(sl or 0)]).decode("utf-8", "replace"))
                    q += int(sl or 0)
                if want == T.ENUM:
                    c.enum_values = vals
                else:
                    c.set_values = vals
        elif ft == OM_SIMPLE_PRIMARY_KEY:
            q = 0
            while q < len(data):
                idx, q = lenenc(data, q)
                tm.pk.append(int(idx or 0))
        elif ft == OM_PRIMARY_KEY_WITH_PREFIX:
            q = 0
            while q < len(data):
                idx, q = lenenc(data, q)
                _, q = lenenc(data, q)
                tm.pk.append(int(idx or 0))
        p = end


# ------------------------------------------------------------------------------------------------ ROWS
@dataclass
class RowsHeader:
    table_id: int
    flags: int
    ncols: int
    present: bytes
    present_after: bytes | None
    data_off: int               # offset of the first row image inside body()
    body: memoryview


def parse_rows_header(ev: bytes, fde: FormatDescription, typ: int) -> RowsHeader:
    b = body(ev, fde)
    default = 10 if typ in ROWS_V2 else 8
    phl = fde.post_len(typ, default)
    if phl == 6:
        table_id = struct.unpack_from("<I", b, 0)[0]
        flags = struct.unpack_from("<H", b, 4)[0]
        p = 6
    else:
        table_id = int.from_bytes(b[0:6], "little")
        flags = struct.unpack_from("<H", b, 6)[0]
        p = 8
    if typ in ROWS_V2 and phl >= 10:
        extra = struct.unpack_from("<H", b, p)[0]
        p += max(extra, 2)
    else:
        p = phl
    n, p = lenenc(b, p)
    assert n is not None
    nb = (n + 7) // 8
    present = bytes(b[p:p + nb])
    p += nb
    after = None
    if typ in UPDATE_TYPES:
        after = bytes(b[p:p + nb])
        p += nb
    if typ in COMPRESSED_ROWS:                  # MariaDB log_bin_compress: the row images are compressed
        b = memoryview(bytes(b[:p]) + maria_uncompress(bytes(b[p:])))
    return RowsHeader(table_id, flags, n, present, after, p, b)


def bit_count(bm: bytes, n: int) -> int:
    return sum(1 for i in range(n) if bm[i // 8] & (1 << (i % 8)))


def bit_set(bm: bytes, i: int) -> bool:
    return bool(bm[i // 8] & (1 << (i % 8)))

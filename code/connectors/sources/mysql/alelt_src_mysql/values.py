"""Binary value formats of binlog row images (stdlib only).

Formats follow the public MySQL binlog / row-based replication documentation (and the MariaDB knowledge base for
MariaDB, which uses the same encodings with mysql56_temporal_format=ON):
- integers little-endian; FLOAT/DOUBLE IEEE little-endian
- NEWDECIMAL: packed 9-digit groups, big-endian, sign in the top bit of the first byte (negative => all bytes inverted)
- DATETIME2 / TIMESTAMP2 / TIME2: big-endian packed integers + 0..3 bytes of fractional seconds
- DATE: 3 bytes little-endian (day 5 bits, month 4 bits, year 15 bits)
"""
from __future__ import annotations

import struct
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

_DIG2BYTES = (0, 1, 1, 2, 2, 3, 3, 4, 4, 4)


class ValueError_(ValueError):
    """Undecodable value (bad length / out-of-range). Message is the quarantine reason."""


def le_uint(b: bytes | memoryview, off: int, n: int) -> int:
    return int.from_bytes(b[off:off + n], "little")


def be_uint(b: bytes | memoryview, off: int, n: int) -> int:
    return int.from_bytes(b[off:off + n], "big")


def decimal_size(precision: int, scale: int) -> int:
    intg = precision - scale
    return (intg // 9) * 4 + _DIG2BYTES[intg % 9] + (scale // 9) * 4 + _DIG2BYTES[scale % 9]


def decode_decimal(buf: bytes | memoryview, off: int, precision: int, scale: int) -> tuple[Decimal, int]:
    size = decimal_size(precision, scale)
    raw = bytearray(buf[off:off + size])
    if len(raw) != size:
        raise ValueError_("truncated DECIMAL")
    negative = (raw[0] & 0x80) == 0
    raw[0] ^= 0x80
    if negative:
        raw = bytearray(x ^ 0xFF for x in raw)
    intg = precision - scale
    intg0, intg0x = divmod(intg, 9)
    frac0, frac0x = divmod(scale, 9)
    pos = 0
    digits_int = ""
    if intg0x:
        n = _DIG2BYTES[intg0x]
        digits_int += str(int.from_bytes(raw[pos:pos + n], "big"))
        pos += n
    for _ in range(intg0):
        digits_int += f"{int.from_bytes(raw[pos:pos + 4], 'big'):09d}"
        pos += 4
    digits_frac = ""
    for _ in range(frac0):
        digits_frac += f"{int.from_bytes(raw[pos:pos + 4], 'big'):09d}"
        pos += 4
    if frac0x:
        n = _DIG2BYTES[frac0x]
        digits_frac += f"{int.from_bytes(raw[pos:pos + n], 'big'):0{frac0x}d}"
        pos += n
    text = (digits_int.lstrip("0") or "0") + ("." + digits_frac if scale else "")
    value = Decimal(("-" if negative else "") + text)
    return value, size


def frac_size(fsp: int) -> int:
    return (fsp + 1) // 2


def _frac_us(buf: bytes | memoryview, off: int, fsp: int) -> int:
    n = frac_size(fsp)
    if n == 0:
        return 0
    v = be_uint(buf, off, n)
    return v * (10000 if n == 1 else 100 if n == 2 else 1)


def decode_datetime2(buf: bytes | memoryview, off: int, fsp: int) -> tuple[datetime | None, int, str | None]:
    """Returns (value, size, note). Zero / partial-zero dates ('0000-00-00 ...') decode to None with a note."""
    size = 5 + frac_size(fsp)
    if len(buf) - off < size:
        raise ValueError_("truncated DATETIME2")
    packed = be_uint(buf, off, 5) - 0x8000000000
    ymd = packed >> 17
    hms = packed & ((1 << 17) - 1)
    ym = ymd >> 5
    day = ymd & 31
    month = ym % 13
    year = ym // 13
    second = hms & 63
    minute = (hms >> 6) & 63
    hour = hms >> 12
    us = _frac_us(buf, off + 5, fsp)
    if year == 0 or month == 0 or day == 0:
        return None, size, "zero date -> NULL"
    try:
        return datetime(year, month, day, hour, minute, second, us), size, None
    except ValueError as e:
        raise ValueError_(f"invalid DATETIME2: {e}") from None


def decode_timestamp2(buf: bytes | memoryview, off: int, fsp: int) -> tuple[datetime | None, int, str | None]:
    size = 4 + frac_size(fsp)
    if len(buf) - off < size:
        raise ValueError_("truncated TIMESTAMP2")
    sec = be_uint(buf, off, 4)
    us = _frac_us(buf, off + 4, fsp)
    if sec == 0 and us == 0:
        return None, size, "zero timestamp -> NULL"
    return datetime.fromtimestamp(sec, tz=timezone.utc) + timedelta(microseconds=us), size, None


def decode_time2(buf: bytes | memoryview, off: int, fsp: int) -> tuple[timedelta, int]:
    """TIME2 as a signed duration (MySQL TIME range is -838:59:59 .. 838:59:59)."""
    n = frac_size(fsp)
    size = 3 + n
    if len(buf) - off < size:
        raise ValueError_("truncated TIME2")
    if n == 3:
        packed = be_uint(buf, off, 6) - 0x800000000000
    else:
        intpart = be_uint(buf, off, 3) - 0x800000
        frac = be_uint(buf, off + 3, n) if n else 0
        if n == 1:
            if intpart < 0 and frac:
                intpart += 1
                frac -= 0x100
            frac *= 10000
        elif n == 2:
            if intpart < 0 and frac:
                intpart += 1
                frac -= 0x10000
            frac *= 100
        packed = (intpart << 24) + frac
    neg = packed < 0
    packed = abs(packed)
    hms = packed >> 24
    us = packed & 0xFFFFFF
    hour = (hms >> 12) & 0x3FF
    minute = (hms >> 6) & 0x3F
    second = hms & 0x3F
    td = timedelta(hours=hour, minutes=minute, seconds=second, microseconds=us)
    return (-td if neg else td), size


def time_of_day(td: timedelta) -> time | None:
    """Canonical LOCALTIME for a TIME duration; None if outside 00:00:00..23:59:59.999999."""
    if td < timedelta(0) or td >= timedelta(days=1):
        return None
    total_us = td.days * 86_400_000_000 + td.seconds * 1_000_000 + td.microseconds
    s, us = divmod(total_us, 1_000_000)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return time(h, m, s, us)


def decode_date(buf: bytes | memoryview, off: int) -> tuple[date | None, int, str | None]:
    v = le_uint(buf, off, 3)
    day, month, year = v & 31, (v >> 5) & 15, v >> 9
    if year == 0 or month == 0 or day == 0:
        return None, 3, "zero date -> NULL"
    try:
        return date(year, month, day), 3, None
    except ValueError as e:
        raise ValueError_(f"invalid DATE: {e}") from None


def decode_datetime_old(buf: bytes | memoryview, off: int) -> tuple[datetime | None, int, str | None]:
    """Pre-5.6 DATETIME: 8-byte little-endian YYYYMMDDhhmmss."""
    v = le_uint(buf, off, 8)
    d, t = divmod(v, 1_000_000)
    year, md = divmod(d, 10000)
    month, day = divmod(md, 100)
    hour, ms = divmod(t, 10000)
    minute, second = divmod(ms, 100)
    if year == 0 or month == 0 or day == 0:
        return None, 8, "zero date -> NULL"
    return datetime(year, month, day, hour, minute, second), 8, None


def decode_time_old(buf: bytes | memoryview, off: int) -> tuple[timedelta, int]:
    """Pre-5.6 TIME: 3-byte little-endian signed hhmmss."""
    v = le_uint(buf, off, 3)
    if v & 0x800000:
        v -= 0x1000000
    neg = v < 0
    v = abs(v)
    h, ms = divmod(v, 10000)
    m, s = divmod(ms, 100)
    td = timedelta(hours=h, minutes=m, seconds=s)
    return (-td if neg else td), 3


def decode_float(buf: bytes | memoryview, off: int) -> float:
    return float(struct.unpack_from("<f", buf, off)[0])


def decode_double(buf: bytes | memoryview, off: int) -> float:
    return float(struct.unpack_from("<d", buf, off)[0])


def signed(v: int, nbytes: int) -> int:
    bit = 1 << (8 * nbytes - 1)
    return v - (bit << 1) if v & bit else v

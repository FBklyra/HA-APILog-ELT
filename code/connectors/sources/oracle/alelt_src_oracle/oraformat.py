"""Oracle datetime format models -> Python values (stdlib only).

Used for the literals LogMiner writes into SQL_REDO / SQL_UNDO (``TO_DATE('..', 'fmt')``, ``TO_TIMESTAMP``,
``TO_TIMESTAMP_TZ``). The mining session sets explicit NLS formats (``logminer.SESSION_NLS``) so the
formats seen in practice are the ISO-like ones below, but any common format model element is accepted.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
MONTH_NAMES = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER",
               "OCTOBER", "NOVEMBER", "DECEMBER"]

# longest first so 'HH24' wins over 'HH', 'MONTH' over 'MON' over 'MM'
_ELEMENTS = ["SYYYY", "YYYY", "RRRR", "HH24", "HH12", "MONTH", "TZH", "TZM", "TZR", "TZD", "A.M.", "P.M.",
             "YYY", "MON", "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "FF7", "FF8", "FF9", "FF",
             "RR", "YY", "MM", "DD", "HH", "MI", "SS", "AM", "PM", "X", "Y"]


class FormatError(ValueError):
    pass


def _tokens(fmt: str) -> list[tuple[str, str]]:
    """[('el', 'YYYY') | ('lit', '-')] ; FM/FX modifiers ignored."""
    out: list[tuple[str, str]] = []
    i, up = 0, fmt.upper()
    while i < len(fmt):
        if up.startswith(("FM", "FX"), i):
            i += 2
            continue
        if fmt[i] == '"':
            j = fmt.find('"', i + 1)
            if j < 0:
                raise FormatError(f"unterminated quoted text in format {fmt!r}")
            out.append(("lit", fmt[i + 1:j]))
            i = j + 1
            continue
        for el in _ELEMENTS:
            if up.startswith(el, i):
                out.append(("el", el))
                i += len(el)
                break
        else:
            if fmt[i].isalnum():
                raise FormatError(f"unsupported format element at {fmt[i:]!r} in {fmt!r}")
            out.append(("lit", fmt[i]))
            i += 1
    return out


def _tz_from_name(name: str) -> tzinfo:
    n = name.strip()
    m = re.fullmatch(r"([+-])(\d{1,2}):?(\d{2})", n)
    if m:
        off = timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
        return timezone(-off if m.group(1) == "-" else off)
    if n.upper() in ("UTC", "GMT", "Z", "UCT"):
        return timezone.utc
    try:
        return ZoneInfo(n)
    except Exception as e:  # noqa: BLE001
        raise FormatError(f"unknown time zone region {name!r}") from e


def parse(value: str, fmt: str, *, with_tz: bool = False) -> datetime:
    """Parse ``value`` with Oracle format model ``fmt``. Naive result unless the format/value carries a zone."""
    toks = _tokens(fmt)
    s, i = value, 0
    year = month = day = None
    hour = minute = second = 0
    micro = 0
    pm: bool | None = None
    tzh: int | None = None
    tzneg = False
    tzm = 0
    tz: tzinfo | None = None

    def num(maxlen: int, signed: bool = False) -> int:
        nonlocal i
        j = i
        if signed and j < len(s) and s[j] in "+-":
            j += 1
        while j < len(s) and s[j].isdigit() and j - i < maxlen + (1 if signed else 0):
            j += 1
        if j == i or (signed and j == i + 1 and s[i] in "+-"):
            raise FormatError(f"expected digits at {s[i:]!r} (value {value!r}, format {fmt!r})")
        v = int(s[i:j])
        i = j
        return v

    for kind, el in toks:
        if kind == "lit":
            # separators are matched loosely (Oracle accepts any punctuation in place of another)
            if i < len(s) and (s[i] == el or (not s[i].isalnum() and not el.isalnum())):
                i += 1
            elif el.strip() == "" and (i >= len(s) or s[i] != " "):
                pass
            else:
                if s[i:i + len(el)].upper() == el.upper():
                    i += len(el)
                else:
                    raise FormatError(f"expected {el!r} at {s[i:]!r} (format {fmt!r})")
            continue
        if el in ("YYYY", "RRRR", "SYYYY"):
            year = num(4, signed=el == "SYYYY")
        elif el == "YYY":
            year = 2000 + num(3)
        elif el in ("RR", "YY"):
            y = num(2)
            year = (2000 + y if y < 50 else 1900 + y) if el == "RR" else 2000 + y
        elif el == "Y":
            year = 2000 + num(1)
        elif el == "MM":
            month = num(2)
        elif el == "MON":
            m3 = s[i:i + 3].upper()
            if m3 not in MONTHS:
                raise FormatError(f"bad month {s[i:]!r}")
            month = MONTHS.index(m3) + 1
            i += 3
        elif el == "MONTH":
            for k, nm in enumerate(MONTH_NAMES):
                if s[i:i + len(nm)].upper() == nm:
                    month, i = k + 1, i + len(nm)
                    break
            else:
                raise FormatError(f"bad month name {s[i:]!r}")
            while i < len(s) and s[i] == " ":
                i += 1
        elif el == "DD":
            day = num(2)
        elif el in ("HH24",):
            hour = num(2)
        elif el in ("HH", "HH12"):
            hour = num(2)
        elif el == "MI":
            minute = num(2)
        elif el == "SS":
            second = num(2)
        elif el.startswith("FF"):
            j = i
            while j < len(s) and s[j].isdigit():
                j += 1
            frac = s[i:j]
            i = j
            micro = int((frac + "000000")[:6]) if frac else 0   # Oracle keeps up to ns; Python keeps us
        elif el in ("AM", "PM", "A.M.", "P.M."):
            w = s[i:i + len(el)].upper()
            if w.replace(".", "") not in ("AM", "PM"):
                raise FormatError(f"expected AM/PM at {s[i:]!r}")
            pm = w.startswith("P")
            i += len(el)
        elif el == "X":
            if i < len(s) and s[i] in ".,":
                i += 1
        elif el == "TZH":
            tzneg = i < len(s) and s[i] == "-"
            tzh = abs(num(2, signed=True))
        elif el == "TZM":
            tzm = num(2)
        elif el in ("TZR", "TZD"):
            j = i
            while j < len(s) and not s[j].isspace():
                j += 1
            name = s[i:j]
            i = j
            if el == "TZR":
                tz = _tz_from_name(name)
    if i < len(s) and s[i:].strip():
        raise FormatError(f"trailing text {s[i:]!r} (value {value!r}, format {fmt!r})")
    if year is None or month is None or day is None:
        raise FormatError(f"format {fmt!r} lacks year/month/day")
    if pm is not None:
        hour = hour % 12 + (12 if pm else 0)
    dt = datetime(year, month, day, hour, minute, second, micro)
    if tzh is not None and tz is None:
        off = timedelta(hours=tzh, minutes=tzm)
        tz = timezone(-off if tzneg else off)
    if tz is not None:
        dt = dt.replace(tzinfo=tz)
    elif with_tz:
        raise FormatError(f"no time zone in {value!r}")
    return dt


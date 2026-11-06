"""Binlog positions for MySQL and MariaDB (stdlib only).

Text form (canonical, what the core stores):

    <gtid-state>@<file>:<pos>     e.g. 'a1b2...:1-57@binlog.000003:1234'   (MySQL GTID set)
                                       '0-1-57,1-2-9@mariadb-bin.000002:880' (MariaDB GTID list)
    @<file>:<pos>                 no GTIDs (MySQL gtid_mode=OFF)
    <gtid-state>                  resume-only form (typed by an operator, or a snapshot without coordinates)

<file>:<pos> is the end offset of the event (= where the next event starts). The GTID state of an event inside a
transaction already includes that transaction's GTID, so every event of a transaction sorts after the previous commit
and before the next transaction; inside one transaction the file offset orders the events.

Ordering (comparator per engine):
- both have a GTID state: containment order (a subset of b => a < b). Equal states fall back to file:pos.
  Incomparable states (different histories) fall back to the number of transactions, then file:pos, then text.
- otherwise: (binlog file sequence number, offset).
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field

MYSQL = "mysql"
MARIADB = "mariadb"
FLAVORS = (MYSQL, MARIADB)

_UUID = r"[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}"
_COORD = re.compile(r"^(?P<file>[^:@\s]+):(?P<pos>\d+)$")


# ----------------------------------------------------------------------------- MySQL GTID set
@dataclass
class GtidSet:
    """MySQL GTID set: {source uuid: [(start, end_inclusive), ...]} with merged, sorted intervals.
    Tagged GTIDs (uuid:tag:n, MySQL 8.3+) are kept as key 'uuid:tag'."""

    sids: dict[str, list[tuple[int, int]]] = field(default_factory=dict)

    @classmethod
    def parse(cls, text: str) -> GtidSet:
        out = cls()
        t = re.sub(r"\s+", "", text or "")
        if not t:
            return out
        for part in t.split(","):
            if not part:
                continue
            pieces = part.split(":")
            if len(pieces) < 2 or not re.fullmatch(_UUID, pieces[0]):
                raise ValueError(f"invalid MySQL GTID set element {part!r}")
            sid = str(uuid.UUID(pieces[0]))
            tag = ""
            for p in pieces[1:]:
                if re.fullmatch(r"\d+(-\d+)?", p):
                    a, _, b = p.partition("-")
                    lo, hi = int(a), int(b or a)
                    if lo < 1 or hi < lo:
                        raise ValueError(f"invalid GTID interval {p!r}")
                    out.add_interval(sid + (":" + tag if tag else ""), lo, hi)
                elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,31}", p):
                    tag = p.lower()
                else:
                    raise ValueError(f"invalid MySQL GTID set element {part!r}")
        return out

    def copy(self) -> GtidSet:
        return GtidSet({k: list(v) for k, v in self.sids.items()})

    def add_interval(self, sid: str, lo: int, hi: int) -> None:
        iv = self.sids.setdefault(sid, [])
        iv.append((lo, hi))
        iv.sort()
        merged: list[tuple[int, int]] = []
        for a, b in iv:
            if merged and a <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], b))
            else:
                merged.append((a, b))
        self.sids[sid] = merged

    def add(self, sid: str, gno: int) -> None:
        self.add_interval(sid, gno, gno)

    def contains(self, sid: str, gno: int) -> bool:
        return any(a <= gno <= b for a, b in self.sids.get(sid, ()))

    def issubset(self, other: GtidSet) -> bool:
        for sid, ivs in self.sids.items():
            oiv = other.sids.get(sid, [])
            for a, b in ivs:
                if not any(x <= a and b <= y for x, y in oiv):
                    return False
        return True

    def count(self) -> int:
        return sum(b - a + 1 for ivs in self.sids.values() for a, b in ivs)

    def __bool__(self) -> bool:
        return bool(self.sids)

    @property
    def text(self) -> str:
        parts = []
        by_uuid: dict[str, list[tuple[str, list[tuple[int, int]]]]] = {}
        for k in sorted(self.sids):
            u, _, tag = k.partition(":")
            by_uuid.setdefault(u, []).append((tag, self.sids[k]))
        for u in sorted(by_uuid):
            seg = [u]
            for tag, ivs in sorted(by_uuid[u]):
                if tag:
                    seg.append(tag)
                seg.extend(f"{a}" if a == b else f"{a}-{b}" for a, b in ivs)
            parts.append(":".join(seg))
        return ",".join(parts)

    def encode(self) -> bytes:
        """COM_BINLOG_DUMP_GTID data (same layout as the PREVIOUS_GTIDS_LOG_EVENT body):
        untagged: n_sids u64, then per sid: uuid(16) n_intervals u64 (start u64, end_excl u64)*;
        tagged (MySQL 8.3+, any 'uuid:tag' key): byte 0 = 1 (format), bytes 1-6 = n_sids, byte 7 = 1, and every sid
        is uuid(16) + tag (serialized length + ASCII, empty for untagged) before its intervals (verified against the
        PREVIOUS_GTIDS_LOG_EVENT written by MySQL 9.7)."""
        out = bytearray()
        keys = sorted(self.sids, key=lambda k: (k.partition(":")[0], k.partition(":")[2]))
        tagged = any(":" in k for k in keys)
        if tagged:
            out += b"\x01" + len(keys).to_bytes(6, "little") + b"\x01"
        else:
            out += len(keys).to_bytes(8, "little")
        for k in keys:
            u, _, tag = k.partition(":")
            out += uuid.UUID(u).bytes
            if tagged:
                t = tag.encode("ascii")
                if len(t) > 32:
                    raise ValueError(f"GTID tag too long: {tag!r}")
                out += bytes([len(t) << 1]) + t
            out += len(self.sids[k]).to_bytes(8, "little")
            for a, b in self.sids[k]:
                out += a.to_bytes(8, "little") + (b + 1).to_bytes(8, "little")
        return bytes(out)

    @classmethod
    def decode(cls, data: bytes) -> GtidSet:
        """Inverse of encode (PREVIOUS_GTIDS_LOG_EVENT body uses the same layout, tagged or not)."""
        out = cls()
        tagged = data[0] == 1 and data[7] == 1
        n = int.from_bytes(data[1:7], "little") if tagged else int.from_bytes(data[0:8], "little")
        off = 8
        for _ in range(n):
            sid = str(uuid.UUID(bytes=bytes(data[off:off + 16])))
            off += 16
            if tagged:
                ln = data[off] >> 1
                tag = bytes(data[off + 1:off + 1 + ln]).decode("ascii").lower()
                off += 1 + ln
                if tag:
                    sid = f"{sid}:{tag}"
            k = int.from_bytes(data[off:off + 8], "little")
            off += 8
            for _ in range(k):
                a = int.from_bytes(data[off:off + 8], "little")
                b = int.from_bytes(data[off + 8:off + 16], "little")
                off += 16
                if b > a:
                    out.add_interval(sid, a, b - 1)
        return out


# ----------------------------------------------------------------------------- MariaDB GTID list
@dataclass
class MariaGtidList:
    """MariaDB GTID state: last GTID per replication domain {domain: (server_id, seq_no)}."""

    domains: dict[int, tuple[int, int]] = field(default_factory=dict)

    @classmethod
    def parse(cls, text: str) -> MariaGtidList:
        out = cls()
        t = re.sub(r"\s+", "", text or "")
        for part in filter(None, t.split(",")):
            m = re.fullmatch(r"(\d+)-(\d+)-(\d+)", part)
            if not m:
                raise ValueError(f"invalid MariaDB GTID {part!r}; expected domain-server-seq")
            d, s, n = (int(x) for x in m.groups())
            out.set(d, s, n)
        return out

    def copy(self) -> MariaGtidList:
        return MariaGtidList(dict(self.domains))

    def set(self, domain: int, server: int, seq: int) -> None:
        cur = self.domains.get(domain)
        if cur is None or seq >= cur[1]:
            self.domains[domain] = (server, seq)

    def issubset(self, other: MariaGtidList) -> bool:
        return all(d in other.domains and other.domains[d][1] >= s for d, (_, s) in self.domains.items())

    def count(self) -> int:
        return sum(s for _, s in self.domains.values())

    def __bool__(self) -> bool:
        return bool(self.domains)

    @property
    def text(self) -> str:
        return ",".join(f"{d}-{s}-{n}" for d, (s, n) in sorted(self.domains.items()))


# ----------------------------------------------------------------------------- binlog coordinates
def file_seq(name: str) -> tuple[str, int]:
    """'binlog.000012' -> ('binlog', 12). Files without a numeric suffix sort by name only."""
    base, dot, num = name.rpartition(".")
    if dot and num.isdigit():
        return base, int(num)
    return name, -1


@dataclass(frozen=True)
class BinlogPosition:
    flavor: str
    gtid: GtidSet | MariaGtidList | None
    file: str | None = None
    pos: int | None = None

    @property
    def text(self) -> str:
        g = self.gtid.text if self.gtid is not None else ""
        if self.file is None:
            return g
        return f"{g}@{self.file}:{self.pos}"

    def __str__(self) -> str:
        return self.text

    @property
    def coord(self) -> tuple[str, int] | None:
        return (self.file, int(self.pos or 0)) if self.file is not None else None

    @classmethod
    def parse(cls, text: str, flavor: str) -> BinlogPosition:
        if flavor not in FLAVORS:
            raise ValueError(f"unknown flavor {flavor!r}")
        t = (text or "").strip()
        g_text, at, coord = t.rpartition("@") if "@" in t else (t, "", "")
        file = pos = None
        if at:
            m = _COORD.match(coord)
            if not m:
                raise ValueError(f"invalid binlog coordinate {coord!r}; expected <file>:<pos>")
            file, pos = m.group("file"), int(m.group("pos"))
        elif _COORD.match(t) and not re.match(_UUID, t):
            # bare 'binlog.000003:1234'
            m = _COORD.match(t)
            assert m
            return cls(flavor, None, m.group("file"), int(m.group("pos")))
        gt: GtidSet | MariaGtidList | None
        if not g_text:
            gt = None
        elif flavor == MYSQL:
            gt = GtidSet.parse(g_text)
        else:
            gt = MariaGtidList.parse(g_text)
        if gt is None and file is None:
            raise ValueError("empty binlog position")
        return cls(flavor, gt, file, pos)


def _coord_key(p: BinlogPosition) -> tuple[str, int, int]:
    base, n = file_seq(p.file or "")
    return base, n, int(p.pos or 0)


def compare_positions(a: BinlogPosition, b: BinlogPosition) -> int:
    if a.gtid is not None and b.gtid is not None and (a.gtid or b.gtid):
        sa, sb = a.gtid.issubset(b.gtid), b.gtid.issubset(a.gtid)  # type: ignore[arg-type]
        if sa and not sb:
            return -1
        if sb and not sa:
            return 1
        if not sa and not sb:
            ca, cb = a.gtid.count(), b.gtid.count()
            if ca != cb:
                return -1 if ca < cb else 1
    if a.file is not None and b.file is not None:
        ka, kb = _coord_key(a), _coord_key(b)
        if ka != kb:
            return -1 if ka < kb else 1
        return 0
    if a.file is None and b.file is not None and a.gtid is not None and b.gtid is not None:
        return 1          # resume-only form = end of that GTID state
    if b.file is None and a.file is not None and a.gtid is not None and b.gtid is not None:
        return -1
    return (a.text > b.text) - (a.text < b.text)


def compare_text(a: str, b: str, flavor: str) -> int:
    return compare_positions(BinlogPosition.parse(a, flavor), BinlogPosition.parse(b, flavor))

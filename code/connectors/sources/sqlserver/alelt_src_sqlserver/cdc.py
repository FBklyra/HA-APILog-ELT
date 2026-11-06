"""SQL Server CDC reader: documented change-data-capture functions and tables only.

| Need | Documented interface |
|---|---|
| end of the captured log | ``sys.fn_cdc_get_max_lsn()`` |
| oldest change still retained per capture instance | ``sys.fn_cdc_get_min_lsn(N'<capture_instance>')`` |
| transaction boundaries / commit time / tran_id | ``cdc.lsn_time_mapping`` (``start_lsn`` = commit LSN) |
| changes | ``cdc.fn_cdc_get_all_changes_<capture_instance>(@from, @to, N'all update old')`` |
| LSN of a time | ``sys.fn_cdc_map_lsn_to_time`` |
| dev / test only: run one capture scan when SQL Server Agent is not running | ``sys.sp_cdc_scan`` |

A window ``(after, to]`` always ends on a commit LSN (``__$start_lsn`` of a transaction), so it never cuts a
transaction. Retention: when the resume LSN is older than ``fn_cdc_get_min_lsn`` of an instance that already existed
at that time, changes were purged by the cleanup job -> :class:`RetentionGapError` (never skip silently).
"""
from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any

from .assemble import CaptureInstance, TxnMeta
from .position import ZERO, increment, lsn_text

log = logging.getLogger("alelt_src_sqlserver")
_CI = re.compile(r"^[A-Za-z0-9_]{1,100}$")      # capture instance names are sysname built from schema_table


class CdcError(RuntimeError):
    pass


class RetentionGapError(CdcError):
    """The resume position is older than the oldest retained change: a resync of the affected tables is needed."""


def change_fn(ci: str) -> str:
    if not _CI.match(ci):
        raise CdcError(f"unexpected capture instance name {ci!r}")
    return f"cdc.[fn_cdc_get_all_changes_{ci}]"


def lit(b: bytes) -> str:
    """binary(10) LSN as a T-SQL literal (LSNs are bytes ALELT read from the server, never user text)."""
    if len(b) != 10:
        raise CdcError(f"LSN must be 10 bytes, got {len(b)}")
    return "0x" + bytes(b).hex().upper()


def _b(v: Any) -> bytes | None:
    if v is None:
        return None
    b = bytes(v)
    return None if not any(b) else b


class CdcReader:
    def __init__(self, conn: Any, instances: dict[str, CaptureInstance], dev_manual_scan: bool = False,
                 window_txns: int = 5000, server_tz: tzinfo | None = None) -> None:
        self.conn = conn
        self.instances = instances
        self.dev_manual_scan = dev_manual_scan
        self.window_txns = max(1, int(window_txns))
        self._tz = server_tz
        # SRC-ms-02: fn_cdc_get_min_lsn per instance is cached for min_lsn_ttl_s (it only moves when the cleanup job
        # runs); a change-function error drops the cache so the next window re-reads it
        self.min_lsn_ttl_s = 30.0
        self._min_cache: dict[str, tuple[float, bytes | None]] = {}

    # ------------------------------------------------------------------ helpers
    def _one(self, sql: str, args: tuple[Any, ...] = ()) -> Any:
        cur = self.conn.cursor()
        if args:
            cur.execute(sql, args)
        else:
            cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r else None

    @property
    def server_tz(self) -> tzinfo:
        """lsn_time_mapping times are server local time: current offset of the server (DST change: set server_tz)."""
        if self._tz is None:
            off = self._one("SELECT DATEPART(TZOFFSET, SYSDATETIMEOFFSET())")
            self._tz = timezone(timedelta(minutes=int(off or 0)))
        return self._tz

    def scan(self) -> None:
        """DEV / TEST ONLY (setting dev_manual_scan): one capture scan, for servers without SQL Server Agent."""
        cur = self.conn.cursor()
        cur.execute("EXEC sys.sp_cdc_scan @maxtrans = 500, @maxscans = 1, @continuous = 0")
        try:
            while cur.nextset():
                pass
        except Exception:  # noqa: BLE001 - no further result sets
            pass

    def max_lsn(self) -> bytes:
        return bytes(self._one("SELECT sys.fn_cdc_get_max_lsn()") or ZERO)

    def min_lsn(self, ci: str, fresh: bool = False) -> bytes | None:
        import time
        now = time.monotonic()
        hit = self._min_cache.get(ci)
        if hit is not None and not fresh and now - hit[0] < self.min_lsn_ttl_s:
            return hit[1]
        v = _b(self._one("SELECT sys.fn_cdc_get_min_lsn(%s)", (ci,)))
        self._min_cache[ci] = (now, v)
        return v

    def lsn_time(self, lsn: bytes) -> datetime | None:
        if not any(lsn):
            return None
        v = self._one(f"SELECT sys.fn_cdc_map_lsn_to_time({lit(lsn)})")
        return v if isinstance(v, datetime) else None

    # ------------------------------------------------------------------ retention
    def check_retention(self, after: bytes) -> None:
        """Raise RetentionGapError when changes after ``after`` were already purged for a known instance."""
        if not any(after):
            return
        at = self.lsn_time(after)
        gaps = []
        for name, ci in self.instances.items():
            mn = self.min_lsn(name, fresh=True)     # retention decision: never from the cache
            if mn is None or increment(after) >= mn:
                continue
            created = ci.create_date
            if created is not None and at is not None and created > at:
                continue          # instance created after the resume point: nothing was missed
            gaps.append(f"{name} (min_lsn {lsn_text(mn)} > resume {lsn_text(after)})")
        if gaps:
            raise RetentionGapError("CDC retention gap, changes purged by the cleanup job: " + "; ".join(gaps)
                                    + " - resync these tables (snapshot) and raise the cleanup retention")

    # ------------------------------------------------------------------ windows
    def window_end(self, after: bytes, end: bytes) -> bytes | None:
        """Last commit LSN of at most window_txns transactions in (after, end]; None when nothing committed."""
        if end <= after:
            return None
        cur = self.conn.cursor()
        cur.execute(f"SELECT MAX(start_lsn) FROM (SELECT TOP ({self.window_txns}) start_lsn FROM cdc.lsn_time_mapping "
                    f"WHERE start_lsn > {lit(after)} AND start_lsn <= {lit(end)} ORDER BY start_lsn) w")
        r = cur.fetchone()
        return bytes(r[0]) if r and r[0] is not None else None

    def txn_meta(self, after: bytes, to: bytes) -> dict[bytes, TxnMeta]:
        cur = self.conn.cursor()
        cur.execute("SELECT start_lsn, tran_begin_time, tran_end_time, tran_id FROM cdc.lsn_time_mapping "
                    f"WHERE start_lsn > {lit(after)} AND start_lsn <= {lit(to)}")
        return {bytes(a): TxnMeta(tran_id=bytes(d) if d is not None else None, begin=b, end=c)
                for a, b, c, d in cur.fetchall()}

    probe_min_instances = 4          # SRC-ms-02: with more instances, one probe query finds the ones with changes
    fetch_rows = 5000

    def active(self, ranges: dict[str, bytes], to: bytes) -> list[str]:
        """Instances with at least one change in (from, to]: ONE query (UNION ALL of EXISTS probes) instead of one
        change-function call per instance - most capture instances are quiet in a given window."""
        if len(ranges) <= self.probe_min_instances:
            return sorted(ranges)
        parts = [f"SELECT N'{name}' AS ci WHERE EXISTS (SELECT 1 FROM {change_fn(name)}({lit(frm)}, {lit(to)}, N'all'))"
                 for name, frm in sorted(ranges.items())]
        cur = self.conn.cursor()
        try:
            cur.execute(" UNION ALL ".join(parts))
        except Exception:
            self._min_cache.clear()
            raise
        return sorted(str(r[0]) for r in cur.fetchall())

    def rows(self, after: bytes, to: bytes) -> list[dict[str, Any]]:
        """All change rows of every active capture instance with ``after < __$start_lsn <= to``."""
        out: list[dict[str, Any]] = []
        lo = increment(after)
        ranges: dict[str, bytes] = {}
        for name in sorted(self.instances):
            mn = self.min_lsn(name)
            if mn is None:
                continue                      # instance not valid (dropped meanwhile): catalog refresh follows
            frm = max(lo, mn)
            if frm <= to:
                ranges[name] = frm
        for name in self.active(ranges, to):
            cur = self.conn.cursor()
            try:
                cur.execute(f"SELECT * FROM {change_fn(name)}({lit(ranges[name])}, {lit(to)}, N'all update old')")
            except Exception:
                self._min_cache.clear()
                raise
            cols = [d[0] for d in cur.description]
            while True:                       # bounded fetches (the window itself is bounded by window_txns)
                chunk = cur.fetchmany(self.fetch_rows)
                if not chunk:
                    break
                for r in chunk:
                    d = dict(zip(cols, r))
                    d["ci"] = name
                    out.append(d)
        return out

    def read(self, after: bytes) -> tuple[bytes | None, list[dict[str, Any]], dict[bytes, TxnMeta]]:
        """One window after ``after``: (window end commit LSN or None, rows, txn meta)."""
        if self.dev_manual_scan:
            self.scan()
        end = self.window_end(after, self.max_lsn())
        if end is None:
            return None, [], {}
        return end, self.rows(after, end), self.txn_meta(after, end)


def golden_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Copy rows for a golden file (bytes as hex) - used by the corpus recorder."""
    return [dict(r) for r in rows]

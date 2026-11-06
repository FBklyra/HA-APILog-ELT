"""LogMiner session loop (documented DBMS_LOGMNR API + V$ views), python-oracledb thin mode.

One mining *window* = ``[start_scn, end_scn]``:

1. pick redo logs covering the window: archived logs (``V$ARCHIVED_LOG``) for sequences already archived, online
   logs (``V$LOG`` / ``V$LOGFILE``) for the rest; ``DBMS_LOGMNR.ADD_LOGFILE`` each (skipped inside a PDB, where
   Oracle 21c+ selects the logs itself);
2. ``DBMS_LOGMNR.START_LOGMNR(STARTSCN, ENDSCN, OPTIONS => DICT_FROM_ONLINE_CATALOG)`` - **uncommitted** rows;
3. read ``V$LOGMNR_CONTENTS`` for the captured schemas + every START/COMMIT/ROLLBACK/MISSING_SCN;
4. the session is kept (SRC-ora-02): the next window calls ``START_LOGMNR`` again in the same session (allowed by
   the DBMS_LOGMNR documentation: a session can be started several times with a new SCN range) after adding only
   the logs not registered yet (``ADDFILE``) and removing logs entirely behind the window (``REMOVE_LOGFILE``). The
   session is reset (``END_LOGMNR`` + ``NEW``) only when a registered file no longer holds the registered sequence
   (online log archived / reused) or after an error. Source impact per window = one START_LOGMNR + one query.

The window end never passes the *safe SCN*: an SCN observed at least ``flush_lag_s`` seconds ago (LGWR writes the
log buffer at least every 3 s and on every commit, so all redo up to that SCN is on disk). Without this guard an
uncommitted change still in the log buffer could fall behind the next window start.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Iterator
from datetime import timedelta, timezone, tzinfo
from typing import Any

log = logging.getLogger("alelt_src_oracle")

# Session NLS so LogMiner prints unambiguous literals (it formats SQL_REDO with the session settings).
SESSION_NLS = [
    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'",
    "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'",
    "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'",
    "ALTER SESSION SET TIME_ZONE = 'UTC'",
]

CONTENTS_COLS = ("SCN", "XID", "OPERATION_CODE", "OPERATION", "SEG_OWNER", "TABLE_NAME", "ROW_ID", "CSF",
                 "ROLLBACK", "RS_ID", "SSN", "SQL_REDO", "SQL_UNDO", "TIMESTAMP", "INFO")


def contents_sql(n_schemas: int, ordered: bool = False) -> str:
    """`ordered` (several redo threads = RAC, SRC-ora-07): ORDER BY SCN, then the redo record address (RS_ID, SSN)
    so the merge of the threads is SCN-ascending by contract, not by observation."""
    binds = ", ".join(f":o{i}" for i in range(n_schemas)) or "NULL"
    return ("SELECT SCN, RAWTOHEX(XID) AS XID, OPERATION_CODE, OPERATION, SEG_OWNER, TABLE_NAME, ROW_ID, CSF, "
            "ROLLBACK, RS_ID, SSN, SQL_REDO, SQL_UNDO, TIMESTAMP, INFO FROM V$LOGMNR_CONTENTS "
            f"WHERE SCN >= :s AND SCN <= :e "
            f"AND (SEG_OWNER IN ({binds}) OR SEG_OWNER = 'UNKNOWN' OR OPERATION_CODE IN (6, 7, 34, 36))"
            + (" ORDER BY SCN, RS_ID, SSN" if ordered else ""))


class ScnOrderError(RuntimeError):
    """V$LOGMNR_CONTENTS delivered a lower SCN after a higher one: commit tokens would be emitted out of order
    (S3). The window is not consumed further; mining switches to the ordered query (SRC-ora-07)."""


class LogNotAvailable(RuntimeError):
    """The redo for the requested SCN is no longer on disk (archived log deleted) - re-snapshot needed."""


class Miner:
    def __init__(self, conn: Any, schemas: list[str], flush_lag_s: float = 5.0, window_scns: int = 50_000,
                 arraysize: int = 2000) -> None:
        self.conn = conn
        self.schemas = [s.upper() for s in schemas]
        self.flush_lag_s = flush_lag_s
        self.window_scns = window_scns
        self.arraysize = arraysize
        self._samples: deque[tuple[float, int]] = deque(maxlen=64)
        self._registered: dict[str, tuple[int, int, int]] = {}   # file name -> (thread, sequence, next_change)
        self._active = False                                      # a LogMiner session exists in this connection
        self.stats = {"windows": 0, "sessions": 0, "logs_added": 0, "logs_removed": 0}
        self.in_pdb = self._in_pdb()
        self.ordered = self._threads() > 1
        self.db_tz = self._db_tz()
        cur = conn.cursor()
        for s in SESSION_NLS:
            cur.execute(s)

    # ---- facts
    def _one(self, sql: str, **binds: Any) -> Any:
        cur = self.conn.cursor()
        cur.execute(sql, binds)
        r = cur.fetchone()
        return r[0] if r else None

    def _in_pdb(self) -> bool:
        try:
            name = self._one("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL")
        except Exception:  # noqa: BLE001 - pre-12c
            return False
        return bool(name) and str(name).upper() != "CDB$ROOT" and self._cdb()

    def _threads(self) -> int:
        try:
            return int(self._one("SELECT COUNT(DISTINCT THREAD#) FROM V$LOG") or 1)
        except Exception:  # noqa: BLE001
            return 1

    def _cdb(self) -> bool:
        try:
            return str(self._one("SELECT CDB FROM V$DATABASE")).upper() == "YES"
        except Exception:  # noqa: BLE001
            return False

    def _db_tz(self) -> tzinfo:
        """Offset of the server clock (V$LOGMNR_CONTENTS.TIMESTAMP is server-local). ASSUMPTION: fixed offset."""
        off = self._one("SELECT TO_CHAR(SYSTIMESTAMP, 'TZH:TZM') FROM DUAL")
        try:
            sign = -1 if str(off).strip().startswith("-") else 1
            h, m = str(off).strip().lstrip("+-").split(":")
            return timezone(sign * timedelta(hours=int(h), minutes=int(m)))
        except Exception:  # noqa: BLE001
            return timezone.utc

    def current_scn(self) -> int:
        return int(self._one("SELECT CURRENT_SCN FROM V$DATABASE"))

    def oldest_open_txn_scn(self) -> int | None:
        """START_SCN of the oldest active transaction (V$TRANSACTION), used for a safe snapshot start position."""
        v = self._one("SELECT MIN(START_SCN) FROM V$TRANSACTION")
        return int(v) if v is not None else None

    def safe_scn(self) -> int:
        now = time.monotonic()
        self._samples.append((now, self.current_scn()))
        safe = None
        for t, s in self._samples:
            if now - t >= self.flush_lag_s:
                safe = s
        return safe if safe is not None else -1

    # ---- logs
    def logs_for(self, start_scn: int, end_scn: int) -> list[str]:
        return [e[0] for e in self.log_entries(start_scn, end_scn)]

    def log_entries(self, start_scn: int, end_scn: int) -> list[tuple[str, int, int, int]]:
        """(file name, thread, sequence, next_change) of every log covering the window, thread/sequence order."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#, NAME FROM V$ARCHIVED_LOG "
            "WHERE NEXT_CHANGE# > :s AND FIRST_CHANGE# <= :e AND NAME IS NOT NULL AND STATUS = 'A' "
            "AND STANDBY_DEST = 'NO' AND DELETED = 'NO' ORDER BY THREAD#, SEQUENCE#, DEST_ID",
            {"s": start_scn, "e": end_scn})
        chosen: dict[tuple[int, int], tuple[int, int, str]] = {}
        for th, seq, first, nxt, name in cur:
            chosen.setdefault((int(th), int(seq)), (int(first), int(nxt), name))
        cur.execute(
            "SELECT L.THREAD#, L.SEQUENCE#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, MIN(F.MEMBER) FROM V$LOG L "
            "JOIN V$LOGFILE F ON F.GROUP# = L.GROUP# WHERE L.NEXT_CHANGE# > :s AND L.FIRST_CHANGE# <= :e "
            "AND (F.STATUS IS NULL OR F.STATUS <> 'INVALID') AND L.STATUS <> 'UNUSED' "
            "GROUP BY L.THREAD#, L.SEQUENCE#, L.FIRST_CHANGE#, L.NEXT_CHANGE#",
            {"s": start_scn, "e": end_scn})
        for th, seq, first, nxt, name in cur:
            chosen.setdefault((int(th), int(seq)), (int(first), int(nxt), name))
        if not chosen:
            raise LogNotAvailable(f"no redo log covers SCN {start_scn}")
        by_thread: dict[int, list[tuple[int, int, int]]] = {}
        for (th, seq), (first, nxt, _) in chosen.items():
            by_thread.setdefault(th, []).append((seq, first, nxt))
        for th, lst in by_thread.items():
            lst.sort()
            if lst[0][1] > start_scn:
                raise LogNotAvailable(f"thread {th}: oldest available redo starts at SCN {lst[0][1]} > {start_scn} "
                                      f"(archived log removed?)")
            for a, b in zip(lst, lst[1:]):
                if b[0] != a[0] + 1:
                    raise LogNotAvailable(f"thread {th}: log sequence gap {a[0]} -> {b[0]}")
        return [(chosen[k][2], k[0], k[1], chosen[k][1]) for k in sorted(chosen)]

    # ---- session (kept across windows)
    def _end(self) -> None:
        if self._active:
            try:
                self.conn.cursor().execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
            except Exception as e:  # noqa: BLE001 - session may already be gone
                log.debug("END_LOGMNR failed: %s", e)
        self._active = False
        self._registered.clear()

    def close(self) -> None:
        self._end()

    def _prepare(self, start_scn: int, end_scn: int) -> None:
        """Register exactly the logs the window needs, touching only the difference to the current session."""
        if self.in_pdb:
            return
        want = self.log_entries(start_scn, end_scn)
        names = {n for n, _, _, _ in want}
        stale = [n for n, th, seq, _ in want if n in self._registered and self._registered[n][:2] != (th, seq)]
        same_seq_other_file = {(th, seq) for n, th, seq, _ in want if n not in self._registered} & {
            v[:2] for v in self._registered.values()}
        if stale or same_seq_other_file:
            self._end()                   # a registered file now holds other redo (online log reused / archived)
        cur = self.conn.cursor()
        for n in list(self._registered):
            if n not in names:            # entirely behind the window (V$ lists every log with NEXT_CHANGE# > start)
                cur.execute("BEGIN DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => :n); END;", {"n": n})
                del self._registered[n]
                self.stats["logs_removed"] += 1
        for n, th, seq, nxt in want:
            if n in self._registered:
                self._registered[n] = (th, seq, nxt)
                continue
            cur.execute("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :n, OPTIONS => :o); END;",
                        {"n": n, "o": 3 if self._registered else 1})       # NEW = 1, ADDFILE = 3
            self._registered[n] = (th, seq, nxt)
            self.stats["logs_added"] += 1

    # ---- mining
    def window(self, start_scn: int, end_scn: int) -> Iterator[dict[str, Any]]:
        """Rows of V$LOGMNR_CONTENTS with SCN in [start_scn, end_scn], SCN order. The LogMiner session stays open
        for the next window; any error resets it (END_LOGMNR, logs re-registered)."""
        try:
            self._prepare(start_scn, end_scn)
            self.conn.cursor().execute(
                "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :s, ENDSCN => :e, "
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;", {"s": start_scn, "e": end_scn})
        except BaseException:
            self._end()
            raise
        if not self._active:
            self.stats["sessions"] += 1
        self._active = True
        self.stats["windows"] += 1
        ok = False
        try:
            q = self.conn.cursor()
            q.arraysize = self.arraysize
            q.prefetchrows = self.arraysize
            binds: dict[str, Any] = {f"o{i}": s for i, s in enumerate(self.schemas)}
            binds.update(s=start_scn, e=end_scn)
            q.execute(contents_sql(len(self.schemas), self.ordered), binds)
            last = -1
            for r in q:
                row = dict(zip(CONTENTS_COLS, r))
                scn = int(row["SCN"]) if row.get("SCN") is not None else last
                if scn < last:
                    self.ordered = True
                    raise ScnOrderError(f"SCN {scn} after {last} in window [{start_scn}, {end_scn}]: switching to "
                                        "the ordered contents query; the capture restarts from the durable position")
                last = scn
                for k in ("SQL_REDO", "SQL_UNDO", "INFO"):
                    v = row.get(k)
                    if v is not None and hasattr(v, "read"):
                        row[k] = v.read()
                yield row
            ok = True
        finally:
            if not ok:
                self._end()

"""Minimal Db2 CLI + db2ReadLog binding via ctypes (stdlib only, Windows x64).

Proven approach (DESIGN.md §8 V1-V3):
- x64 process, Db2 x64 DLLs (db2cli64.dll, db2app64.dll);
- connect with CLI ``SQLConnect`` (never SQLDriverConnect) -> catalog SELECTs -> COMMIT -> db2ReadLog QUERY/READ;
- filter ON: each record is prefixed by db2ReadLogFilterData (56 B).

Read-only by default; ``execute()`` exists only for the dev L2 script against the ALELTTEST schema of a local test database.
"""
from __future__ import annotations

import ctypes as C
import os
import struct
from dataclasses import dataclass
from typing import Iterator, Optional

DB2BIN = os.environ.get("ALELT_DB2BIN", r"D:\IBM\SQLLIB\BIN")

# ---------------------------------------------------------------- Db2 structures (db2ApiDf.h, pack(8) _WIN64)


class LRI(C.Structure):
    _fields_ = [("lriType", C.c_uint64), ("part1", C.c_uint64), ("part2", C.c_uint64)]


class TimeOfLog(C.Structure):
    _fields_ = [("seconds", C.c_uint32), ("accuracy", C.c_uint32)]


class ReadLogInfo(C.Structure):
    _fields_ = [("initialLRI", LRI), ("firstReadLRI", LRI), ("nextStartLRI", LRI),
                ("firstReusedLRI", LRI), ("logRecsWritten", C.c_uint32),
                ("logBytesWritten", C.c_uint32), ("timeOfLRIReuse", C.c_uint32),
                ("currentTimeValue", TimeOfLog), ("futureUse1", C.c_uint32),
                ("oldestInFlightLSN", C.c_uint64), ("finalLRI", LRI)]


class ReadLogStruct(C.Structure):
    _fields_ = [("iCallerAction", C.c_uint32), ("piStartLRI", C.POINTER(LRI)),
                ("piEndLRI", C.POINTER(LRI)), ("poLogBuffer", C.c_char_p),
                ("iLogBufferSize", C.c_uint32), ("iFilterOption", C.c_uint32),
                ("poReadLogInfo", C.POINTER(ReadLogInfo)),
                ("piMinRequiredLRI", C.POINTER(LRI)), ("piStartTime", C.c_char_p)]


class SQLCA(C.Structure):
    _fields_ = [("sqlcaid", C.c_char * 8), ("sqlcabc", C.c_int32), ("sqlcode", C.c_int32),
                ("sqlerrml", C.c_short), ("sqlerrmc", C.c_char * 70), ("sqlerrp", C.c_char * 8),
                ("sqlerrd", C.c_int32 * 6), ("sqlwarn", C.c_char * 11), ("sqlstate", C.c_char * 5)]


DB2VERSION1010 = 10010000
READ, QUERY = 1, 3
FILTER_ON = 1
RLOG_READ_TO_CURRENT = 2654
FILTER_HDR = 56                                   # V3
LOG_HDR = struct.Struct("<IHHQQQ6sH")             # V4, 40 B
REC_NORMAL, REC_CLR, REC_COMMIT, REC_ABORT = 0x004E, 0x0043, 0x0084, 0x0041   # V5
FUNC = {161: "DELETE", 162: "INSERT", 163: "UPDATE", 164: "DELETE_EMPTY_PAGE", 165: "INSERT_EMPTY_PAGE",
        110: "UNDO_INSERT", 111: "UNDO_DELETE", 112: "UNDO_UPDATE", 166: "UNDO_DELETE_EMPTY_PAGE",
        124: "ALTER_TABLE_ATTR"}                  # V7


class Db2Error(RuntimeError):
    pass


def lri_text(lri: LRI) -> str:
    """Canonical Position.text for Db2: '<part1>:<part2 hex16>'."""
    return f"{lri.part1}:{lri.part2:016X}"


@dataclass
class LogRecord:
    lri_part1: int
    lri_part2: int
    rtype: int
    flag: int
    tid: str
    raw: bytes                          # full log record (header included), filter prefix excluded
    framed: bytes                       # filter prefix + record, exactly as returned by db2ReadLog
    func: Optional[int] = None
    tbspaceid: Optional[int] = None
    tableid: Optional[int] = None

    @property
    def lri(self) -> str:
        return f"{self.lri_part1}:{self.lri_part2:016X}"

    @property
    def label(self) -> str:
        if self.rtype == REC_COMMIT:
            return "COMMIT"
        if self.rtype == REC_ABORT:
            return "ROLLBACK"
        if self.func is not None:
            return FUNC.get(self.func, f"FUNC_{self.func}") + ("[CLR]" if self.rtype == REC_CLR else "")
        return f"TYPE_0x{self.rtype:04X}"


def parse_records(buf: bytes, count: int) -> Iterator[LogRecord]:
    off = 0
    for _ in range(count):
        lri1, part1, part2 = struct.unpack_from("<QQQ", buf, off)
        del lri1
        start = off
        off += FILTER_HDR
        size, rtype, rflag, _lsn, _lfs, _back, tid, _stream = LOG_HDR.unpack_from(buf, off)
        rec = buf[off:off + size]
        r = LogRecord(part1, part2, rtype, rflag, tid.hex().upper(), rec, buf[start:off + size])
        if rtype in (REC_NORMAL, REC_CLR):
            hdr = LOG_HDR.size + ((16 + (8 if rflag & 0x0002 else 0)) if rtype == REC_CLR else 0)
            if len(rec) >= hdr + 6:
                _comp, r.func, r.tbspaceid, r.tableid = struct.unpack_from("<BBHH", rec, hdr)   # V6
        off += size
        yield r


class Db2Cli:
    """One CLI connection. Use as a context manager."""

    def __init__(self, database: str, user: Optional[str] = None, password: Optional[str] = None) -> None:
        if C.sizeof(C.c_void_p) != 8:
            raise Db2Error("x64 Python required (Db2 x64 DLLs)")
        os.environ.setdefault("DB2CODEPAGE", "1208")     # UTF-8 client codepage (V9); must precede DLL load
        os.add_dll_directory(DB2BIN)
        os.environ["PATH"] = DB2BIN + os.pathsep + os.environ.get("PATH", "")
        self.cli = C.WinDLL(os.path.join(DB2BIN, "db2cli64.dll"))
        self.api = C.WinDLL(os.path.join(DB2BIN, "db2app64.dll"))
        self.env, self.dbc = C.c_void_p(), C.c_void_p()
        self._chk(self.cli.SQLAllocHandle(1, None, C.byref(self.env)), "alloc env")
        self.cli.SQLSetEnvAttr(self.env, 200, C.c_void_p(3), 0)          # SQL_ATTR_ODBC_VERSION=3
        self._chk(self.cli.SQLAllocHandle(2, self.env, C.byref(self.dbc)), "alloc dbc")
        self.cli.SQLSetConnectAttr(self.dbc, 102, C.c_void_p(0), 0)       # SQL_ATTR_AUTOCOMMIT off
        u = user.encode() if user else None
        p = password.encode() if password else None
        self._chk(self.cli.SQLConnect(self.dbc, database.encode(), -3, u, -3 if u else 0, p, -3 if p else 0),
                  "SQLConnect", 2, self.dbc)
        self.database = database

    # -------------------------------------------------------------- CLI
    def _chk(self, rc: int, what: str, htype: Optional[int] = None, h: Optional[C.c_void_p] = None) -> int:
        if rc in (0, 1, 100):
            return rc
        msg = ""
        if h is not None:
            state, native = C.create_string_buffer(6), C.c_int32()
            text, ln = C.create_string_buffer(1024), C.c_short()
            self.cli.SQLGetDiagRec(htype, h, 1, state, C.byref(native), text, 1024, C.byref(ln))
            msg = f" {state.value.decode()} {native.value} {text.value.decode(errors='replace')}"
        raise Db2Error(f"CLI error in {what}: rc={rc}{msg}")

    def query(self, sql: str) -> list[list[Optional[str]]]:
        st = C.c_void_p()
        self._chk(self.cli.SQLAllocHandle(3, self.dbc, C.byref(st)), "alloc stmt")
        try:
            self._chk(self.cli.SQLExecDirect(st, sql.encode(), -3), "exec", 3, st)
            ncol = C.c_short()
            self.cli.SQLNumResultCols(st, C.byref(ncol))
            rows: list[list[Optional[str]]] = []
            while self.cli.SQLFetch(st) == 0:
                row: list[Optional[str]] = []
                for i in range(1, ncol.value + 1):
                    buf, ind = C.create_string_buffer(4096), C.c_int64()
                    self.cli.SQLGetData(st, i, 1, buf, 4096, C.byref(ind))
                    row.append(None if ind.value < 0 else buf.value.decode("utf-8", "replace"))
                rows.append(row)
            return rows
        finally:
            self.cli.SQLFreeHandle(3, st)

    def execute(self, sql: str) -> None:
        """DEV ONLY (L2 ALELTTEST schema, local test database). Never used against ERP (R1)."""
        st = C.c_void_p()
        self._chk(self.cli.SQLAllocHandle(3, self.dbc, C.byref(st)), "alloc stmt")
        try:
            self._chk(self.cli.SQLExecDirect(st, sql.encode(), -3), f"exec {sql[:40]}", 3, st)
        finally:
            self.cli.SQLFreeHandle(3, st)

    def commit(self) -> None:
        self.cli.SQLEndTran(2, self.dbc, 0)

    def rollback(self) -> None:
        self.cli.SQLEndTran(2, self.dbc, 1)

    def close(self) -> None:
        self.commit()
        self.cli.SQLDisconnect(self.dbc)
        self.cli.SQLFreeHandle(2, self.dbc)
        self.cli.SQLFreeHandle(1, self.env)

    def __enter__(self) -> "Db2Cli":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -------------------------------------------------------------- db2ReadLog
    def _readlog(self, req: ReadLogStruct) -> SQLCA:
        sqlca = SQLCA()
        self.api.db2ReadLog(DB2VERSION1010, C.byref(req), C.byref(sqlca))
        if sqlca.sqlcode < 0:
            raise Db2Error(f"db2ReadLog failed: SQLCODE {sqlca.sqlcode} "
                           f"{sqlca.sqlerrmc[:sqlca.sqlerrml].decode(errors='replace')}")
        return sqlca

    def log_query(self) -> ReadLogInfo:
        """QUERY: current log state (initialLRI, nextStartLRI). Caller must COMMIT before (V2)."""
        self.commit()
        info = ReadLogInfo()
        req = ReadLogStruct(iCallerAction=QUERY, iFilterOption=FILTER_ON, poReadLogInfo=C.pointer(info))
        self._readlog(req)
        return info

    def log_read(self, start: LRI, end: Optional[LRI] = None, buf_kb: int = 64,
                 max_loops: int = 10_000) -> Iterator[tuple[bytes, int, ReadLogInfo]]:
        """READ loop from `start` until sqlcode 2654 (V2). Yields (buffer bytes, rec count, info) per call."""
        self.commit()
        info = ReadLogInfo()
        if end is None:
            end = self.log_query().nextStartLRI
        s, e = LRI(), LRI()
        C.memmove(C.byref(s), C.byref(start), C.sizeof(LRI))
        C.memmove(C.byref(e), C.byref(end), C.sizeof(LRI))
        buf = C.create_string_buffer(buf_kb * 1024)
        req = ReadLogStruct(iCallerAction=READ, piStartLRI=C.pointer(s), piEndLRI=C.pointer(e),
                            poLogBuffer=C.cast(buf, C.c_char_p), iLogBufferSize=len(buf),
                            iFilterOption=FILTER_ON, poReadLogInfo=C.pointer(info))
        for _ in range(max_loops):
            sqlca = self._readlog(req)
            n = info.logRecsWritten
            yield buf.raw[:info.logBytesWritten], n, info
            if sqlca.sqlcode == RLOG_READ_TO_CURRENT or n == 0:
                return
            C.memmove(C.byref(s), C.byref(info.nextStartLRI), C.sizeof(LRI))

    def table_ids(self) -> dict[tuple[int, int], str]:
        rows = self.query("SELECT TABSCHEMA, TABNAME, TBSPACEID, TABLEID FROM SYSCAT.TABLES "
                          "WHERE TYPE='T' AND TABSCHEMA NOT LIKE 'SYS%'")
        return {(int(t or 0), int(o or 0)): f"{(s or '').strip()}.{n}" for s, n, t, o in rows}

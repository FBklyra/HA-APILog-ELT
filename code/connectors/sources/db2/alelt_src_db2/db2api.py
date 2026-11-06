"""Thin ctypes access to the Db2 CLI and the db2ReadLog API (Windows x64 Db2 client).

Read-only helpers used by catalog.py and the corpus builder. Uses SQLConnect (SQLDriverConnect is
known to hang on this client). Struct layouts follow the IBM Db2 public documentation of the
db2ReadLog API; the installed client headers are the reference, nothing is vendored.
"""
from __future__ import annotations

import ctypes as C
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

DB2BIN = os.environ.get("ALELT_DB2BIN", r"D:\IBM\SQLLIB\BIN")

# CLI constants (ODBC / Db2 CLI)
_ENV, _DBC, _STMT = 1, 2, 3
_ATTR_ODBC_VERSION, _OV_ODBC3 = 200, 3
_ATTR_AUTOCOMMIT = 102
_NTS = -3
_C_CHAR, _C_WCHAR, _C_SBIGINT, _C_DOUBLE, _C_BINARY = 1, -8, -25, 8, -2
_NO_DATA, _NULL_DATA = 100, -1
_COMMIT, _ROLLBACK = 0, 1

# db2ReadLog constants
DB2VERSION1010 = 10010000
RL_READ, RL_QUERY = 1, 3
RL_FILTER_ON = 1
RL_READ_TO_CURRENT = 2654


class Db2Error(RuntimeError):
    pass


class LRI(C.Structure):
    _fields_ = [("lriType", C.c_uint64), ("part1", C.c_uint64), ("part2", C.c_uint64)]

    def text(self) -> str:
        return f"{self.part1}:{self.part2:016X}"


class _TimeOfLog(C.Structure):
    _fields_ = [("seconds", C.c_uint32), ("accuracy", C.c_uint32)]


class ReadLogInfo(C.Structure):
    _fields_ = [("initialLRI", LRI), ("firstReadLRI", LRI), ("nextStartLRI", LRI),
                ("firstReusedLRI", LRI), ("logRecsWritten", C.c_uint32),
                ("logBytesWritten", C.c_uint32), ("timeOfLRIReuse", C.c_uint32),
                ("currentTimeValue", _TimeOfLog), ("futureUse1", C.c_uint32),
                ("oldestInFlightLSN", C.c_uint64), ("finalLRI", LRI)]


class _ReadLogStruct(C.Structure):
    _fields_ = [("iCallerAction", C.c_uint32), ("piStartLRI", C.POINTER(LRI)),
                ("piEndLRI", C.POINTER(LRI)), ("poLogBuffer", C.c_void_p),
                ("iLogBufferSize", C.c_uint32), ("iFilterOption", C.c_uint32),
                ("poReadLogInfo", C.POINTER(ReadLogInfo)),
                ("piMinRequiredLRI", C.POINTER(LRI)), ("piStartTime", C.c_char_p)]


class _SQLCA(C.Structure):
    _fields_ = [("sqlcaid", C.c_char * 8), ("sqlcabc", C.c_int32), ("sqlcode", C.c_int32),
                ("sqlerrml", C.c_short), ("sqlerrmc", C.c_char * 70), ("sqlerrp", C.c_char * 8),
                ("sqlerrd", C.c_int32 * 6), ("sqlwarn", C.c_char * 11), ("sqlstate", C.c_char * 5)]


@dataclass
class ReadChunk:
    raw: bytes
    recs: int
    next_start: str
    sqlcode: int


def _load() -> tuple[Any, Any]:
    os.add_dll_directory(DB2BIN)
    os.environ["PATH"] = DB2BIN + os.pathsep + os.environ.get("PATH", "")
    cli = C.WinDLL(os.path.join(DB2BIN, "db2cli64.dll"))
    api = C.WinDLL(os.path.join(DB2BIN, "db2app64.dll"))
    H, P = C.c_void_p, C.POINTER
    cli.SQLAllocHandle.argtypes = [C.c_short, H, P(H)]
    cli.SQLSetEnvAttr.argtypes = [H, C.c_int32, H, C.c_int32]
    cli.SQLSetConnectAttr.argtypes = [H, C.c_int32, H, C.c_int32]
    cli.SQLConnect.argtypes = [H, C.c_char_p, C.c_short, C.c_char_p, C.c_short, C.c_char_p, C.c_short]
    cli.SQLExecDirectW.argtypes = [H, C.c_wchar_p, C.c_int32]
    cli.SQLNumResultCols.argtypes = [H, P(C.c_short)]
    cli.SQLFetch.argtypes = [H]
    cli.SQLGetData.argtypes = [H, C.c_ushort, C.c_short, H, C.c_int64, P(C.c_int64)]
    cli.SQLDescribeColW.argtypes = [H, C.c_ushort, C.c_wchar_p, C.c_short, P(C.c_short), P(C.c_short),
                                    P(C.c_uint64), P(C.c_short), P(C.c_short)]
    cli.SQLGetDiagRec.argtypes = [C.c_short, H, C.c_short, C.c_char_p, P(C.c_int32), C.c_char_p,
                                  C.c_short, P(C.c_short)]
    cli.SQLEndTran.argtypes = [C.c_short, H, C.c_short]
    cli.SQLFreeHandle.argtypes = [C.c_short, H]
    cli.SQLDisconnect.argtypes = [H]
    api.db2ReadLog.argtypes = [C.c_uint32, H, H]
    return cli, api


class Db2Connection:
    """One CLI connection; autocommit OFF by default. Not thread-safe."""

    def __init__(self, database: str, user: str | None = None, password: str | None = None,
                 autocommit: bool = False) -> None:
        self._cli, self._api = _load()
        self._env, self._dbc = C.c_void_p(), C.c_void_p()
        self._ok(self._cli.SQLAllocHandle(_ENV, None, C.byref(self._env)), "alloc env")
        self._cli.SQLSetEnvAttr(self._env, _ATTR_ODBC_VERSION, C.c_void_p(_OV_ODBC3), 0)
        self._ok(self._cli.SQLAllocHandle(_DBC, self._env, C.byref(self._dbc)), "alloc dbc")
        self._cli.SQLSetConnectAttr(self._dbc, _ATTR_AUTOCOMMIT, C.c_void_p(1 if autocommit else 0), 0)
        u = user.encode() if user else None
        p = password.encode() if password else None
        self._ok(self._cli.SQLConnect(self._dbc, database.encode(), _NTS, u, _NTS if u else 0,
                                      p, _NTS if p else 0), "connect", _DBC, self._dbc)
        self.database = database

    # ------------------------------------------------------------ CLI
    def _ok(self, rc: int, what: str, htype: int | None = None, h: C.c_void_p | None = None) -> None:
        if rc in (0, 1, _NO_DATA):
            return
        msg = ""
        if h is not None and htype is not None:
            st, nat, txt, ln = C.create_string_buffer(6), C.c_int32(), C.create_string_buffer(1024), C.c_short()
            self._cli.SQLGetDiagRec(htype, h, 1, st, C.byref(nat), txt, 1024, C.byref(ln))
            msg = f" {st.value.decode()} {nat.value} {txt.value.decode(errors='replace')}"
        raise Db2Error(f"CLI {what} rc={rc}{msg}")

    def execute(self, sql: str, fetch: bool = True,
                ctypes: Sequence[str] | None = None) -> list[list[Any]]:
        """Run one statement. ctypes per column: 'str' (default, UTF-16), 'int', 'float', 'bytes'."""
        st = C.c_void_p()
        self._ok(self._cli.SQLAllocHandle(_STMT, self._dbc, C.byref(st)), "alloc stmt")
        try:
            self._ok(self._cli.SQLExecDirectW(st, sql, _NTS), f"exec [{sql[:80]}]", _STMT, st)
            if not fetch:
                return []
            n = C.c_short()
            self._cli.SQLNumResultCols(st, C.byref(n))
            rows: list[list[Any]] = []
            while self._cli.SQLFetch(st) == 0:
                rows.append([self._get(st, i, (ctypes[i - 1] if ctypes else "str"))
                             for i in range(1, n.value + 1)])
            return rows
        finally:
            self._cli.SQLFreeHandle(_STMT, st)

    @staticmethod
    def _ind(ind: C.c_int64) -> int:
        """Indicator as signed value; some client paths only write the low 32 bits."""
        v = ind.value
        return v - (1 << 32) if (1 << 31) <= v < (1 << 32) else v

    def _get(self, st: C.c_void_p, i: int, kind: str) -> Any:
        ind = C.c_int64(0)
        if kind == "int":
            v = C.c_int64()
            self._cli.SQLGetData(st, i, _C_SBIGINT, C.byref(v), 8, C.byref(ind))
            return None if self._ind(ind) == _NULL_DATA else v.value
        if kind == "float":
            d = C.c_double()
            self._cli.SQLGetData(st, i, _C_DOUBLE, C.byref(d), 8, C.byref(ind))
            return None if self._ind(ind) == _NULL_DATA else d.value
        ctype = _C_BINARY if kind == "bytes" else _C_WCHAR
        term = 0 if kind == "bytes" else 2
        out = bytearray()
        buf = C.create_string_buffer(65536)
        while True:
            rc = self._cli.SQLGetData(st, i, ctype, buf, len(buf), C.byref(ind))
            if rc == _NO_DATA:
                break
            self._ok(rc, "getdata", _STMT, st)
            n = self._ind(ind)
            if n == _NULL_DATA:
                return None
            got = len(buf) - term if (rc == 1 and (n < 0 or n >= len(buf) - term)) else n
            out += buf.raw[:got]
            if rc == 0:
                break
        return bytes(out) if kind == "bytes" else out.decode("utf-16-le")

    def commit(self) -> None:
        self._ok(self._cli.SQLEndTran(_DBC, self._dbc, _COMMIT), "commit", _DBC, self._dbc)

    def rollback(self) -> None:
        self._ok(self._cli.SQLEndTran(_DBC, self._dbc, _ROLLBACK), "rollback", _DBC, self._dbc)

    def close(self, commit: bool = True) -> None:
        self._cli.SQLEndTran(_DBC, self._dbc, _COMMIT if commit else _ROLLBACK)
        self._cli.SQLDisconnect(self._dbc)
        self._cli.SQLFreeHandle(_DBC, self._dbc)
        self._cli.SQLFreeHandle(_ENV, self._env)

    # ------------------------------------------------------------ db2ReadLog
    def _readlog(self, req: _ReadLogStruct) -> int:
        ca = _SQLCA()
        self._api.db2ReadLog(DB2VERSION1010, C.byref(req), C.byref(ca))
        if ca.sqlcode < 0:
            raise Db2Error(f"db2ReadLog SQLCODE {ca.sqlcode} {ca.sqlerrmc[:ca.sqlerrml]!r}")
        return int(ca.sqlcode)

    def query_lri(self) -> tuple[str, str]:
        """QUERY: returns (initialLRI, nextStartLRI) text forms. Caller must have committed."""
        info = ReadLogInfo()
        req = _ReadLogStruct(iCallerAction=RL_QUERY, iFilterOption=RL_FILTER_ON, poReadLogInfo=C.pointer(info))
        self._readlog(req)
        return info.initialLRI.text(), info.nextStartLRI.text()

    def read_log(self, start: str, end: str | None = None, buf_size: int = 256 * 1024,
                 max_loops: int = 10000) -> list[ReadChunk]:
        """READ (filter ON) from start until end/current. Returns raw buffers exactly as returned."""
        def mk(t: str) -> LRI:
            p1, p2 = t.split(":")
            return LRI(1, int(p1), int(p2, 16))
        s = mk(start)
        e = mk(end) if end else LRI(1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
        info = ReadLogInfo()
        buf = C.create_string_buffer(buf_size)
        req = _ReadLogStruct(iCallerAction=RL_READ, piStartLRI=C.pointer(s), piEndLRI=C.pointer(e),
                             poLogBuffer=C.cast(buf, C.c_void_p), iLogBufferSize=buf_size,
                             iFilterOption=RL_FILTER_ON, poReadLogInfo=C.pointer(info))
        out: list[ReadChunk] = []
        for _ in range(max_loops):
            code = self._readlog(req)
            out.append(ReadChunk(buf.raw[:info.logBytesWritten], info.logRecsWritten,
                                 info.nextStartLRI.text(), code))
            if code == RL_READ_TO_CURRENT or info.logRecsWritten == 0:
                break
            C.memmove(C.byref(s), C.byref(info.nextStartLRI), C.sizeof(LRI))
        return out

    def __enter__(self) -> Db2Connection:
        return self

    def __exit__(self, exc_type: object, *_: object) -> None:
        self.close(commit=exc_type is None)

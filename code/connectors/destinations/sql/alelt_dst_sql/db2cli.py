"""Minimal DB-API-style access to the local Db2 CLI (db2cli64.dll) through ctypes, with parameter binding.

Same approach as connectors/sources/db2/alelt_src_db2/db2api.py (SQLConnect to a catalogued alias; no vendored
headers - constants follow the public ODBC / Db2 CLI documentation). Used by the db2 dialect when the local Db2
client is installed (ALELT_DB2BIN, default D:\\IBM\\SQLLIB\\BIN); remote targets can use ibm_db (`?driver=ibm_db`).
Autocommit OFF; one statement handle per execute; not thread-safe.
"""
from __future__ import annotations

import ctypes as C
import os
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

DB2BIN = os.environ.get("ALELT_DB2BIN", r"D:\IBM\SQLLIB\BIN")

_ENV, _DBC, _STMT = 1, 2, 3
_ATTR_ODBC_VERSION, _OV_ODBC3, _ATTR_AUTOCOMMIT = 200, 3, 102
_NTS, _NULL_DATA, _NO_DATA = -3, -1, 100
_COMMIT, _ROLLBACK = 0, 1
_PARAM_INPUT = 1
# C types
_C_WCHAR, _C_SBIGINT, _C_DOUBLE, _C_BINARY = -8, -25, 8, -2
# SQL types
_SQL_DECIMAL, _SQL_NUMERIC, _SQL_INTEGER, _SQL_SMALLINT, _SQL_FLOAT, _SQL_REAL, _SQL_DOUBLE = 3, 2, 4, 5, 6, 7, 8
_SQL_BIGINT, _SQL_TINYINT, _SQL_BIT, _SQL_BOOLEAN = -5, -6, -7, 16
_SQL_DATE, _SQL_TIME, _SQL_TIMESTAMP = 91, 92, 93
_SQL_WVARCHAR, _SQL_CLOB, _SQL_VARBINARY, _SQL_BLOB = -9, -99, -3, -98
_SQL_BINARY, _SQL_LONGVARBINARY, _SQL_DECFLOAT = -2, -4, -360
_INTS = {_SQL_INTEGER, _SQL_SMALLINT, _SQL_BIGINT, _SQL_TINYINT}
_BINS = {_SQL_BINARY, _SQL_VARBINARY, _SQL_LONGVARBINARY, _SQL_BLOB}

_lib: Any = None


class Error(RuntimeError):
    def __init__(self, msg: str, sqlstate: str = "", sqlcode: int = 0) -> None:
        super().__init__(msg)
        self.sqlstate, self.sqlcode = sqlstate, sqlcode


def _load() -> Any:
    global _lib
    if _lib is not None:
        return _lib
    if os.name == "nt":
        os.add_dll_directory(DB2BIN)
    cli = C.WinDLL(os.path.join(DB2BIN, "db2cli64.dll")) if os.name == "nt" else C.CDLL("libdb2.so")
    H, P = C.c_void_p, C.POINTER
    cli.SQLAllocHandle.argtypes = [C.c_short, H, P(H)]
    cli.SQLSetEnvAttr.argtypes = [H, C.c_int32, H, C.c_int32]
    cli.SQLSetConnectAttr.argtypes = [H, C.c_int32, H, C.c_int32]
    cli.SQLConnect.argtypes = [H, C.c_char_p, C.c_short, C.c_char_p, C.c_short, C.c_char_p, C.c_short]
    cli.SQLPrepareW.argtypes = [H, C.c_wchar_p, C.c_int32]
    cli.SQLExecute.argtypes = [H]
    cli.SQLBindParameter.argtypes = [H, C.c_ushort, C.c_short, C.c_short, C.c_short, C.c_uint64, C.c_short, H,
                                     C.c_int64, P(C.c_int64)]
    cli.SQLNumResultCols.argtypes = [H, P(C.c_short)]
    cli.SQLRowCount.argtypes = [H, P(C.c_int64)]
    cli.SQLFetch.argtypes = [H]
    cli.SQLGetData.argtypes = [H, C.c_ushort, C.c_short, H, C.c_int64, P(C.c_int64)]
    cli.SQLDescribeColW.argtypes = [H, C.c_ushort, C.c_wchar_p, C.c_short, P(C.c_short), P(C.c_short),
                                    P(C.c_uint64), P(C.c_short), P(C.c_short)]
    cli.SQLGetDiagRec.argtypes = [C.c_short, H, C.c_short, C.c_char_p, P(C.c_int32), C.c_char_p, C.c_short,
                                  P(C.c_short)]
    cli.SQLEndTran.argtypes = [C.c_short, H, C.c_short]
    cli.SQLFreeHandle.argtypes = [C.c_short, H]
    cli.SQLDisconnect.argtypes = [H]
    _lib = cli
    return cli


def available() -> bool:
    return os.path.exists(os.path.join(DB2BIN, "db2cli64.dll" if os.name == "nt" else "libdb2.so"))


def _check(cli: Any, rc: int, what: str, htype: int, h: Any) -> None:
    if rc in (0, 1, _NO_DATA):
        return
    st, nat, txt, ln = C.create_string_buffer(6), C.c_int32(), C.create_string_buffer(1024), C.c_short()
    cli.SQLGetDiagRec(htype, h, 1, st, C.byref(nat), txt, 1024, C.byref(ln))
    raise Error(f"Db2 CLI {what} rc={rc} {st.value.decode()} {nat.value} {txt.value.decode(errors='replace')}",
                st.value.decode(), nat.value)


def _ind(ind: C.c_int64) -> int:
    v = ind.value
    return v - (1 << 32) if (1 << 31) <= v < (1 << 32) else v


class Connection:
    def __init__(self, database: str, user: str | None = None, password: str | None = None) -> None:
        self._cli = cli = _load()
        self._env, self._dbc = C.c_void_p(), C.c_void_p()
        _check(cli, cli.SQLAllocHandle(_ENV, None, C.byref(self._env)), "alloc env", _ENV, self._env)
        cli.SQLSetEnvAttr(self._env, _ATTR_ODBC_VERSION, C.c_void_p(_OV_ODBC3), 0)
        _check(cli, cli.SQLAllocHandle(_DBC, self._env, C.byref(self._dbc)), "alloc dbc", _ENV, self._env)
        cli.SQLSetConnectAttr(self._dbc, _ATTR_AUTOCOMMIT, C.c_void_p(0), 0)
        u = user.encode() if user else None
        p = password.encode() if password else None
        _check(cli, cli.SQLConnect(self._dbc, database.encode(), _NTS, u, _NTS if u else 0, p, _NTS if p else 0),
               "connect", _DBC, self._dbc)
        self.closed = False

    def cursor(self) -> Cursor:
        return Cursor(self)

    def commit(self) -> None:
        _check(self._cli, self._cli.SQLEndTran(_DBC, self._dbc, _COMMIT), "commit", _DBC, self._dbc)

    def rollback(self) -> None:
        _check(self._cli, self._cli.SQLEndTran(_DBC, self._dbc, _ROLLBACK), "rollback", _DBC, self._dbc)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._cli.SQLEndTran(_DBC, self._dbc, _ROLLBACK)
        self._cli.SQLDisconnect(self._dbc)
        self._cli.SQLFreeHandle(_DBC, self._dbc)
        self._cli.SQLFreeHandle(_ENV, self._env)


class Cursor:
    def __init__(self, conn: Connection) -> None:
        self.conn, self._cli = conn, conn._cli
        self.description: list[tuple[Any, ...]] | None = None
        self.rowcount = -1
        self._rows: list[tuple[Any, ...]] = []

    # -------------------------------------------------------------- binding
    def _bind(self, st: Any, i: int, v: Any, keep: list[Any]) -> None:
        ind = C.c_int64(0)
        if v is None:
            buf, ctype, stype, size, dd, blen = C.create_unicode_buffer(1), _C_WCHAR, _SQL_WVARCHAR, 1, 0, 2
            ind.value = _NULL_DATA
        elif isinstance(v, bool):
            buf, ctype, stype, size, dd, blen = C.c_int64(int(v)), _C_SBIGINT, _SQL_SMALLINT, 5, 0, 8
        elif isinstance(v, int):
            buf, ctype, stype, size, dd, blen = C.c_int64(v), _C_SBIGINT, _SQL_BIGINT, 19, 0, 8
        elif isinstance(v, float):
            buf, ctype, stype, size, dd, blen = C.c_double(v), _C_DOUBLE, _SQL_DOUBLE, 15, 0, 8
        elif isinstance(v, (bytes, bytearray, memoryview)):
            b = bytes(v)
            buf = C.create_string_buffer(b, max(1, len(b)))
            ctype, stype, size, dd, blen = _C_BINARY, (_SQL_VARBINARY if len(b) <= 32672 else _SQL_BLOB), \
                max(1, len(b)), 0, len(b)
            ind.value = len(b)
        else:
            if isinstance(v, Decimal):
                s = format(v, "f")
                exp = v.as_tuple().exponent
                stype, size, dd = _SQL_DECIMAL, 31, max(0, -exp) if isinstance(exp, int) else 0
            elif isinstance(v, datetime):
                s = v.replace(tzinfo=None).isoformat(sep=" ")
                stype, size, dd = _SQL_TIMESTAMP, 32, 6
            elif isinstance(v, date):
                s, stype, size, dd = v.isoformat(), _SQL_DATE, 10, 0
            elif isinstance(v, time):
                s, stype, size, dd = v.replace(microsecond=0).isoformat(), _SQL_TIME, 8, 0
            else:
                s = str(v)
                stype, size, dd = (_SQL_WVARCHAR if len(s) <= 8000 else _SQL_CLOB), max(1, len(s)), 0
            raw = s.encode("utf-16-le")
            buf = C.create_string_buffer(raw + b"\x00\x00", len(raw) + 2)
            ctype, blen = _C_WCHAR, len(raw) + 2
            ind.value = len(raw)
        keep += [buf, ind]
        _check(self._cli, self._cli.SQLBindParameter(st, i, _PARAM_INPUT, ctype, stype, size, dd,
                                                     C.cast(C.byref(buf), C.c_void_p), blen, C.byref(ind)),
               f"bind #{i}", _STMT, st)

    # -------------------------------------------------------------- execute / fetch
    def execute(self, sql: str, params: Any = ()) -> Cursor:
        cli = self._cli
        st = C.c_void_p()
        _check(cli, cli.SQLAllocHandle(_STMT, self.conn._dbc, C.byref(st)), "alloc stmt", _DBC, self.conn._dbc)
        keep: list[Any] = []
        try:
            _check(cli, cli.SQLPrepareW(st, sql, _NTS), f"prepare [{sql[:120]}]", _STMT, st)
            for i, v in enumerate(params or (), 1):
                self._bind(st, i, v, keep)
            _check(cli, cli.SQLExecute(st), f"execute [{sql[:120]}]", _STMT, st)
            rc = C.c_int64(-1)
            cli.SQLRowCount(st, C.byref(rc))
            self.rowcount = rc.value
            n = C.c_short()
            cli.SQLNumResultCols(st, C.byref(n))
            self.description, self._rows = None, []
            if n.value:
                types = []
                desc = []
                for c in range(1, n.value + 1):
                    name, ln, typ, size, dd, nl = (C.create_unicode_buffer(256), C.c_short(), C.c_short(),
                                                   C.c_uint64(), C.c_short(), C.c_short())
                    cli.SQLDescribeColW(st, c, name, 256, C.byref(ln), C.byref(typ), C.byref(size), C.byref(dd),
                                        C.byref(nl))
                    types.append(typ.value)
                    desc.append((name.value, typ.value, None, size.value, size.value, dd.value, bool(nl.value)))
                self.description = desc
                while cli.SQLFetch(st) == 0:
                    self._rows.append(tuple(self._get(st, c, t) for c, t in enumerate(types, 1)))
        finally:
            cli.SQLFreeHandle(_STMT, st)
        return self

    def executemany(self, sql: str, seq: Any) -> None:
        for p in seq:
            self.execute(sql, p)

    def _get(self, st: Any, i: int, t: int) -> Any:
        cli = self._cli
        ind = C.c_int64(0)
        if t in _INTS:
            v = C.c_int64()
            cli.SQLGetData(st, i, _C_SBIGINT, C.byref(v), 8, C.byref(ind))
            return None if _ind(ind) == _NULL_DATA else v.value
        if t in (_SQL_BOOLEAN, _SQL_BIT):
            v = C.c_int64()
            cli.SQLGetData(st, i, _C_SBIGINT, C.byref(v), 8, C.byref(ind))
            return None if _ind(ind) == _NULL_DATA else bool(v.value)
        if t in (_SQL_DOUBLE, _SQL_FLOAT, _SQL_REAL):
            d = C.c_double()
            cli.SQLGetData(st, i, _C_DOUBLE, C.byref(d), 8, C.byref(ind))
            return None if _ind(ind) == _NULL_DATA else d.value
        binary = t in _BINS
        ctype, term = (_C_BINARY, 0) if binary else (_C_WCHAR, 2)
        out = bytearray()
        buf = C.create_string_buffer(65536)
        while True:
            rc = cli.SQLGetData(st, i, ctype, buf, len(buf), C.byref(ind))
            if rc == _NO_DATA:
                break
            _check(cli, rc, "getdata", _STMT, st)
            n = _ind(ind)
            if n == _NULL_DATA:
                return None
            got = len(buf) - term if (rc == 1 and (n < 0 or n >= len(buf) - term)) else n
            out += buf.raw[:got]
            if rc == 0:
                break
        if binary:
            return bytes(out)
        s = out.decode("utf-16-le")
        if t in (_SQL_DECIMAL, _SQL_NUMERIC, _SQL_DECFLOAT):
            return Decimal(s)
        if t == _SQL_DATE:
            return date.fromisoformat(s)
        if t == _SQL_TIME:
            return time.fromisoformat(s.replace(".", ":"))
        if t == _SQL_TIMESTAMP:
            s = s.replace("T", " ")
            if len(s) > 26:                       # timestamp(12): keep microseconds
                s = s[:26]
            return datetime.fromisoformat(s)
        return s

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows.pop(0) if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        out, self._rows = self._rows, []
        return out

    def close(self) -> None:
        self._rows = []


def connect(database: str, user: str | None = None, password: str | None = None) -> Connection:
    return Connection(database, user, password)

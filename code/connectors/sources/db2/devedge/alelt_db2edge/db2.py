"""Db2 CLI + db2ReadLog through ctypes (public IBM APIs only, ADR-001). x64 only (the Db2 client is 64-bit).

Windows: db2cli64.dll + db2app64.dll from <SQLLIB>\\BIN.  Linux: libdb2.so(.1) from <sqllib>/lib64 (CLI and the
administrative API are in the same library). Structure layouts are identical on Win64 (LLP64) and Linux x86-64 /
aarch64 (LP64): every field is a fixed-width integer or a pointer.
"""
from __future__ import annotations

import ctypes as C
import hashlib
import os
import struct
import sys
import uuid
import zlib
from typing import Any, Dict, List, Optional, Tuple

DB2VERSION1010 = 10010000
RL_READ, RL_QUERY, RL_FILTER_ON = 1, 3, 1
RL_READ_TO_CURRENT = 2654
FILTER_HDR = 56                    # db2ReadLogFilterData: LRI1(24) LRI2(24) realLen u32 sqlcode i32
LOG_HDR = struct.Struct("<IHHQQQ6sH")
U64 = 0xFFFFFFFFFFFFFFFF


class LRI(C.Structure):
    _fields_ = [("lriType", C.c_uint64), ("part1", C.c_uint64), ("part2", C.c_uint64)]

    def text(self) -> str:
        return f"{self.part1}:{self.part2:016X}"


class _TimeOfLog(C.Structure):
    _fields_ = [("seconds", C.c_uint32), ("accuracy", C.c_uint32)]


class ReadLogInfo(C.Structure):
    _fields_ = [("initialLRI", LRI), ("firstReadLRI", LRI), ("nextStartLRI", LRI), ("firstReusedLRI", LRI),
                ("logRecsWritten", C.c_uint32), ("logBytesWritten", C.c_uint32), ("timeOfLRIReuse", C.c_uint32),
                ("currentTimeValue", _TimeOfLog), ("futureUse1", C.c_uint32), ("oldestInFlightLSN", C.c_uint64),
                ("finalLRI", LRI)]


class ReadLogStruct(C.Structure):
    _fields_ = [("iCallerAction", C.c_uint32), ("piStartLRI", C.POINTER(LRI)), ("piEndLRI", C.POINTER(LRI)),
                ("poLogBuffer", C.c_void_p), ("iLogBufferSize", C.c_uint32), ("iFilterOption", C.c_uint32),
                ("poReadLogInfo", C.POINTER(ReadLogInfo)), ("piMinRequiredLRI", C.POINTER(LRI)),
                ("piStartTime", C.c_char_p)]


class SQLCA(C.Structure):
    _fields_ = [("sqlcaid", C.c_char * 8), ("sqlcabc", C.c_int32), ("sqlcode", C.c_int32),
                ("sqlerrml", C.c_short), ("sqlerrmc", C.c_char * 70), ("sqlerrp", C.c_char * 8),
                ("sqlerrd", C.c_int32 * 6), ("sqlwarn", C.c_char * 11), ("sqlstate", C.c_char * 5)]


def lri_key(text: str) -> Tuple[int, int]:
    """Total order of LRI text '<part1>:<part2 hex16>' (ValueError when malformed)."""
    a, sep, b = text.partition(":")
    if not sep:
        raise ValueError(f"invalid LRI {text!r}")
    return int(a), int(b, 16)


def lri_struct(text: str) -> LRI:
    p1, p2 = lri_key(text)
    return LRI(1, p1, p2)


class Db2Error(RuntimeError):
    pass


def _load_libs(lib_dir: str) -> Tuple[Any, Any]:
    if sys.platform == "win32":
        if hasattr(os, "add_dll_directory"):
            os.add_dll_directory(lib_dir)
        os.environ["PATH"] = lib_dir + os.pathsep + os.environ.get("PATH", "")
        return C.WinDLL(os.path.join(lib_dir, "db2cli64.dll")), C.WinDLL(os.path.join(lib_dir, "db2app64.dll"))
    last: Optional[OSError] = None
    for name in ("libdb2.so.1", "libdb2.so"):
        try:
            lib = C.CDLL(os.path.join(lib_dir, name) if lib_dir else name)
            return lib, lib
        except OSError as e:
            last = e
    raise Db2Error(f"Db2 client library not found in {lib_dir or 'the loader path'}: {last}")


class Db2:
    """Db2 CLI connection (SQLConnect - SQLDriverConnect hangs, ADR-010) + db2ReadLog on that connection."""

    def __init__(self, database: str, lib_dir: str, user: Optional[str] = None, password: Optional[str] = None) -> None:
        self.cli, self.api = _load_libs(lib_dir)
        self.env, self.dbc = C.c_void_p(), C.c_void_p()
        self._ok(self.cli.SQLAllocHandle(1, None, C.byref(self.env)), "alloc env")
        self.cli.SQLSetEnvAttr(self.env, 200, C.c_void_p(3), 0)          # ODBC 3
        self._ok(self.cli.SQLAllocHandle(2, self.env, C.byref(self.dbc)), "alloc dbc")
        u = user.encode() if user else None
        pw = password.encode() if password else None
        self._ok(self.cli.SQLConnect(self.dbc, database.encode(), -3, u, -3 if u else 0, pw, -3 if pw else 0),
                 "connect", 2, self.dbc)
        self.cli.SQLSetConnectAttr(self.dbc, 102, C.c_void_p(0), 0)       # autocommit off

    def _ok(self, rc: int, what: str, htype: Optional[int] = None, h: Optional[C.c_void_p] = None) -> None:
        if rc in (0, 1, 100):
            return
        msg = ""
        if h is not None:
            st, nat, txt, ln = C.create_string_buffer(6), C.c_int32(), C.create_string_buffer(1024), C.c_short()
            self.cli.SQLGetDiagRec(htype, h, 1, st, C.byref(nat), txt, 1024, C.byref(ln))
            msg = f" {st.value.decode()} {nat.value} {txt.value.decode(errors='replace')}"
        raise Db2Error(f"CLI {what}: rc={rc}{msg}")

    def query(self, sql: str) -> List[List[Optional[str]]]:
        st = C.c_void_p()
        self._ok(self.cli.SQLAllocHandle(3, self.dbc, C.byref(st)), "alloc stmt")
        try:
            self._ok(self.cli.SQLExecDirect(st, sql.encode(), -3), "exec", 3, st)
            n = C.c_short()
            self.cli.SQLNumResultCols(st, C.byref(n))
            rows: List[List[Optional[str]]] = []
            buf, ind = C.create_string_buffer(8192), C.c_int64()
            while self.cli.SQLFetch(st) == 0:
                row: List[Optional[str]] = []
                for i in range(1, n.value + 1):
                    self.cli.SQLGetData(st, i, 1, buf, len(buf), C.byref(ind))
                    row.append(None if ind.value < 0 else buf.value.decode("utf-8", "replace"))
                rows.append(row)
            return rows
        finally:
            self.cli.SQLFreeHandle(3, st)

    def query_params(self, sql: str, params: List[str], timeout_s: int = 0) -> List[List[Optional[str]]]:
        """Prepared statement with parameter markers (values bound as UTF-16 text, typed by SQLDescribeParam), rows
        fetched as UTF-16 -> str. Used by VERIFY_TABLE (alelt_db2edge.verify): never builds SQL from values.
        SQLLEN/SQLULEN are passed as 64-bit values (correct for both the 32- and 64-bit CLI conventions on x64)."""
        st = C.c_void_p()
        self._ok(self.cli.SQLAllocHandle(3, self.dbc, C.byref(st)), "alloc stmt")
        keep: List[Any] = []
        try:
            if timeout_s:
                self.cli.SQLSetStmtAttr(st, 0, C.c_void_p(int(timeout_s)), 0)       # SQL_ATTR_QUERY_TIMEOUT
            self._ok(self.cli.SQLPrepare(st, sql.encode(), -3), "prepare", 3, st)
            for i, v in enumerate(params, 1):
                raw = str(v).encode("utf-16-le")
                buf = C.create_string_buffer(raw + b"\0\0", len(raw) + 2)
                ln = C.c_int64(len(raw))
                dt, ds, dd, dn = C.c_short(-9), C.c_uint64(0), C.c_short(0), C.c_short(0)    # SQL_WVARCHAR
                if self.cli.SQLDescribeParam(st, i, C.byref(dt), C.byref(ds), C.byref(dd), C.byref(dn)) not in (0, 1):
                    dt, ds, dd = C.c_short(-9), C.c_uint64(0), C.c_short(0)
                size = ds.value & 0xFFFFFFFF or max(1, len(raw) // 2)
                keep += [buf, ln]
                self._ok(self.cli.SQLBindParameter(st, i, 1, -8, dt, C.c_uint64(size), dd, buf,
                                                   C.c_int64(len(raw) + 2), C.byref(ln)), "bind", 3, st)
            self._ok(self.cli.SQLExecute(st), "execute", 3, st)
            n = C.c_short()
            self.cli.SQLNumResultCols(st, C.byref(n))
            rows: List[List[Optional[str]]] = []
            cap = 16384
            buf, ind = C.create_string_buffer(cap), C.c_int64()
            while True:
                rc = self.cli.SQLFetch(st)
                if rc == 100:
                    break
                self._ok(rc, "fetch", 3, st)
                row: List[Optional[str]] = []
                for i in range(1, n.value + 1):
                    parts: List[bytes] = []
                    null = False
                    while True:
                        ind.value = 0
                        rc = self.cli.SQLGetData(st, i, -8, buf, C.c_int64(cap), C.byref(ind))
                        if rc == 100:
                            break
                        self._ok(rc, "getdata", 3, st)
                        iv = ind.value
                        if iv in (-1, 0xFFFFFFFF):
                            null = True
                            break
                        if iv in (-4, 0xFFFFFFFC) or iv >= cap - 1 or rc == 1:
                            got = (cap - 2) & ~1
                        else:
                            got = iv & 0xFFFFFFFF
                        parts.append(buf.raw[:got])
                        if rc == 0:
                            break
                    row.append(None if null else b"".join(parts).decode("utf-16-le", "replace"))
                rows.append(row)
            return rows
        finally:
            self.cli.SQLFreeHandle(3, st)

    def commit(self) -> None:
        self.cli.SQLEndTran(2, self.dbc, 0)

    def _readlog(self, req: ReadLogStruct) -> int:
        ca = SQLCA()
        self.api.db2ReadLog(DB2VERSION1010, C.byref(req), C.byref(ca))
        if ca.sqlcode < 0:
            raise Db2Error(f"db2ReadLog: SQLCODE {ca.sqlcode} {ca.sqlerrmc[:ca.sqlerrml].decode(errors='replace')}")
        return int(ca.sqlcode)

    def query_lri(self) -> Tuple[str, str]:
        """QUERY -> (initialLRI, nextStartLRI). Caller must have committed."""
        info = ReadLogInfo()
        self._readlog(ReadLogStruct(iCallerAction=RL_QUERY, iFilterOption=RL_FILTER_ON, poReadLogInfo=C.pointer(info)))
        return info.initialLRI.text(), info.nextStartLRI.text()

    def read(self, start: str, buf_size: int) -> Tuple[bytes, int, str, int]:
        """One READ (filter ON) from `start` to the end of the log: (raw, recs, nextStartLRI, sqlcode)."""
        s, e, info = lri_struct(start), LRI(1, U64, U64), ReadLogInfo()
        buf = C.create_string_buffer(buf_size)
        req = ReadLogStruct(iCallerAction=RL_READ, piStartLRI=C.pointer(s), piEndLRI=C.pointer(e),
                            poLogBuffer=C.cast(buf, C.c_void_p), iLogBufferSize=buf_size,
                            iFilterOption=RL_FILTER_ON, poReadLogInfo=C.pointer(info))
        code = self._readlog(req)
        return buf.raw[:info.logBytesWritten], int(info.logRecsWritten), info.nextStartLRI.text(), code

    def probe(self, name: str) -> Dict[str, Any]:
        """Whitelisted probes only (R5)."""
        if name == "current_position":
            self.commit()
            return {"current": self.query_lri()[1]}
        if name == "db_health":
            ok = bool(self.query("SELECT 1 FROM SYSIBM.SYSDUMMY1 WITH UR"))
            self.commit()                        # no open unit of work before the next db2ReadLog (LNX-24)
            return {"ok": ok}
        if name == "log_config":
            rows = self.query("SELECT NAME, VALUE FROM SYSIBMADM.DBCFG WHERE NAME IN "
                              "('logarchmeth1','logfilsiz','logprimary','logsecond','cur_commit') WITH UR")
            self.commit()
            return {r[0] or "": r[1] for r in rows}
        raise ValueError(f"probe {name!r} not whitelisted")

    def server_info(self) -> Dict[str, Any]:
        """The server this connection really reached (an alias resolves through the node directory): host name
        from SYSIBMADM.ENV_SYS_INFO (read-only, committed). Used by the agents' deny-list re-check (LNX-08)."""
        try:
            rows = self.query("SELECT HOST_NAME FROM SYSIBMADM.ENV_SYS_INFO WITH UR")
        except Db2Error as e:
            return {"host": None, "error": str(e)}
        finally:
            try:
                self.commit()
            except Exception:  # noqa: BLE001
                pass
        return {"host": (rows[0][0] or "").strip() if rows and rows[0] else None}

    def close(self) -> None:
        self.commit()
        self.cli.SQLDisconnect(self.dbc)
        self.cli.SQLFreeHandle(2, self.dbc)
        self.cli.SQLFreeHandle(1, self.env)


def record_lris(raw: bytes, recs: int) -> List[str]:
    """LRI (from the filter header) of every record in a filter-ON buffer."""
    out, off = [], 0
    for _ in range(recs):
        if off + FILTER_HDR + LOG_HDR.size > len(raw):
            raise Db2Error("truncated db2ReadLog buffer")
        _, p1, p2 = struct.unpack_from("<QQQ", raw, off)
        out.append(f"{p1}:{p2:016X}")
        size = struct.unpack_from("<I", raw, off + FILTER_HDR)[0]
        off += FILTER_HDR + size
    return out


def make_batch(raw: bytes, recs: int, next_pos: str, batch_from: str, connection_id: str, agent_id: str,
               codec: str = "zlib") -> Tuple[Dict[str, Any], bytes, str]:
    """Wire batch for one READ result: (BatchHeader dict, body, to_pos). Body = raw records verbatim, `codec`
    applied (zlib falls back to none when not smaller); sha256/raw_bytes describe the uncompressed body."""
    lris = record_lris(raw, recs)
    body, used = raw, "none"
    if codec == "zlib":
        z = zlib.compress(raw, 6)
        if len(z) < len(raw):
            body, used = z, "zlib"
    hdr = {"batch_id": str(uuid.uuid4()), "connection_id": connection_id, "agent_id": agent_id, "engine": "db2",
           "from_pos": {"engine": "db2", "text": batch_from},
           "to_pos": {"engine": "db2", "text": lris[-1]}, "next_pos": {"engine": "db2", "text": next_pos},
           "rec_count": recs, "raw_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
           "codec": used, "protocol": 1}
    return hdr, body, lris[-1]

"""Binlog replication client: COM_REGISTER_SLAVE + COM_BINLOG_DUMP / COM_BINLOG_DUMP_GTID (public client/server
protocol documentation). PyMySQL (MIT) is used only for the connection handshake, authentication and packet I/O;
the replication commands and the event stream are handled here.
"""
from __future__ import annotations

import logging
import socket
import struct
from collections.abc import Iterator
from typing import Any

from .position import MARIADB, MYSQL, BinlogPosition, GtidSet, MariaGtidList

log = logging.getLogger("alelt_src_mysql.protocol")

COM_BINLOG_DUMP = 0x12
COM_REGISTER_SLAVE = 0x15
COM_BINLOG_DUMP_GTID = 0x1E

BINLOG_DUMP_NON_BLOCK = 0x01
BINLOG_SEND_ANNOTATE_ROWS_EVENT = 0x02      # MariaDB
BINLOG_THROUGH_GTID = 0x04                  # MySQL COM_BINLOG_DUMP_GTID
MARIA_SLAVE_CAPABILITY_GTID = 4


class BinlogProtocolError(RuntimeError):
    pass


class BinlogPurgedError(BinlogProtocolError):
    """The source no longer has the binlog the resume position needs (purged by retention): the capture runner stops
    the connection and opens a resync incident (alelt_common.spi.GAP_ERROR_NAMES, REQ-20) instead of retrying."""


_PURGED = ("purged", "could not find first log file", "could not find gtid state", "binary log is not open",
           "requires a gtid set", "could not find binlog")


def classify_dump_error(e: Exception) -> BinlogProtocolError:
    """ER_MASTER_FATAL_ERROR_READING_BINLOG (1236) about missing binlogs -> BinlogPurgedError, else protocol error."""
    code = e.args[0] if getattr(e, "args", None) else None
    msg = str(e.args[1] if len(getattr(e, "args", ())) > 1 else e).lower()
    if code == 1236 and any(k in msg for k in _PURGED):
        return BinlogPurgedError(f"binlog no longer available on the source: {e}")
    return BinlogProtocolError(f"binlog stream: {e}")


def ssl_param(settings: dict[str, Any]) -> Any:
    """TLS for the source sessions (SRC-my-10): `ssl_ca` = that CA with hostname check; `ssl` alone = the system CA
    store with hostname check; `ssl_insecure: true` = encryption without server verification (explicit opt-in)."""
    if settings.get("ssl_ca"):
        return {"ca": settings["ssl_ca"]}
    if settings.get("ssl_insecure"):
        return {"check_hostname": False}
    import ssl as _ssl
    return _ssl.create_default_context()


def connect(settings: dict[str, Any], password: str | None, *, read_timeout: float | None = 30.0,
            database: str | None = None) -> Any:
    """PyMySQL connection with explicit timeouts; session in UTC so TIMESTAMP values are unambiguous."""
    import pymysql

    ssl = None
    if settings.get("ssl") or settings.get("ssl_ca"):
        ssl = ssl_param(settings)
    return pymysql.connect(
        host=str(settings.get("host") or "127.0.0.1"), port=int(settings.get("port") or 3306),
        user=str(settings.get("user") or ""), password=password or "", database=database,
        charset="utf8mb4", autocommit=True, connect_timeout=int(settings.get("connect_timeout") or 10),
        read_timeout=read_timeout, write_timeout=int(settings.get("write_timeout") or 30), ssl=ssl,
        init_command="SET time_zone = '+00:00'")


def detect_flavor(conn: Any) -> str:
    with conn.cursor() as c:
        c.execute("SELECT @@version, @@version_comment")
        v, comment = c.fetchone()
    return MARIADB if "mariadb" in f"{v} {comment}".lower() else MYSQL


def current_position(conn: Any, flavor: str) -> BinlogPosition:
    """Current end of the binlog: GTID state + file:pos."""
    file = pos = None
    gtid_text = None
    with conn.cursor() as c:
        for stmt in (("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS") if flavor == MYSQL
                     else ("SHOW BINLOG STATUS", "SHOW MASTER STATUS")):
            try:
                c.execute(stmt)
            except Exception:  # noqa: BLE001 - older/newer server syntax
                continue
            row = c.fetchone()
            if row:
                cols = [d[0].lower() for d in c.description]
                r = dict(zip(cols, row))
                file, pos = r.get("file"), int(r.get("position") or 4)
                gtid_text = r.get("executed_gtid_set")
            break
        if flavor == MYSQL:
            c.execute("SELECT @@global.gtid_mode")
            mode = str(c.fetchone()[0]).upper()
            if mode != "ON":
                gtid_text = None
            elif gtid_text is None:
                c.execute("SELECT @@global.gtid_executed")
                gtid_text = c.fetchone()[0]
        else:
            c.execute("SELECT @@global.gtid_binlog_pos")
            gtid_text = c.fetchone()[0]
    if file is None:
        raise BinlogProtocolError("binary logging is disabled on the source (SHOW BINARY LOG STATUS is empty)")
    g: GtidSet | MariaGtidList | None
    if gtid_text is None:
        g = None
    elif flavor == MYSQL:
        g = GtidSet.parse(str(gtid_text))
    else:
        g = MariaGtidList.parse(str(gtid_text))
    return BinlogPosition(flavor, g, str(file), int(pos or 4))


class BinlogClient:
    """One replication session. Iterate to get raw events (19-byte header .. CRC32, no leading OK byte).

    non_blocking=True: the server sends EOF at the current end of the binlog (iteration ends).
    Otherwise the server keeps the stream open and sends heartbeats every `heartbeat_s` while idle; reads time
    out after `3 * heartbeat_s` (dead connection => BinlogProtocolError, the runner reconnects).
    """

    def __init__(self, settings: dict[str, Any], password: str | None, flavor: str, *, replica_server_id: int,
                 heartbeat_s: float = 5.0, annotate: bool = False) -> None:
        self.settings = settings
        self.password = password
        self.flavor = flavor
        self.replica_server_id = int(replica_server_id)
        self.heartbeat_s = heartbeat_s
        self.annotate = annotate
        self.conn: Any = None

    # ---------------------------------------------------------------- session
    def open(self, start: BinlogPosition, *, non_blocking: bool = False) -> None:
        self.conn = connect(self.settings, self.password, read_timeout=max(3 * self.heartbeat_s, 10.0))
        hb_ns = int(self.heartbeat_s * 1_000_000_000)
        with self.conn.cursor() as c:
            c.execute("SELECT @@global.binlog_checksum")
            ck = c.fetchone()[0]
            c.execute("SET @master_binlog_checksum = %s, @source_binlog_checksum = %s", (ck, ck))
            c.execute(f"SET @master_heartbeat_period = {hb_ns}, @source_heartbeat_period = {hb_ns}")
            if self.flavor == MARIADB:
                c.execute(f"SET @mariadb_slave_capability = {MARIA_SLAVE_CAPABILITY_GTID}")
                if start.gtid:                       # empty state would mean "from the oldest binlog"
                    c.execute("SET @slave_connect_state = %s", (start.gtid.text,))
                    c.execute("SET @slave_gtid_strict_mode = 1")
                    c.execute("SET @slave_gtid_ignore_duplicates = 0")
        self._register()
        flags = BINLOG_DUMP_NON_BLOCK if non_blocking else 0
        if self.flavor == MYSQL and isinstance(start.gtid, GtidSet) and (start.gtid or start.file is None):
            data = start.gtid.encode()
            fname = b""
            payload = (struct.pack("<HI", flags | BINLOG_THROUGH_GTID, self.replica_server_id)
                       + struct.pack("<I", len(fname)) + fname + struct.pack("<Q", 4)
                       + struct.pack("<I", len(data)) + data)
            self.conn._execute_command(COM_BINLOG_DUMP_GTID, payload)
        else:
            if self.flavor == MARIADB and start.gtid:
                fname, pos = b"", 4
            else:
                if start.file is None:
                    raise BinlogProtocolError("file:pos position needed when the source has no GTIDs")
                fname, pos = start.file.encode(), int(start.pos or 4)
            if self.flavor == MARIADB and self.annotate:
                flags |= BINLOG_SEND_ANNOTATE_ROWS_EVENT
            payload = struct.pack("<IHI", pos, flags, self.replica_server_id) + fname
            self.conn._execute_command(COM_BINLOG_DUMP, payload)
        log.info("binlog dump started", extra={"component": "alelt_src_mysql", "position": start.text})

    def _register(self) -> None:
        host = socket.gethostname().encode()[:60]
        user = b""
        pw = b""
        port = 0
        payload = (struct.pack("<I", self.replica_server_id) + bytes([len(host)]) + host + bytes([len(user)]) + user
                   + bytes([len(pw)]) + pw + struct.pack("<HII", port, 0, 0))
        self.conn._execute_command(COM_REGISTER_SLAVE, payload)
        self.conn._read_packet()

    def __iter__(self) -> Iterator[bytes]:
        import pymysql

        if self.conn is None:
            raise BinlogProtocolError("open() first")
        while True:
            try:
                pkt = self.conn._read_packet()
            except pymysql.err.MySQLError as e:
                raise classify_dump_error(e) from e
            data = pkt.get_all_data()
            if pkt.is_eof_packet():
                return
            if not data or data[0] != 0:
                raise BinlogProtocolError(f"unexpected packet 0x{data[:1].hex()}")
            yield bytes(data[1:])

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:  # noqa: BLE001 - already dead
                pass
            self.conn = None

    def __enter__(self) -> BinlogClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

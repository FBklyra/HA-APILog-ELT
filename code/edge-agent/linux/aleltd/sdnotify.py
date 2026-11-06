"""sd_notify(3) without libsystemd: one datagram per message to $NOTIFY_SOCKET (AF_UNIX, SOCK_DGRAM).

Silently does nothing when not started by systemd (no NOTIFY_SOCKET) or on platforms without AF_UNIX datagrams.
Supports abstract sockets ('@name'). Watchdog interval from WATCHDOG_USEC (honoured only when WATCHDOG_PID is
unset or ours).
"""
from __future__ import annotations

import logging
import os
import socket
import time
from typing import Optional

log = logging.getLogger("aleltd.sdnotify")


class Notifier:
    def __init__(self, env: Optional[dict] = None) -> None:
        e = os.environ if env is None else env
        self.addr: Optional[str] = e.get("NOTIFY_SOCKET") or None
        self._sock: Optional[socket.socket] = None
        self._broken = False
        usec = e.get("WATCHDOG_USEC")
        pid = e.get("WATCHDOG_PID")
        self.watchdog_s: Optional[float] = None
        if usec and usec.isdigit() and int(usec) > 0 and (not pid or pid == str(os.getpid())):
            self.watchdog_s = int(usec) / 1e6

    @property
    def enabled(self) -> bool:
        return bool(self.addr) and not self._broken

    def notify(self, *fields: str) -> bool:
        """Send 'KEY=VALUE' fields as one message. Returns True when the datagram was sent."""
        if not self.enabled or not hasattr(socket, "AF_UNIX"):
            return False
        addr = self.addr or ""
        target = "\0" + addr[1:] if addr.startswith("@") else addr
        msg = "\n".join(fields).encode()
        try:
            if self._sock is None:
                self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)   # type: ignore[attr-defined]
                self._sock.settimeout(1.0)
            self._sock.sendto(msg, target)
            return True
        except OSError as e:
            if not self._broken:
                log.debug("sd_notify unavailable: %s", e)
            self._broken = isinstance(e, (FileNotFoundError, ConnectionRefusedError)) or os.name == "nt"
            return False

    def ready(self, status: str = "") -> bool:
        return self.notify("READY=1", *([f"STATUS={status}"] if status else []))

    def status(self, status: str) -> bool:
        return self.notify(f"STATUS={status}")

    def stopping(self, status: str = "") -> bool:
        return self.notify("STOPPING=1", *([f"STATUS={status}"] if status else []))

    def reloading(self) -> bool:
        mono = int(time.clock_gettime(time.CLOCK_MONOTONIC) * 1e6) if hasattr(time, "clock_gettime") else 0
        return self.notify("RELOADING=1", f"MONOTONIC_USEC={mono}")

    def watchdog(self) -> bool:
        return self.notify("WATCHDOG=1")

    def extend_timeout(self, seconds: float) -> bool:
        return self.notify(f"EXTEND_TIMEOUT_USEC={int(seconds * 1e6)}")

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

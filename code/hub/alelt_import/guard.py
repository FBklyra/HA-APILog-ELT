"""Production safety guard: a deny list of production endpoints the hub (and the agents it configures) must never
contact. Entries (case-insensitive):

    db2prod.corp.local         exact host
    *.prod.corp.local          wildcard host
    10.20.0.15 / 10.20.0.0/16  IP / CIDR (host targets are resolved best-effort)
    db2prod.corp.local:50000   host + port
    xy12345.ca-central-1       account identifier (matches `<id>` and `<id>.<anything>` hosts, e.g. warehouse URLs)
    https://api.example.com    URL (its host is used)

Targets are every host-like value in a connection's settings (host, hostname, server, account, url, ...)."""
from __future__ import annotations

import fnmatch
import ipaddress
import re
import socket
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

HOST_KEYS = re.compile(r"^(host|hostname|server|address|account|account_identifier|url|endpoint|base_url|dsn|"
                       r"api_host|tunnel_host|ssh_host)$", re.I)
_ENTRY = re.compile(r"^[A-Za-z0-9*._:/\[\]-]{1,253}$")


class ProdTargetError(RuntimeError):
    """A requested endpoint matches the production deny list."""

    def __init__(self, target: str, entry: str) -> None:
        super().__init__(f"'{target}' matches the production deny list entry '{entry}': ALELT will not "
                         "connect to it. Use a test environment, or ask a PlatformAdmin to review the deny list.")
        self.target, self.entry = target, entry


def normalize_entry(e: str) -> str:
    e = e.strip().lower()
    if "://" in e:
        e = urlsplit(e).netloc or e
    e = e.rsplit("@", 1)[-1].rstrip("/")
    h, p = _split(e) if "/" not in e else (e, None)
    if "/" not in e and h:                   # canonical host: no trailing dot, IPv6 without brackets
        e = (f"[{h}]" if ":" in h else h) + (f":{p}" if p is not None else "")
    if not e or not _ENTRY.match(e):
        raise ValueError(f"invalid deny list entry {e!r}")
    return e


def _split(target: str) -> tuple[str, int | None]:
    """(host, port): lower case, trailing FQDN dot removed, `[v6]:port` and bare IPv6 understood (LNX-17)."""
    t = target.strip().lower()
    if "://" in t:
        u = urlsplit(t)
        try:
            port = u.port
        except ValueError:
            port = None
        return (u.hostname or "").rstrip("."), port
    t = t.rsplit("@", 1)[-1].split("/", 1)[0]
    if t.startswith("["):
        h, _, rest = t[1:].partition("]")
        p = rest[1:] if rest.startswith(":") else ""
        return h, int(p) if p.isdigit() else None
    if t.count(":") == 1:
        h, _, p = t.partition(":")
        return h.rstrip("."), int(p) if p.isdigit() else None
    return t.rstrip("."), None


@dataclass(frozen=True)
class DenyList:
    entries: tuple[str, ...]

    @classmethod
    def of(cls, entries: Iterable[str]) -> DenyList:
        return cls(tuple(sorted({normalize_entry(e) for e in entries if e and e.strip()})))

    def match(self, target: str, port: int | None = None, resolve: bool = True,
              fail_closed: bool = False) -> str | None:
        """The matching entry, or None. `fail_closed`: a host name that cannot be resolved while IP / CIDR entries
        exist counts as a match (the agents use this: an unknown address is never assumed safe)."""
        host, tport = _split(target)
        port = port if port is not None else tport
        if not host:
            return None
        ips: list[ipaddress._BaseAddress] = []
        try:
            ips = [ipaddress.ip_address(host)]
        except ValueError:
            if resolve and any(_is_ip(e) for e in self.entries):
                try:
                    ips = [ipaddress.ip_address(i[4][0]) for i in socket.getaddrinfo(host, None)]
                except (OSError, ValueError):
                    ips = []
                    if fail_closed:
                        return f"<{host}: not resolvable, refused (fail closed)>"
        for e in self.entries:
            eh, ep = _split(e) if "/" not in e else (e, None)
            if ep is not None and port is not None and ep != port:
                continue
            if "/" in eh or _is_ip(eh):
                try:
                    net = ipaddress.ip_network(eh, strict=False)
                except ValueError:
                    continue
                if any(ip in net for ip in ips):
                    return e
                continue
            if "*" in eh:
                if fnmatch.fnmatch(host, eh):
                    return e
            elif host == eh or host.startswith(eh + "."):
                return e
        return None

    def check_settings(self, settings: Mapping[str, Any]) -> None:
        """Raise ProdTargetError if any host-like value in `settings` (recursively) is denied."""
        port = settings.get("port")
        p = int(port) if isinstance(port, (int, str)) and str(port).isdigit() else None
        for target in host_values(settings):
            m = self.match(target, p)
            if m:
                raise ProdTargetError(target, m)


def _is_ip(e: str) -> bool:
    try:
        ipaddress.ip_network(e, strict=False)
        return True
    except ValueError:
        return False


def host_values(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            if isinstance(v, str) and HOST_KEYS.match(str(k)) and v.strip():
                out.append(v.strip())
            elif isinstance(v, (Mapping, list)):
                out += host_values(v)
    elif isinstance(obj, list):
        for v in obj:
            out += host_values(v)
    return out


ENVIRONMENTS = ("TEST", "PROD")

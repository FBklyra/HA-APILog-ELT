"""Production safety guard (REQ-12 / X-55), checked before any source connection, again after connecting (the
server the source really reached, LNX-08) and on SIGHUP.

- `deny_hosts` (same entry syntax as the hub deny list, alelt_import.guard): any host-like source setting, the
  database alias and DSN matching an entry => refuse (exit 78, systemd does not restart: RestartPreventExitStatus).
  Host names are normalised (case, trailing dot, `[v6]:port`); a host that cannot be resolved while IP / CIDR
  entries exist is refused (fail closed, LNX-17).
- `environment: PROD` => refuse unless `allow_prod: true` is set in the local, root-owned config (DBA decision).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List

_HOSTISH = re.compile(r"(host|server|addr|endpoint|dsn|url|account)", re.I)
_DBISH = ("database", "db", "dbname", "alias", "service_name", "sid")


class GuardError(RuntimeError):
    pass


def _deny(deny_hosts: List[str]) -> Any:
    try:
        from alelt_import.guard import DenyList
    except ImportError:
        raise GuardError("deny_hosts is set but alelt_import.guard is not importable: refusing (fail closed)") from None
    try:
        return DenyList.of(deny_hosts)
    except ValueError as e:
        raise GuardError(f"deny_hosts: {e}") from None


def _host_values(obj: Any, key: str = "") -> List[str]:
    out: List[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out += _host_values(v, str(k))
    elif isinstance(obj, list):
        for v in obj:
            out += _host_values(v, key)
    elif isinstance(obj, str) and obj.strip() and _HOSTISH.search(key) and not key.lower().endswith(("_env", "_file")):
        out.append(obj.strip())
    return out


def check(environment: str, allow_prod: bool, deny_hosts: List[str], targets: Dict[str, Any]) -> None:
    if environment == "PROD" and not allow_prod:
        raise GuardError("environment=PROD: refused - set \"allow_prod\": true in the agent config (DBA-owned) to "
                         "capture a production database")
    if not deny_hosts:
        return
    deny = _deny(deny_hosts)
    port = targets.get("port")
    p = int(port) if isinstance(port, (int, str)) and str(port).isdigit() else None
    for target in _host_values(targets):
        m = deny.match(target, p, resolve=True, fail_closed=True)
        if m:
            raise GuardError(f"source '{target}' matches the deny list entry '{m}': the agent will not connect to it")
    for k in _DBISH:
        v = targets.get(k)
        if isinstance(v, str) and v.strip():
            m = deny.match(v.strip(), resolve=False)
            if m:
                raise GuardError(f"source {k} '{v}' matches the deny list entry '{m}': the agent will not connect "
                                 "to it")


def check_connected(deny_hosts: List[str], server: Dict[str, Any]) -> None:
    """After connecting (LNX-08): the server the connection really reached (a Db2 alias goes through the node
    directory, so the configured name says nothing). `server` = {"host": ..., ...}; an empty / unknown answer is
    refused when a deny list is configured (fail closed)."""
    if not deny_hosts:
        return
    hosts = [h for h in _host_values(server) if h]
    if not hosts:
        raise GuardError("deny_hosts is set but the connected server could not be identified: refusing "
                         f"(fail closed; {server.get('error') or 'no host reported'})")
    deny = _deny(deny_hosts)
    for h in hosts:
        m = deny.match(h, None, resolve=True, fail_closed=True)
        if m:
            raise GuardError(f"connected server '{h}' matches the deny list entry '{m}': disconnected, the agent "
                             "will not capture it")

"""Production host -> test host mapping, a required step between an import and its apply.

An import describes the pipelines as they run in production; a parallel test must run against TEST copies. Before apply
every host-like value of every selected connection (source and destination settings: host, server, account, url, ...,
see alelt_import.guard.HOST_KEYS) must be mapped to the host of its test counterpart (identity only when the host is a
test host already). The mapped settings are then checked against the production deny list as before (strict: a
mapping never bypasses it)."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .guard import HOST_KEYS, DenyList, _split


def host_of(key: str, value: str) -> str:
    v = value.strip()
    if "://" in v:
        return (urlsplit(v).hostname or "").lower()
    return _split(v)[0].lower()


def hosts(settings: Any) -> list[str]:
    """Hosts referenced by one settings mapping (recursive), lower case, unique, in order."""
    out: list[str] = []

    def walk(o: Any) -> None:
        if isinstance(o, Mapping):
            for k, v in o.items():
                if isinstance(v, str) and HOST_KEYS.match(str(k)) and v.strip():
                    h = host_of(str(k), v)
                    if h and h not in out:
                        out.append(h)
                elif isinstance(v, (Mapping, list)):
                    walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(settings)
    return out


def normalize(mapping: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (mapping or {}).items():
        k2, v2 = str(k).strip().lower(), str(v).strip()
        if not k2 or not v2 or any(c in v2 for c in " /@?#"):
            raise ValueError(f"invalid host mapping {k!r} -> {v!r} (target must be a host name or address)")
        out[k2] = v2
    return out


def _replace(key: str, value: str, mapping: Mapping[str, str]) -> str:
    v = value.strip()
    h = host_of(key, v)
    if h not in mapping:
        return value
    new = mapping[h]
    if "://" in v:
        u = urlsplit(v)
        netloc = u.netloc
        userinfo, _, hp = netloc.rpartition("@")
        port = f":{u.port}" if u.port else ""
        netloc = (f"{userinfo}@" if userinfo else "") + new + port
        return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))
    rest = v.rsplit("@", 1)
    tail = rest[-1]
    host_part, sep, after = tail.partition("/")
    hp, colon, port = host_part.partition(":") if host_part.count(":") == 1 else (host_part, "", "")
    out = new + (colon + port if colon else "") + (sep + after if sep else "")
    return (rest[0] + "@" + out) if len(rest) == 2 else out


def apply(settings: Any, mapping: Mapping[str, str]) -> Any:
    """Copy of `settings` with every mapped host replaced (ports, paths, credentials kept)."""
    if isinstance(settings, Mapping):
        out: dict[str, Any] = {}
        for k, v in settings.items():
            if isinstance(v, str) and HOST_KEYS.match(str(k)) and v.strip():
                out[k] = _replace(str(k), v, mapping)
            else:
                out[k] = apply(v, mapping) if isinstance(v, (Mapping, list)) else v
        return out
    if isinstance(settings, list):
        return [apply(v, mapping) for v in settings]
    return settings


def required(conns: Iterable[Any]) -> list[dict[str, Any]]:
    """[{host, connections: [...], sides: [...]}] that need a mapping for these plan connections."""
    acc: dict[str, dict[str, Any]] = {}
    for pc in conns:
        for side in ("source", "destination"):
            for h in hosts((getattr(pc, side) or {}).get("settings") or {}):
                e = acc.setdefault(h, {"host": h, "connections": [], "sides": []})
                if pc.connection_id not in e["connections"]:
                    e["connections"].append(pc.connection_id)
                if side not in e["sides"]:
                    e["sides"].append(side)
    return [acc[h] for h in sorted(acc)]


def check(conns: Iterable[Any], mapping: Mapping[str, str], deny: DenyList) -> tuple[list[str], list[str]]:
    """(unmapped hosts, mapping targets that are on the deny list)."""
    conns = list(conns)
    need = [e["host"] for e in required(conns)]
    unmapped = [h for h in need if h not in mapping]
    denied = sorted({t for h, t in mapping.items() if h in need and deny.match(t)})
    return unmapped, denied

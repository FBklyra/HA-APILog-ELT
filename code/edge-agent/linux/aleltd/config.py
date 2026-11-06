"""Agent configuration: the `aleltagent.json` keys of the Windows service, validated strictly (stdlib only).

Unknown keys are rejected (keys starting with `_` are comments). Windows-only keys (`log.dir`, `log.max_mb`,
`log.keep`, `source.plugin`, `hub.client_cert_thumbprint`, `hub.websocket`) are accepted and ignored so one
hub-generated file works on both platforms. Linux additions: `hub.token_env`, `hub.client_cert_file`,
`hub.client_key_file`, `hub.ca_file`, `source.connector`, `source.password_env`, `source.password_file`,
`source.user_env`, `capture.start|batch_ms|max_txn_mb|catalog_s`, `log.timestamps`, `shutdown`, `allow_prod`.
"""
from __future__ import annotations

import ipaddress
import json
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Optional

LIMIT_KEYS = ("cpu_pct", "mem_mb", "batch_kb", "max_rate_kbps", "poll_ms")
DEFAULT_LIMITS = {"cpu_pct": 100, "mem_mb": 512, "batch_kb": 1024, "max_rate_kbps": 0, "poll_ms": 1000}
DEFAULT_LIMITS_MAX = {"cpu_pct": 100, "mem_mb": 1024, "batch_kb": 8192, "max_rate_kbps": 0, "poll_ms": 600000}
LIMIT_MIN = {"cpu_pct": 1, "mem_mb": 64, "batch_kb": 16, "max_rate_kbps": 0, "poll_ms": 50}
ENGINES = ("db2", "postgres", "mysql", "mariadb", "sqlserver", "oracle", "fake")
# LNX-22: secrets never inline in source.settings (R9) - use source.password_env / password_file or *_env / *_file
_SECRET_KEY = re.compile(r"(passw|pwd|secret|token|api_?key|private_?key|credential)", re.I)
_SECRET_OK_SUFFIX = ("_env", "_file", "_path")


def _is_loopback(host: str) -> bool:
    h = (host or "").strip("[]").lower()
    if h in ("localhost", "localhost.", "ip6-localhost"):
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


def _inline_secrets(settings: Any, where: str = "source.settings") -> List[str]:
    out: List[str] = []
    if isinstance(settings, dict):
        for k, v in settings.items():
            ks = str(k)
            if isinstance(v, (dict, list)):
                out += _inline_secrets(v, f"{where}.{ks}")
            elif _SECRET_KEY.search(ks) and not ks.lower().endswith(_SECRET_OK_SUFFIX) and v not in (None, ""):
                out.append(f"{where}.{ks}")
    elif isinstance(settings, list):
        for i, v in enumerate(settings):
            out += _inline_secrets(v, f"{where}[{i}]")
    return out


class ConfigError(ValueError):
    pass


def valid_allow_entry(entry: str) -> bool:
    """Allow-list entry syntax, identical in the three agents (docs/AGENT-COMMANDS.md): SCHEMA.TABLE or SCHEMA.*."""
    s, dot, t = str(entry).strip().partition(".")
    return bool(dot) and bool(s) and bool(t) and "*" not in s and (t == "*" or "*" not in t)


def table_allowed(allowed: List[str], schema: str, table: str) -> bool:
    """Exact SCHEMA.TABLE (case-insensitive) or SCHEMA.* (every base table of the schema); same vectors as the dev
    edge (alelt_db2edge.table_allowed) and the C agent (alelt_config_table_allowed): edge-agent/shared/allowlist_vectors.json.
    """
    fq = f"{schema}.{table}".upper()
    for a in allowed:
        a = str(a).strip().upper()
        if not valid_allow_entry(a):
            continue
        if a == fq or (a.endswith(".*") and a[:-2] == schema.upper()):
            return True
    return False


def _obj(d: Any, where: str, allowed: tuple, ignored: tuple = ()) -> Dict[str, Any]:
    if d is None:
        return {}
    if not isinstance(d, dict):
        raise ConfigError(f"{where}: must be an object")
    for k in d:
        if k.startswith("_") or k in allowed or k in ignored:
            continue
        raise ConfigError(f"{where}.{k}: unknown key")
    return {k: v for k, v in d.items() if k in allowed}


def _num(d: Dict[str, Any], k: str, default: float, lo: float, hi: float, where: str, integer: bool = True) -> Any:
    v = d.get(k, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)) or (integer and int(v) != v):
        raise ConfigError(f"{where}.{k}: must be {'an integer' if integer else 'a number'}")
    if not lo <= v <= hi:
        raise ConfigError(f"{where}.{k}: {v} out of range [{lo}, {hi}]")
    return int(v) if integer else float(v)


def _str(d: Dict[str, Any], k: str, default: Optional[str], where: str, required: bool = False) -> Optional[str]:
    v = d.get(k, default)
    if v is None:
        if required:
            raise ConfigError(f"{where}.{k}: required")
        return None
    if not isinstance(v, str) or (required and not v.strip()):
        raise ConfigError(f"{where}.{k}: must be a non-empty string")
    return v


def _bool(d: Dict[str, Any], k: str, default: bool, where: str) -> bool:
    v = d.get(k, default)
    if not isinstance(v, bool):
        raise ConfigError(f"{where}.{k}: must be true/false")
    return v


def _path(v: str, base: Path) -> str:
    p = Path(os.path.expanduser(os.path.expandvars(v)))
    return str(p if p.is_absolute() else (base / p))


def default_state_dir() -> str:
    sd = os.environ.get("STATE_DIRECTORY", "").split(":")[0]      # systemd StateDirectory=
    return sd or "/var/lib/alelt"


@dataclass
class HubCfg:
    url: str
    mode: str = "mtls"                    # mtls | token | dev
    token_env: str = "ALELT_AGENT_TOKEN"
    heartbeat_s: int = 15
    command_wait_s: int = 10
    connect_timeout_ms: int = 10000
    request_timeout_ms: int = 60000
    backoff_max_s: int = 300
    client_cert_file: Optional[str] = None
    client_key_file: Optional[str] = None
    ca_file: Optional[str] = None
    server_pins_sha256: List[str] = field(default_factory=list)
    allow_insecure_http: bool = False     # LNX-09: plain http only for dev mode / loopback / this explicit opt-in


@dataclass
class GovernorCfg:
    enabled: bool = True
    interval_s: int = 15
    host_cpu_pause_pct: int = 85
    host_cpu_resume_pct: int = 70
    sustain_samples: int = 3
    max_log_util_pct: int = 90


@dataclass
class SourceCfg:
    engine: str
    database: str = ""
    settings: Dict[str, Any] = field(default_factory=dict)
    dll_dirs: List[str] = field(default_factory=list)     # Db2 client library dirs (Linux: <sqllib>/lib64)
    connector: Optional[str] = None                       # 'module:Class' override for remote engines
    user_env: Optional[str] = None
    password_env: Optional[str] = None
    password_file: Optional[str] = None                   # relative => $CREDENTIALS_DIRECTORY (LoadCredential=)

    def password(self) -> Optional[str]:
        if self.password_env and os.environ.get(self.password_env):
            return os.environ[self.password_env]
        if self.password_file:
            p = Path(self.password_file)
            if not p.is_absolute():
                p = Path(os.environ.get("CREDENTIALS_DIRECTORY", ".")) / p
            return p.read_text(encoding="utf-8").strip()
        return None


@dataclass
class CaptureCfg:
    autostart: bool = False
    start: str = "current"            # first start only: current | oldest | <engine position text>
    batch_ms: int = 1000              # remote engines: soft batch cut by time
    max_txn_mb: int = 1024            # remote engines: one transaction held in memory until its commit
    catalog_s: int = 300              # remote engines: catalog refresh


@dataclass
class HaCfg:
    mode: str = "standalone"          # standalone | active_passive
    lease_ttl_s: int = 30
    renew_s: int = 10


@dataclass
class ShutdownCfg:
    deadline_s: int = 45              # whole graceful stop (keep below systemd TimeoutStopSec)
    drain_s: int = 10                 # part of it spent shipping the spool before releasing the lease


@dataclass
class Config:
    agent_id: str
    connection_id: str
    hub: HubCfg
    source: SourceCfg
    spool_dir: str
    spool_max_mb: int = 2048
    log_level: str = "INFO"
    log_timestamps: str = "auto"      # auto (omit under journald) | always | never
    limits: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_LIMITS))
    limits_max: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_LIMITS_MAX))
    require_job_limits: bool = False
    governor: GovernorCfg = field(default_factory=GovernorCfg)
    allowed_tables: List[str] = field(default_factory=list)
    max_chunk_rows: int = 5000
    capture: CaptureCfg = field(default_factory=CaptureCfg)
    ha: HaCfg = field(default_factory=HaCfg)
    deny_hosts: List[str] = field(default_factory=list)
    environment: str = "TEST"
    allow_prod: bool = False
    shutdown: ShutdownCfg = field(default_factory=ShutdownCfg)
    path: Optional[str] = None

    @property
    def state_file(self) -> str:
        return os.path.join(self.spool_dir, "state.json")

    @property
    def credential_file(self) -> str:
        """REQ-26: per-agent credential (`alelta_`) issued at registration, 0600, next to state.json."""
        return os.path.join(self.spool_dir, "credential.json")

    def clamp(self, lim: Dict[str, Any]) -> Dict[str, int]:
        """Limits clamped to limits_max (DBA ceilings); 0 in limits_max = no ceiling for rate."""
        out = dict(self.limits)
        for k, v in lim.items():
            if k not in LIMIT_KEYS or isinstance(v, bool) or not isinstance(v, (int, float)):
                continue
            v = max(LIMIT_MIN[k], int(v))
            mx = self.limits_max.get(k, 0)
            if k == "max_rate_kbps":                 # 0 = unlimited (the only key where 0 means "no ceiling")
                v = min(v, mx) if mx and v else (mx or v)
            else:                                    # LNX-23: always a ceiling (>= LIMIT_MIN, checked at parse)
                v = min(v, mx)
            out[k] = v
        return out


def parse(d: Dict[str, Any], base: Optional[Path] = None, path: Optional[str] = None) -> Config:
    base = base or Path.cwd()
    top = _obj(d, "config", ("agent_id", "connection_id", "hub", "spool", "log", "limits", "limits_max",
                             "require_job_limits", "governor", "source", "snapshot", "capture", "ha", "deny_hosts",
                             "environment", "allow_prod", "shutdown"))
    agent_id = _str(top, "agent_id", None, "config", True)
    cid = _str(top, "connection_id", None, "config", True)
    for k, v in (("agent_id", agent_id), ("connection_id", cid)):
        if len(v or "") > 100 or not all(c.isalnum() or c in "-_.:" for c in v or ""):
            raise ConfigError(f"config.{k}: letters, digits, - _ . : only (max 100)")

    h = _obj(top.get("hub"), "hub", ("url", "mode", "token_env", "dev_token_env", "heartbeat_s", "command_wait_s",
                                     "connect_timeout_ms", "request_timeout_ms", "backoff_max_s", "client_cert_file",
                                     "client_key_file", "ca_file", "server_pins_sha256", "allow_insecure_http"),
             ("client_cert_thumbprint", "websocket"))
    url = _str(h, "url", None, "hub", True) or ""
    if not url.startswith(("http://", "https://")):
        raise ConfigError("hub.url: must be http(s)://")
    mode = _str(h, "mode", "mtls", "hub") or "mtls"
    if mode not in ("mtls", "token", "dev"):
        raise ConfigError("hub.mode: mtls | token | dev")
    pins = h.get("server_pins_sha256", [])
    if not isinstance(pins, list) or not all(isinstance(p, str) and len(p) == 64 for p in pins):
        raise ConfigError("hub.server_pins_sha256: list of 64-hex SHA-256 values")
    hub = HubCfg(url=url.rstrip("/"), mode=mode,
                 token_env=_str(h, "token_env", None, "hub") or _str(h, "dev_token_env", "ALELT_AGENT_TOKEN", "hub")
                 or "ALELT_AGENT_TOKEN",
                 heartbeat_s=_num(h, "heartbeat_s", 15, 1, 3600, "hub"),
                 command_wait_s=_num(h, "command_wait_s", 10, 0, 60, "hub"),
                 connect_timeout_ms=_num(h, "connect_timeout_ms", 10000, 100, 300000, "hub"),
                 request_timeout_ms=_num(h, "request_timeout_ms", 60000, 1000, 600000, "hub"),
                 backoff_max_s=_num(h, "backoff_max_s", 300, 1, 3600, "hub"),
                 client_cert_file=_str(h, "client_cert_file", None, "hub"),
                 client_key_file=_str(h, "client_key_file", None, "hub"),
                 ca_file=_str(h, "ca_file", None, "hub"), server_pins_sha256=[p.lower() for p in pins],
                 allow_insecure_http=_bool(h, "allow_insecure_http", False, "hub"))
    if mode == "mtls" and not url.startswith("https://"):
        raise ConfigError("hub.mode=mtls requires an https:// hub.url")
    if url.startswith("http://") and mode != "dev" and not hub.allow_insecure_http:
        from urllib.parse import urlsplit
        if not _is_loopback(urlsplit(url).hostname or ""):
            raise ConfigError("hub.url: http:// sends the agent token in clear - use https:// (or a loopback hub, "
                              "hub.mode=dev, or hub.allow_insecure_http=true for an isolated lab)")
    if mode == "mtls" and not (hub.client_cert_file and hub.client_key_file):
        raise ConfigError("hub.mode=mtls requires hub.client_cert_file and hub.client_key_file")
    for k in ("client_cert_file", "client_key_file", "ca_file"):
        if getattr(hub, k):
            setattr(hub, k, _path(getattr(hub, k), base))

    sp = _obj(top.get("spool"), "spool", ("dir", "max_mb"))
    sdir = sp.get("dir") or os.path.join(default_state_dir(), "spool")
    if not isinstance(sdir, str):
        raise ConfigError("spool.dir: must be a string")
    if "%" in sdir and os.name != "nt":                # Windows default (%ProgramData%...) in a shared file
        sdir = os.path.join(default_state_dir(), "spool")

    lg = _obj(top.get("log"), "log", ("level", "timestamps"), ("dir", "max_mb", "keep"))
    level = (_str(lg, "level", "INFO", "log") or "INFO").upper()
    if level not in ("DEBUG", "INFO", "WARNING", "WARN", "ERROR"):
        raise ConfigError("log.level: DEBUG | INFO | WARNING | ERROR")
    ts = _str(lg, "timestamps", "auto", "log") or "auto"
    if ts not in ("auto", "always", "never"):
        raise ConfigError("log.timestamps: auto | always | never")

    lim_max = dict(DEFAULT_LIMITS_MAX)
    lm = _obj(top.get("limits_max"), "limits_max", LIMIT_KEYS)
    for k in LIMIT_KEYS:
        lim_max[k] = _num(lm, k, lim_max[k], 0 if k == "max_rate_kbps" else LIMIT_MIN[k], 10 ** 9, "limits_max")
    lim = dict(DEFAULT_LIMITS)
    li = _obj(top.get("limits"), "limits", LIMIT_KEYS)
    for k in LIMIT_KEYS:
        lim[k] = _num(li, k, lim[k], 0, 10 ** 9, "limits")

    g = _obj(top.get("governor"), "governor", ("enabled", "interval_s", "host_cpu_pause_pct", "host_cpu_resume_pct",
                                               "sustain_samples", "max_log_util_pct"))
    gov = GovernorCfg(enabled=_bool(g, "enabled", True, "governor"),
                      interval_s=_num(g, "interval_s", 15, 1, 3600, "governor"),
                      host_cpu_pause_pct=_num(g, "host_cpu_pause_pct", 85, 1, 100, "governor"),
                      host_cpu_resume_pct=_num(g, "host_cpu_resume_pct", 70, 0, 100, "governor"),
                      sustain_samples=_num(g, "sustain_samples", 3, 1, 100, "governor"),
                      max_log_util_pct=_num(g, "max_log_util_pct", 90, 1, 100, "governor"))
    if gov.host_cpu_resume_pct >= gov.host_cpu_pause_pct:
        raise ConfigError("governor.host_cpu_resume_pct must be below host_cpu_pause_pct")

    s = _obj(top.get("source"), "source", ("engine", "database", "settings", "dll_dirs", "connector", "user_env",
                                           "password_env", "password_file"), ("plugin",))
    if not s:
        raise ConfigError("source: required")
    eng = _str(s, "engine", None, "source", True) or ""
    if eng not in ENGINES and not s.get("connector"):
        raise ConfigError(f"source.engine: one of {', '.join(ENGINES)} (or set source.connector)")
    settings = s.get("settings") or {}
    dll = s.get("dll_dirs") or []
    if not isinstance(settings, dict) or not isinstance(dll, list):
        raise ConfigError("source.settings must be an object, source.dll_dirs a list")
    bad = _inline_secrets(settings)
    if bad:
        raise ConfigError(f"{', '.join(bad)}: secrets must not be inline in the config (R9) - use "
                          "source.password_env / source.password_file (or a *_env / *_file setting)")
    src = SourceCfg(engine=eng, database=_str(s, "database", "", "source") or str(settings.get("database") or ""),
                    settings=dict(settings), dll_dirs=[os.path.expandvars(str(x)) for x in dll],
                    connector=_str(s, "connector", None, "source"), user_env=_str(s, "user_env", None, "source"),
                    password_env=_str(s, "password_env", None, "source"),
                    password_file=_str(s, "password_file", None, "source"))
    if eng == "db2" and not src.database:
        raise ConfigError("source.database: required for db2")

    sn = _obj(top.get("snapshot"), "snapshot", ("allowed_tables", "max_chunk_rows"))
    allowed = sn.get("allowed_tables") or []
    if not isinstance(allowed, list) or not all(isinstance(t, str) and valid_allow_entry(t) for t in allowed):
        raise ConfigError("snapshot.allowed_tables: list of 'SCHEMA.TABLE' or 'SCHEMA.*' (a schema wildcard such as "
                          "'*.*' is refused)")

    c = _obj(top.get("capture"), "capture", ("autostart", "start", "batch_ms", "max_txn_mb", "catalog_s"))
    cap = CaptureCfg(autostart=_bool(c, "autostart", False, "capture"),
                     start=_str(c, "start", "current", "capture") or "current",
                     batch_ms=_num(c, "batch_ms", 1000, 10, 600000, "capture"),
                     max_txn_mb=_num(c, "max_txn_mb", 1024, 1, 65536, "capture"),
                     catalog_s=_num(c, "catalog_s", 300, 10, 86400, "capture"))

    ha_d = _obj(top.get("ha"), "ha", ("mode", "lease_ttl_s", "renew_s"))
    ha_mode = _str(ha_d, "mode", "standalone", "ha") or "standalone"
    if ha_mode not in ("standalone", "active_passive"):
        raise ConfigError("ha.mode: standalone | active_passive")
    ttl = _num(ha_d, "lease_ttl_s", 30, 5, 300, "ha")
    renew = _num(ha_d, "renew_s", 10 if 10 <= ttl // 2 else max(1, ttl // 3), 1, 300, "ha")
    if renew > ttl // 2:
        raise ConfigError("ha.renew_s must be <= lease_ttl_s / 2")

    deny = top.get("deny_hosts", [])
    if not isinstance(deny, list) or not all(isinstance(x, str) for x in deny):
        raise ConfigError("deny_hosts: list of strings")
    env = (_str(top, "environment", "TEST", "config") or "TEST").upper()
    if env not in ("TEST", "PROD"):
        raise ConfigError("environment: TEST | PROD")

    sh = _obj(top.get("shutdown"), "shutdown", ("deadline_s", "drain_s"))
    shut = ShutdownCfg(deadline_s=_num(sh, "deadline_s", 45, 5, 3600, "shutdown"),
                       drain_s=_num(sh, "drain_s", 10, 0, 3600, "shutdown"))
    if shut.drain_s >= shut.deadline_s:
        raise ConfigError("shutdown.drain_s must be below shutdown.deadline_s")

    cfg = Config(agent_id=agent_id or "", connection_id=cid or "", hub=hub, source=src,
                 spool_dir=_path(sdir, base), spool_max_mb=_num(sp, "max_mb", 2048, 1, 10 ** 7, "spool"),
                 log_level="WARNING" if level == "WARN" else level, log_timestamps=ts,
                 limits_max=lim_max, require_job_limits=_bool(top, "require_job_limits", False, "config"),
                 governor=gov, allowed_tables=[t.upper() for t in allowed],
                 max_chunk_rows=_num(sn, "max_chunk_rows", 5000, 1, 10 ** 6, "snapshot"), capture=cap,
                 ha=HaCfg(ha_mode, ttl, renew), deny_hosts=list(deny), environment=env,
                 allow_prod=_bool(top, "allow_prod", False, "config"), shutdown=shut, path=path)
    cfg.limits = dict(DEFAULT_LIMITS)
    cfg.limits = cfg.clamp(lim)
    if cfg.source.engine == "fake" and cfg.environment == "PROD":
        raise ConfigError("source.engine=fake is a test source; refused with environment=PROD")
    return cfg


def load(path: str) -> Config:
    p = Path(path)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except OSError as e:
        raise ConfigError(f"cannot read {path}: {e.strerror}") from None
    except json.JSONDecodeError as e:
        raise ConfigError(f"{path}: invalid JSON ({e})") from None
    return parse(d, p.resolve().parent, str(p))


# Keys that SIGHUP may change at runtime; anything else needs a restart.
RELOADABLE = ("limits", "limits_max", "log_level", "log_timestamps", "governor", "deny_hosts", "spool_max_mb",
              "allowed_tables", "max_chunk_rows")


def diff_restart_required(old: Config, new: Config) -> List[str]:
    keys = [k for k in old.__dataclass_fields__ if k not in RELOADABLE and k != "path"]
    return [k for k in keys if getattr(old, k) != getattr(new, k)]


def merged(old: Config, new: Config) -> Config:
    return replace(old, **{k: getattr(new, k) for k in RELOADABLE})

"""Hub settings from environment (ALELT_*). Secrets come from the environment / secret store only (R9)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]


def _list(name: str) -> list[str]:
    return [x.strip() for x in os.environ.get(name, "").split(",") if x.strip()]


def _json(name: str, default: dict[str, str]) -> dict[str, str]:
    raw = os.environ.get(name)
    return json.loads(raw) if raw else default


DEV_ENVS = frozenset({"dev", "local", "test"})


def _retention(raw: str) -> dict[str, float]:
    raw = raw.strip()
    if not raw:
        return {}
    if raw.startswith("{"):
        return {str(k): float(v) for k, v in json.loads(raw).items()}
    return {"*": float(raw)}


@dataclass(frozen=True)
class Settings:
    db_url: str = field(default_factory=lambda: os.environ.get(
        "ALELT_DB_URL", "postgresql+psycopg://localhost:5432/alelt"))
    spool_dir: Path = field(default_factory=lambda: Path(os.environ.get("ALELT_SPOOL_DIR", str(_REPO / ".alelt" / "hub-spool"))))
    max_body_mb: int = field(default_factory=lambda: int(os.environ.get("ALELT_MAX_BODY_MB", "64")))
    max_raw_mb: int = field(default_factory=lambda: int(os.environ.get("ALELT_MAX_RAW_MB", "512")))   # inflated cap
    # agent identity: 'token' (dev shared token) or 'mtls' (thumbprint allow-list via TLS proxy header)
    agent_auth: str = field(default_factory=lambda: os.environ.get("ALELT_AGENT_AUTH", "token"))
    agent_tokens: list[str] = field(default_factory=lambda: _list("ALELT_AGENT_TOKENS"))
    agent_thumbprints: list[str] = field(default_factory=lambda: [t.lower() for t in _list("ALELT_AGENT_THUMBPRINTS")])
    # REQ-3/X-18: TLS terminates at nginx, which forwards the verified client-cert fingerprint
    thumbprint_header: str = field(default_factory=lambda: os.environ.get("ALELT_THUMBPRINT_HEADER", "X-SSL-Client-Fingerprint"))
    # F-14: TCP peers allowed to assert the client-certificate header (the TLS terminator): IPs / CIDRs.
    # Default: ALELT_FORWARDED_ALLOW_IPS when it is not '*', else loopback only.
    trusted_proxies: list[str] = field(default_factory=lambda: _list("ALELT_TRUSTED_PROXIES") or [
        x for x in _list("ALELT_FORWARDED_ALLOW_IPS") if x != "*"] or ["127.0.0.1", "::1"])
    # per-agent credentials (enrollment -> alelta_ credential, rotation; F-14)
    agent_credential_ttl_days: int = field(default_factory=lambda: int(os.environ.get("ALELT_AGENT_CREDENTIAL_TTL_DAYS", "90")))
    # AHS-04: a claimed enrolment token stops working this long after its claim (0 = never; dev default)
    enrollment_claimed_ttl_h: float = field(default_factory=lambda: float(os.environ.get(
        "ALELT_ENROLLMENT_CLAIMED_TTL_HOURS", "0" if os.environ.get("ALELT_ENV", "prod") in DEV_ENVS else "24")))
    forwarded_allow_ips: list[str] = field(default_factory=lambda: _list("ALELT_FORWARDED_ALLOW_IPS") or ["127.0.0.1"])
    public_url: str = field(default_factory=lambda: os.environ.get("ALELT_PUBLIC_URL", ""))
    agent_credential_grace_s: int = field(default_factory=lambda: int(os.environ.get("ALELT_AGENT_CREDENTIAL_GRACE_S", "300")))
    # console API hardening: allowed browser origins (CORS; empty = same-origin only) and per-client rate limits
    cors_origins: list[str] = field(default_factory=lambda: _list("ALELT_CORS_ORIGINS"))
    rate_limit_per_min: int = field(default_factory=lambda: int(os.environ.get("ALELT_RATE_LIMIT_PER_MIN", "600")))
    auth_fail_limit_per_min: int = field(default_factory=lambda: int(os.environ.get("ALELT_AUTH_FAIL_LIMIT_PER_MIN", "30")))
    # X-129: DBA-owned archive-log retention per connection {"conn": hours} (or one number for all); 0 = unknown
    archive_retention_h: dict[str, float] = field(default_factory=lambda: _retention(os.environ.get("ALELT_ARCHIVE_RETENTION_HOURS", "")))
    archive_horizon_warn_ratio: float = field(default_factory=lambda: float(os.environ.get("ALELT_ARCHIVE_HORIZON_WARN_RATIO", "0.5")))
    # console auth: Entra ID OIDC JWT; dev role header (X-ALELT-Dev-Role) only if ALELT_DEV_AUTH=1 / ALELT_AUTH_MODE=dev (X-17)
    env: str = field(default_factory=lambda: os.environ.get("ALELT_ENV", "prod"))
    auth_mode: str = field(default_factory=lambda: "dev" if os.environ.get("ALELT_DEV_AUTH") == "1"
                           else os.environ.get("ALELT_AUTH_MODE", "oidc"))
    oidc_issuer: str = field(default_factory=lambda: os.environ.get("ALELT_OIDC_ISSUER", ""))
    oidc_audience: str = field(default_factory=lambda: os.environ.get("ALELT_OIDC_AUDIENCE", ""))
    oidc_jwks_url: str = field(default_factory=lambda: os.environ.get("ALELT_OIDC_JWKS_URL", ""))
    # backpressure: refuse new batches while this many are not yet loaded
    max_unloaded_batches: int = field(default_factory=lambda: int(os.environ.get("ALELT_MAX_UNLOADED_BATCHES", "5000")))
    # plugins
    sources: dict[str, str] = field(default_factory=lambda: _json(
        "ALELT_SOURCES", {"db2": "alelt_src_db2.connector:Db2Source",
                      "postgres": "alelt_src_postgres.connector:PostgresSource",
                      "oracle": "alelt_src_oracle.connector:OracleSource",
                      "sqlserver": "alelt_src_sqlserver.connector:SqlServerSource",
                      "mysql": "alelt_src_mysql.connector:MySQLSource",
                      "mariadb": "alelt_src_mariadb.connector:MariaDBSource"}))
    destination: str = field(default_factory=lambda: os.environ.get(
        "ALELT_DESTINATION", "alelt_dst_postgres.connector:PostgresDestination"))
    dest_url: str = field(default_factory=lambda: os.environ.get("ALELT_DEST_URL", ""))
    tables_config: str | None = field(default_factory=lambda: os.environ.get("ALELT_TABLES_CONFIG"))
    # loader / txn knobs
    load_max_rows: int = field(default_factory=lambda: int(os.environ.get("ALELT_LOAD_MAX_ROWS", "50000")))
    load_max_latency_s: float = field(default_factory=lambda: float(os.environ.get("ALELT_LOAD_MAX_LATENCY_S", "0")))
    txn_byte_budget: int = field(default_factory=lambda: int(os.environ.get("ALELT_TXN_BYTE_BUDGET", str(64 << 20))))
    long_txn_seconds: float = field(default_factory=lambda: float(os.environ.get("ALELT_LONG_TXN_SECONDS", "300")))
    latency_target_s: float = field(default_factory=lambda: float(os.environ.get("ALELT_LATENCY_TARGET_S", "120")))
    worker_metrics_port: int = field(default_factory=lambda: int(os.environ.get("ALELT_WORKER_METRICS_PORT", "9100")))
    catchup_lookback: int = field(default_factory=lambda: int(os.environ.get("ALELT_CATCHUP_LOOKBACK_BATCHES", "1000")))
    worker_alive_file: str = field(default_factory=lambda: os.environ.get("ALELT_WORKER_ALIVE_FILE", ""))
    log_level: str = field(default_factory=lambda: os.environ.get("ALELT_LOG_LEVEL", "INFO"))
    # production safety guard: hosts/IPs/CIDRs/host:port/account ids the hub and its agents must never contact
    # (merged with the list PlatformAdmins edit in the console; alelt_import.guard)
    prod_denylist: list[str] = field(default_factory=lambda: _list("ALELT_PROD_DENYLIST"))


    def is_trusted_proxy(self, host: str) -> bool:
        import ipaddress

        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return False
        for entry in self.trusted_proxies:
            try:
                if ip in ipaddress.ip_network(entry, strict=False):
                    return True
            except ValueError:
                continue
        return False

    def retention_hours(self, connection_id: str) -> float:
        return float(self.archive_retention_h.get(connection_id, self.archive_retention_h.get("*", 0.0)))


def production_violations(s: Settings) -> list[str]:
    """Insecure settings refused when ALELT_ENV is not a dev env (security hardening defaults, F-14)."""
    if s.env in DEV_ENVS:
        return []
    out = []
    if s.auth_mode == "dev":
        out.append("dev auth (ALELT_DEV_AUTH / ALELT_AUTH_MODE=dev) (X-17)")
    if s.agent_tokens:
        out.append("static shared agent tokens (ALELT_AGENT_TOKENS): use enrollment tokens -> per-agent credentials "
                   "or mTLS")
    if s.agent_auth not in ("mtls", "token", "enrollment"):
        out.append(f"unknown ALELT_AGENT_AUTH={s.agent_auth!r}")
    if s.agent_auth == "mtls" and any(p in ("*", "0.0.0.0/0", "::/0") for p in s.trusted_proxies):
        out.append("ALELT_TRUSTED_PROXIES must list the TLS proxy addresses, not '*'")
    if "*" in s.forwarded_allow_ips:
        out.append("ALELT_FORWARDED_ALLOW_IPS='*' (any client could forge X-Forwarded-For): list the proxy CIDR")
    if s.public_url.startswith("http://"):
        out.append("ALELT_PUBLIC_URL must be https:// (agents send bearer credentials)")
    if "*" in s.cors_origins:
        out.append("ALELT_CORS_ORIGINS='*'")
    return out


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    s = Settings()
    bad = production_violations(s)
    if bad:
        raise RuntimeError(f"ALELT_ENV={s.env!r} refuses insecure settings: " + "; ".join(bad))
    return s

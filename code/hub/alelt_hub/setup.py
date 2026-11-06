"""First-run setup (system setup only) + agent enrollment.

Steps (state in `hub_setting`, keys `setup.*`): metadata database (info + test), admin, destination (test + bootstrap
via the destination SPI), agent enrollment. Pipelines (sources, tables) are created by the import / pipeline wizards
(`/import`, `/wizard`); this module only links to them.

Enrollment: `POST /api/v1/setup/agents/enroll` returns a one-time token (shown once; only its sha256 is stored) and
pre-filled `devedge.json` / `aleltagent.json` without secrets (R9: the token goes into the agent's environment / secret
store). The token is bound to one connection; the first successful register claims it for that `agent_id`; an
unclaimed token expires after `ttl_hours`. `EnrollmentBindingMiddleware` enforces the binding on every agent call.

Permission: PlatformAdmin (`users.manage`) for every setup mutation; `view.dashboard` to read the status.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import store
from .auth import Principal, require_perm
from .db import app_user, connection, get_engine, group_mapping, hub_setting, md, role_assignment
from .settings import DEV_ENVS, get_settings

router = APIRouter(prefix="/api/v1/setup", tags=["setup"])
_ADMIN = "users.manage"
_PREFIX = "enroll:"
_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
_TTL = 15.0

DB2_PREREQUISITES = [
    {"id": "archive_logging", "label": "Archive logging enabled (LOGARCHMETH1 is not OFF)",
     "check": "db2 get db cfg for <DB> | findstr LOGARCHMETH1"},
    {"id": "data_capture", "label": "DATA CAPTURE CHANGES on every captured table",
     "check": "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES WHERE DATACAPTURE <> 'Y' AND TABSCHEMA = '<SCHEMA>'"},
    {"id": "authority", "label": "Capture user holds DBADM (or SYSADM / SYSMAINT) for db2ReadLog",
     "check": "SELECT * FROM TABLE(SYSPROC.AUTH_LIST_AUTHORITIES_FOR_AUTHID('<USER>', 'U'))"},
    {"id": "no_value_compression", "label": "No VALUE COMPRESSION on captured tables",
     "check": "SELECT TABNAME FROM SYSCAT.TABLES WHERE COMPRESSION IN ('V', 'B')"},
    {"id": "outbound", "label": "Agent host can reach the hub URL (outbound HTTPS only, no inbound port)",
     "check": "Test-NetConnection <hub-host> -Port 443"},
]


# ------------------------------------------------------------------ hub_setting helpers
def _get(c: Any, key: str) -> Any:
    return c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar()


def _put(c: Any, key: str, value: Any) -> None:
    if c.dialect.name == "postgresql":
        stmt = pg_insert(hub_setting).values(key=key, value=value)
        c.execute(stmt.on_conflict_do_update(index_elements=["key"], set_={"value": value}))
        return
    if c.execute(select(hub_setting.c.key).where(hub_setting.c.key == key)).first():
        c.execute(hub_setting.update().where(hub_setting.c.key == key).values(value=value))
    else:
        c.execute(hub_setting.insert().values(key=key, value=value))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _reason(reason: str | None) -> str:
    if not reason or not reason.strip():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, {"code": "REASON_REQUIRED", "message": "reason required"})
    return reason.strip()


def _redact_url(url: str) -> str:
    from .product import redact_url

    return redact_url(url)


def _engine_name(url: str) -> str:
    u = url.split(":", 1)[0]
    return "sqlite" if u.startswith("sqlite") else "postgres" if u.startswith("postgres") else u


# ------------------------------------------------------------------ status
@router.get("/status")
def setup_status(_: Principal = Depends(require_perm("view.dashboard"))) -> dict[str, Any]:
    s = get_settings()
    with get_engine().connect() as c:
        steps = {k: _get(c, f"setup.{k}") for k in ("metadata", "admin", "destination", "agent")}
        completed = _get(c, "setup.completed")
        n_enroll = c.execute(select(hub_setting.c.key).where(hub_setting.c.key.like(f"{_PREFIX}%"))).all()
        n_conn = c.execute(select(connection.c.id)).all()
    return {"completed": bool(completed), "completed_at": (completed or {}).get("at") if completed else None,
            "auth_mode": s.auth_mode, "env": s.env,
            "steps": {k: {"done": bool(v), **(v or {})} for k, v in steps.items()},
            "metadata": metadata_info(), "destination": destination_info(),
            "connections": [r[0] for r in n_conn], "enrollments": len(n_enroll),
            "links": {"import": "/import/new", "wizard": "/wizard"}}


@router.post("/complete")
def setup_complete(body: dict[str, Any] | None = None, p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    with get_engine().begin() as c:
        _put(c, "setup.completed", {"at": _now().isoformat(), "by": p.subject})
        store.audit(c, p.subject, "setup.complete", None, body or {}, p.primary_role)
    return {"completed": True}


# ------------------------------------------------------------------ metadata database
def metadata_info() -> dict[str, Any]:
    url = get_settings().db_url
    eng = _engine_name(url)
    info: dict[str, Any] = {"engine": eng, "url": _redact_url(url)}
    try:
        with get_engine().connect() as c:
            have = set(inspect(c).get_table_names())
            info["schema_ok"] = set(md.tables) <= have
            info["has_data"] = bool(c.execute(text("SELECT count(*) FROM batch")).scalar()) if "batch" in have else False
    except Exception as e:  # noqa: BLE001
        info["error"] = type(e).__name__
    info["mode"] = "sqlite" if eng == "sqlite" else (
        "bundled_postgres" if "@postgres:" in url or "@postgres/" in url else "external_postgres")
    return info


class MetadataTest(BaseModel):
    mode: str = Field(pattern="^(sqlite|bundled_postgres|external_postgres)$")
    url: str | None = None


@router.get("/metadata")
def get_metadata(_: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    return metadata_info()


@router.post("/metadata/test")
def test_metadata(req: MetadataTest, _: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    """Connect to a candidate metadata database. Nothing is stored: switching = change ALELT_DB_URL / ALELT_METADATA in .env
    and restart. Existing data is not moved (warning returned when the current database already holds batches)."""
    cur = metadata_info()
    url = req.url or (get_settings().db_url if req.mode == cur["mode"] else "")
    if req.mode == "sqlite" and not url:
        url = "sqlite:////data/alelt.db"
    if not url:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, {"code": "URL_REQUIRED", "message": "database URL required"})
    if req.mode == "sqlite" and not url.startswith("sqlite"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, {"code": "INVALID", "message": "SQLite URL must start with sqlite:"})
    if req.mode != "sqlite" and not url.startswith("postgresql"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, {"code": "INVALID", "message": "use postgresql+psycopg://..."})
    t0 = time.monotonic()
    ok, detail = True, ""
    try:
        if url.startswith("sqlite"):
            from .dialect import make_engine   # WAL / busy_timeout / BEGIN IMMEDIATE pragmas

            e = make_engine(url)
        else:
            e = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 8})
        with e.connect() as c:
            ver = c.execute(text("SELECT sqlite_version()" if url.startswith("sqlite") else "SELECT version()")).scalar()
        e.dispose()
        detail = str(ver)[:120]
    except Exception as ex:  # noqa: BLE001
        ok, detail = False, f"{type(ex).__name__}: {str(ex).splitlines()[0][:200] if str(ex) else ''}"
    switching = req.mode != cur["mode"] or (req.url and req.url != get_settings().db_url)
    warn = ("Switching the metadata database does not migrate existing data (batches, config revisions, audit). "
            "Requires a migration; set ALELT_DB_URL / ALELT_METADATA in .env and restart.") if switching and cur.get("has_data") else (
        "Set ALELT_DB_URL / ALELT_METADATA in .env and restart to apply." if switching else "")
    return {"ok": ok, "detail": _scrub(detail), "elapsed_ms": int((time.monotonic() - t0) * 1000),
            "switching": bool(switching), "warning": warn, "current": cur}


@router.post("/metadata/ack")
def ack_metadata(p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    info = metadata_info()
    with get_engine().begin() as c:
        _put(c, "setup.metadata", {"mode": info["mode"], "engine": info["engine"], "at": _now().isoformat()})
        store.audit(c, p.subject, "setup.metadata", info["mode"], {"engine": info["engine"]}, p.primary_role)
    return info


def _scrub(s: str) -> str:
    import re

    return re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1***@", s)


# ------------------------------------------------------------------ admin
class AdminReq(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    group_id: str | None = None          # OIDC: identity-provider group mapped to PlatformAdmin
    group_name: str | None = None
    reason: str | None = "first-run setup"


@router.post("/admin")
def create_admin(req: AdminReq, p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    import uuid

    with get_engine().begin() as c:
        uid = c.execute(select(app_user.c.id).where(app_user.c.email == req.email)).scalar()
        if uid is None:
            uid = uuid.uuid4()
            c.execute(app_user.insert().values(id=uid, email=req.email, entra_oid=None))
        if not c.execute(select(role_assignment.c.user_id).where(role_assignment.c.user_id == uid,
                                                               role_assignment.c.role == "PlatformAdmin")).first():
            c.execute(role_assignment.insert().values(user_id=uid, role="PlatformAdmin"))
        if req.group_id:
            if c.execute(select(group_mapping.c.group_id).where(group_mapping.c.group_id == req.group_id)).first():
                c.execute(group_mapping.update().where(group_mapping.c.group_id == req.group_id)
                          .values(group_name=req.group_name or req.group_id, role="PlatformAdmin"))
            else:
                c.execute(group_mapping.insert().values(group_id=req.group_id, group_name=req.group_name or req.group_id,
                                                        role="PlatformAdmin"))
        _put(c, "setup.admin", {"email": req.email, "at": _now().isoformat(),
                                "mode": get_settings().auth_mode})
        store.audit(c, p.subject, "setup.admin", req.email, {"reason": _reason(req.reason),
                                                              "group_id": req.group_id}, p.primary_role)
    return {"email": req.email, "role": "PlatformAdmin", "auth_mode": get_settings().auth_mode}


# ------------------------------------------------------------------ destination
def destination_info() -> dict[str, Any]:
    s = get_settings()
    eng = ("snowflake" if "snowflake" in s.destination else "sql" if "alelt_dst_sql" in s.destination
           else "postgres" if "postgres" in s.destination else s.destination)
    return {"engine": eng, "plugin": s.destination, "url": _redact_url(s.dest_url) if s.dest_url else ""}


class DestinationReq(BaseModel):
    engine: str = Field(pattern="^(postgres|snowflake|sql)$")
    url: str | None = None                       # postgres/sql: empty = configured ALELT_DEST_URL (never stored)
    dialect: str | None = None                   # sql: alelt_dst_sql dialect (postgres, mysql, sqlserver, db2, ...)
    host: str | None = None                      # sql: wizard fields -> URL (alelt_dst_sql.url_from_fields)
    port: int | None = None
    path: str | None = None
    password: str | None = None                  # used once, never stored (R9)
    schema_name: str = Field(default="alelt_setup_check", alias="schema", pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    account: str | None = None                   # snowflake identifiers (key from ALELT_SNOWFLAKE_PRIVATE_KEY[_PATH])
    user: str | None = None
    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    bootstrap: bool = False
    reason: str | None = "first-run setup"

    model_config = {"populate_by_name": True}


@router.get("/destination")
def get_destination(_: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    return destination_info()


@router.post("/destination/test")
def test_destination(req: DestinationReq, p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    """Connect to the destination and (optionally) run the SPI `bootstrap()` (control schema/tables; idempotent).
    Credentials passed here are used once and never stored (R9); persist them in .env / Docker secrets."""
    t0 = time.monotonic()
    steps: list[dict[str, Any]] = []
    ok = True
    if req.engine == "postgres":
        url = req.url or get_settings().dest_url
        if not url:
            raise HTTPException(400, {"code": "URL_REQUIRED", "message": "ALELT_DEST_URL not configured; pass url"})
        try:
            import psycopg

            with psycopg.connect(url, connect_timeout=8) as c:
                v = c.execute("SELECT version()").fetchone()
            steps.append({"step": "connect", "ok": True, "detail": str(v[0] if v else "")[:120]})
            if req.bootstrap:
                from alelt_dst_postgres.connector import PostgresDestination

                d = PostgresDestination(url=url, schema=req.schema_name)
                try:
                    d.bootstrap()
                finally:
                    d.close()
                steps.append({"step": "bootstrap", "ok": True, "detail": "control schema alelt_control ready"})
        except Exception as e:  # noqa: BLE001
            ok = False
            steps.append({"step": "connect" if not steps else "bootstrap", "ok": False,
                          "detail": _scrub(f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")[:300]})
    elif req.engine == "sql":                    # generic SQL destination (connectors/destinations/sql)
        try:
            from alelt_dst_sql import SqlDestination, url_from_fields

            url = req.url or (url_from_fields(req.dialect or "", req.model_dump()) if (req.host or req.path)
                              else get_settings().dest_url)
            d = SqlDestination(url=url, schema=req.schema_name, dialect=req.dialect, password=req.password)
            try:
                steps.append({"step": "connect", "ok": True, "detail": d.test_connection()[:120]})
                if req.bootstrap:
                    d.bootstrap()
                    steps.append({"step": "bootstrap", "ok": True, "detail": f"control schema {d.control} ready"})
            finally:
                d.close()
        except Exception as e:  # noqa: BLE001
            ok = False
            steps.append({"step": "connect" if not steps else "bootstrap", "ok": False,
                          "detail": _scrub(f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")[:300]})
    else:
        try:
            from alelt_dst_snowflake.executor import SnowflakeExecutor, SnowflakeSettings

            env = SnowflakeSettings.from_env() if not req.account else None
            sf = env or SnowflakeSettings(account=req.account or "", user=req.user or "",
                                          role=req.role or "ALELT_LOADER", warehouse=req.warehouse or "ALELT_WH",
                                          database=req.database or "", private_key_path=_env("ALELT_SNOWFLAKE_PRIVATE_KEY_PATH"))
            ex = SnowflakeExecutor(sf)
            try:
                v = ex.execute("SELECT CURRENT_VERSION(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
                steps.append({"step": "connect", "ok": True, "detail": str(v[0] if v else "")})
                if req.bootstrap:
                    from alelt_dst_snowflake.destination import SnowflakeDestination

                    SnowflakeDestination(ex, sf.database, req.schema_name.upper(), compare=lambda a, b: 0,
                                         migration_params={}).bootstrap()
                    steps.append({"step": "bootstrap", "ok": True, "detail": "ALELT_CONTROL ready"})
            finally:
                ex.close()
        except Exception as e:  # noqa: BLE001
            ok = False
            steps.append({"step": "connect" if not steps else "bootstrap", "ok": False,
                          "detail": _scrub(f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")[:300]})
    with get_engine().begin() as c:
        if ok:
            _put(c, "setup.destination", {"engine": req.engine, "bootstrapped": req.bootstrap, "at": _now().isoformat()})
        store.audit(c, p.subject, "setup.destination.test", req.engine,
                    {"ok": ok, "bootstrap": req.bootstrap, "reason": req.reason}, p.primary_role)
    return {"ok": ok, "steps": steps, "elapsed_ms": int((time.monotonic() - t0) * 1000), "configured": destination_info()}


def _env(k: str) -> str | None:
    import os

    return os.environ.get(k) or None


# ------------------------------------------------------------------ agent enrollment
class EnrollReq(BaseModel):
    connection_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    engine: str = Field(default="db2", pattern="^(db2)$")
    database: str = Field(min_length=1, max_length=128)
    host: str | None = None
    port: int | None = None
    user: str | None = None                      # informational; the agent authenticates locally (no password here)
    schemas: list[str] = Field(default_factory=list)
    agent_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]{1,100}$")
    hub_url: str | None = None
    ttl_hours: int = Field(default=24, ge=1, le=720)
    reason: str | None = "agent enrollment"


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def enrollment(token: str) -> dict[str, Any] | None:
    """Binding for an enrollment token (cached briefly); None when unknown, revoked, or expired unclaimed."""
    if not token or len(token) < 20:
        return None
    h = _hash(token)
    hit = _cache.get(h)
    if hit is None or time.monotonic() - hit[0] > _TTL:
        try:
            with get_engine().connect() as c:
                v = _get(c, _PREFIX + h)
        except Exception:  # noqa: BLE001
            v = None
        hit = (time.monotonic(), v)
        _cache[h] = hit
    b = hit[1]
    if not b or b.get("revoked") or b.get("retired_at"):
        return None          # retired = the agent already uses its per-agent credential (replay refused)
    if not b.get("claimed_by") and datetime.fromisoformat(b["expires_at"]) < _now():
        return None
    ttl = get_settings().enrollment_claimed_ttl_h        # AHS-04: not a permanent credential
    if b.get("claimed_at") and ttl > 0 and datetime.fromisoformat(b["claimed_at"]) + timedelta(hours=ttl) < _now():
        return None
    return {**b, "hash": h}


# ------------------------------------------------------------------ per-agent credentials (F-14)
_CRED = "agentcred:"
_ccache: dict[str, tuple[float, dict[str, Any] | None]] = {}


def issue_credential(c: Any, agent_id: str, connection_id: str, *, enrollment_hash: str | None = None,
                     rotated_from: str | None = None) -> tuple[str, datetime]:
    """New `alelta_` credential bound to (agent, connection); only its sha256 is stored. Shown once."""
    s = get_settings()
    token = "alelta_" + secrets.token_urlsafe(32)
    exp = _now() + timedelta(days=s.agent_credential_ttl_days)
    _put(c, _CRED + _hash(token), {"agent_id": agent_id, "connection_id": connection_id,
                                   "created_at": _now().isoformat(), "expires_at": exp.isoformat(),
                                   "enrollment": enrollment_hash, "rotated_from": rotated_from,
                                   "revoked": False, "replaced_at": None, "grace_until": None, "used_at": None})
    store.audit(c, f"agent:{agent_id}", "agent.credential.issue", connection_id,
                {"expires_at": exp.isoformat(), "rotated": bool(rotated_from), "credential_id": _hash(token)[:12]})
    return token, exp


def credential(token: str) -> dict[str, Any] | None:
    """Binding of a per-agent credential; None when unknown, revoked, expired, or replaced (after the grace)."""
    if not token or len(token) < 20:
        return None
    h = _hash(token)
    hit = _ccache.get(h)
    if hit is None or time.monotonic() - hit[0] > _TTL:
        try:
            with get_engine().connect() as c:
                v = _get(c, _CRED + h)
        except Exception:  # noqa: BLE001
            v = None
        hit = (time.monotonic(), v)
        _ccache[h] = hit
    b = hit[1]
    if not b or b.get("revoked") or datetime.fromisoformat(b["expires_at"]) < _now():
        return None
    if b.get("grace_until") and datetime.fromisoformat(b["grace_until"]) < _now():
        return None
    if not b.get("used_at"):
        _first_use(h, b)
    return {**b, "hash": h, "claimed_by": b["agent_id"]}


def _first_use(h: str, b: dict[str, Any]) -> None:
    """First authenticated call with a credential retires the enrollment token it was exchanged for (replay)."""
    with get_engine().begin() as c:
        v = _get(c, _CRED + h)
        if v and not v.get("used_at"):
            _put(c, _CRED + h, {**v, "used_at": _now().isoformat()})
            eh = v.get("enrollment")
            e = _get(c, _PREFIX + eh) if eh else None
            if e and not e.get("retired_at"):
                _put(c, _PREFIX + eh, {**e, "retired_at": _now().isoformat()})
                store.audit(c, f"agent:{v['agent_id']}", "agent.enroll.retire", v["connection_id"], {})
    _ccache.pop(h, None)
    _cache.pop(b.get("enrollment") or "", None)


def rotate_credential(old_hash: str) -> tuple[str, datetime]:
    """Agent-initiated rotation: new credential; the old one stays valid for ALELT_AGENT_CREDENTIAL_GRACE_S."""
    s = get_settings()
    with get_engine().begin() as c:
        v = _get(c, _CRED + old_hash)
        if not v or v.get("revoked") or v.get("replaced_at"):
            raise HTTPException(status.HTTP_409_CONFLICT, "credential already rotated or revoked")
        token, exp = issue_credential(c, v["agent_id"], v["connection_id"], rotated_from=old_hash)
        _put(c, _CRED + old_hash, {**v, "replaced_at": _now().isoformat(),
                                   "grace_until": (_now() + timedelta(seconds=s.agent_credential_grace_s)).isoformat()})
    _ccache.pop(old_hash, None)
    return token, exp


def credential_expiries() -> list[tuple[str, float]]:
    """(agent_id, expiry epoch) of live credentials (metric alelt_agent_credential_expiry_timestamp_seconds)."""
    with get_engine().connect() as c:
        rows = c.execute(select(hub_setting.c.value).where(hub_setting.c.key.like(f"{_CRED}%"))).all()
    out: dict[str, float] = {}
    for (v,) in rows:
        if v.get("revoked") or v.get("replaced_at"):
            continue
        out[v["agent_id"]] = max(out.get(v["agent_id"], 0.0), datetime.fromisoformat(v["expires_at"]).timestamp())
    return sorted(out.items())


def _claim(h: str, agent_id: str) -> None:
    with get_engine().begin() as c:
        v = _get(c, _PREFIX + h)
        if v and not v.get("claimed_by"):
            v = {**v, "claimed_by": agent_id, "claimed_at": _now().isoformat()}
            _put(c, _PREFIX + h, v)
            store.audit(c, f"agent:{agent_id}", "agent.enroll.claim", v["connection_id"], {"agent_id": agent_id})
    _cache.pop(h, None)


def _configs(req: EnrollReq, agent_id: str, hub_url: str) -> tuple[dict[str, Any], dict[str, Any]]:
    devedge = {"hub_url": hub_url, "agent_id": agent_id, "connection_id": req.connection_id, "database": req.database,
               "schemas": req.schemas or ["*"], "token_env": "ALELT_AGENT_TOKEN", "db2_bin": "C:/Program Files/IBM/SQLLIB/BIN",
               "spool_dir": "spool", "spool_max_mb": 256, "codec": "zlib", "start": "current", "heartbeat_s": 15,
               "command_poll_s": 2, "state_file": "devedge-state.json",
               "limits": {"cpu_pct": 50, "mem_mb": 512, "batch_kb": 256, "max_rate_kbps": 0, "poll_ms": 1000}}
    aleltagent = {"_comment": "ALELT agent config (no secrets): set the enrollment token in the agent's environment "
                           "variable ALELT_AGENT_TOKEN or its protected credential store.",
               "agent_id": agent_id, "connection_id": req.connection_id,
               "hub": {"url": hub_url, "mode": "token", "token_env": "ALELT_AGENT_TOKEN", "heartbeat_s": 15,
                       "command_wait_s": 10, "connect_timeout_ms": 10000, "request_timeout_ms": 60000,
                       "backoff_max_s": 300},
               "spool": {"dir": "%ProgramData%\\ALELT\\Agent\\spool", "max_mb": 2048},
               "log": {"dir": "%ProgramData%\\ALELT\\Agent\\log", "level": "INFO", "max_mb": 20, "keep": 5},
               "limits": {"cpu_pct": 50, "mem_mb": 512, "batch_kb": 1024, "max_rate_kbps": 0, "poll_ms": 1000},
               "source": {"plugin": "aleltsrc_db2.dll", "engine": req.engine, "database": req.database,
                          "dll_dirs": ["%DB2PATH%\\BIN"],
                          "settings": {"database": req.database, "catalog_schemas": req.schemas}},
               "capture": {"autostart": False}}
    return devedge, aleltagent


@router.get("/prerequisites")
def prerequisites(engine: str = "db2", _: Principal = Depends(require_perm("view.dashboard"))) -> dict[str, Any]:
    if engine != "db2":
        raise HTTPException(404, {"code": "NOT_FOUND", "message": f"no checklist for {engine}"})
    return {"engine": engine, "items": DB2_PREREQUISITES,
            "note": "Checked on the source host by the agent (PROBE); the hub never connects to the source."}


@router.post("/agents/enroll")
def enroll(req: EnrollReq, request: Request, p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    token = "alelte_" + secrets.token_urlsafe(32)
    agent_id = req.agent_id or f"{req.connection_id}-agent"
    s = get_settings()                                   # AHS-11: never derive the URL from the Host header in prod
    hub_url = (req.hub_url or s.public_url or (str(request.base_url) if s.env in DEV_ENVS else "")).rstrip("/")
    if not hub_url:
        raise HTTPException(400, {"code": "PUBLIC_URL_REQUIRED", "message": "set ALELT_PUBLIC_URL or pass hub_url"})
    if hub_url.startswith("http://") and s.env not in DEV_ENVS and             (urlsplit(hub_url).hostname or "") not in ("localhost", "127.0.0.1", "::1"):
        raise HTTPException(400, {"code": "INSECURE_URL", "message": "agents must reach the hub over https"})
    exp = _now() + timedelta(hours=req.ttl_hours)
    devedge, aleltagent = _configs(req, agent_id, hub_url)
    with get_engine().begin() as c:
        if c.execute(select(connection.c.id).where(connection.c.id == req.connection_id)).scalar() is None:
            c.execute(connection.insert().values(id=req.connection_id, engine=req.engine, db_database=req.database,
                                                 db_instance=f"{req.host}:{req.port}" if req.host else None))
        _put(c, f"source:{req.connection_id}", {"engine": req.engine, "host": req.host, "port": req.port,
                                                "database": req.database, "user": req.user, "schemas": req.schemas})
        _put(c, _PREFIX + _hash(token), {"connection_id": req.connection_id, "agent_id": agent_id,
                                         "created_at": _now().isoformat(), "expires_at": exp.isoformat(),
                                         "created_by": p.subject, "claimed_by": None, "revoked": False})
        _put(c, "setup.agent", {"connection_id": req.connection_id, "agent_id": agent_id, "at": _now().isoformat()})
        store.audit(c, p.subject, "agent.enroll.create", req.connection_id,
                    {"agent_id": agent_id, "expires_at": exp.isoformat(), "reason": _reason(req.reason)},
                    p.primary_role)
    return {"token": token, "token_hint": token[:8] + "...", "agent_id": agent_id, "connection_id": req.connection_id,
            "expires_at": exp.isoformat(), "hub_url": hub_url, "devedge_json": devedge, "aleltagent_json": aleltagent,
            "prerequisites": DB2_PREREQUISITES,
            "instructions": [
                "Copy the token now: it is shown only once.",
                "Save devedge.json (dev edge) or aleltagent.json (Windows service) on the Db2 host.",
                "Set the token in the agent environment: $env:ALELT_AGENT_TOKEN = '<token>' (service: protected store).",
                "Start the agent; it registers with this hub and is bound to connection "
                f"'{req.connection_id}' as agent '{agent_id}'."]}


@router.get("/agents/enrollments")
def list_enrollments(_: Principal = Depends(require_perm(_ADMIN))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        rows = c.execute(select(hub_setting.c.key, hub_setting.c.value)
                         .where(hub_setting.c.key.like(f"{_PREFIX}%"))).all()
    out = []
    for k, v in rows:
        state = "revoked" if v.get("revoked") else "claimed" if v.get("claimed_by") else (
            "expired" if datetime.fromisoformat(v["expires_at"]) < _now() else "pending")
        out.append({"id": k[len(_PREFIX):][:12], **{x: v.get(x) for x in (
            "connection_id", "agent_id", "created_at", "expires_at", "claimed_by", "claimed_at", "created_by")},
            "state": state})
    return sorted(out, key=lambda r: r["created_at"] or "", reverse=True)


@router.post("/agents/enrollments/{eid}/revoke")
def revoke_enrollment(eid: str, body: dict[str, Any], p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    reason = _reason(body.get("reason"))
    with get_engine().begin() as c:
        rows = c.execute(select(hub_setting.c.key, hub_setting.c.value)
                         .where(hub_setting.c.key.like(f"{_PREFIX}{eid}%"))).all()
        if len(rows) != 1:
            raise HTTPException(404, {"code": "NOT_FOUND", "message": "enrollment not found"})
        k, v = rows[0]
        _put(c, k, {**v, "revoked": True})
        store.audit(c, p.subject, "agent.enroll.revoke", v["connection_id"], {"reason": reason}, p.primary_role)
    _cache.clear()
    return {"revoked": True}


@router.get("/agents/credentials")
def list_credentials(_: Principal = Depends(require_perm(_ADMIN))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        rows = c.execute(select(hub_setting.c.key, hub_setting.c.value)
                         .where(hub_setting.c.key.like(f"{_CRED}%"))).all()
    out = []
    for k, v in rows:
        state = ("revoked" if v.get("revoked") else "rotated" if v.get("replaced_at") else
                 "expired" if datetime.fromisoformat(v["expires_at"]) < _now() else "active")
        out.append({"id": k[len(_CRED):][:12], "state": state, **{x: v.get(x) for x in (
            "agent_id", "connection_id", "created_at", "expires_at", "used_at", "replaced_at")}})
    return sorted(out, key=lambda r: r["created_at"] or "", reverse=True)


@router.post("/agents/{agent_id}/credentials/revoke")
def revoke_credentials(agent_id: str, body: dict[str, Any],
                       p: Principal = Depends(require_perm(_ADMIN))) -> dict[str, Any]:
    """Revoke every credential and enrollment of an agent (compromised host): it must be re-enrolled."""
    reason = _reason(body.get("reason"))
    n = 0
    with get_engine().begin() as c:
        for k, v in c.execute(select(hub_setting.c.key, hub_setting.c.value).where(
                hub_setting.c.key.like(f"{_CRED}%") | hub_setting.c.key.like(f"{_PREFIX}%"))).all():
            if (v.get("agent_id") == agent_id or v.get("claimed_by") == agent_id) and not v.get("revoked"):
                _put(c, k, {**v, "revoked": True})
                n += 1
        store.audit(c, p.subject, "agent.credential.revoke", agent_id, {"reason": reason, "count": n},
                    p.primary_role)
    _ccache.clear()
    _cache.clear()
    return {"revoked": n}


# ------------------------------------------------------------------ binding enforcement (pure ASGI)
def _deny(msg: str) -> bytes:
    return json.dumps({"error": {"code": "forbidden", "message": msg}, "detail": msg}).encode()


class EnrollmentBindingMiddleware:
    """For agent calls authenticated by an enrollment token or a per-agent credential (`alelta_`): connection/agent
    must match the binding; an enrollment token must register first (claims it). Static tokens and mTLS are not
    affected."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http" or not scope["path"].startswith("/agent/v1/"):
            await self.app(scope, receive, send)
            return
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        tok = headers.get("x-alelt-agent-token", "")
        b = enrollment(tok) if tok.startswith("alelte_") else credential(tok) if tok.startswith("alelta_") else None
        if b is None:
            await self.app(scope, receive, send)
            return
        path = scope["path"]
        if path == "/agent/v1/register" and scope["method"] == "POST":
            chunks = []
            more = True
            while more:
                m = await receive()
                chunks.append(m.get("body", b""))
                more = m.get("more_body", False)
            body = b"".join(chunks)
            try:
                d = json.loads(body or b"{}")
            except ValueError:
                d = {}
            if d.get("connection_id") != b["connection_id"]:
                return await self._reject(send, f"token is bound to connection {b['connection_id']!r}")
            if d.get("agent_id") != (b.get("claimed_by") or b["agent_id"]):
                return await self._reject(send, f"token is bound to agent {b.get('claimed_by') or b['agent_id']!r}")
            sent = False

            async def replay() -> dict[str, Any]:
                nonlocal sent
                if not sent:
                    sent = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await receive()

            status_code = [0]

            async def watch(msg: dict[str, Any]) -> None:
                if msg["type"] == "http.response.start":
                    status_code[0] = msg["status"]
                await send(msg)

            await self.app(scope, replay, watch)
            if status_code[0] == 200 and not b.get("claimed_by"):
                _claim(b["hash"], d["agent_id"])
            return None
        if not b.get("claimed_by"):
            return await self._reject(send, "enrollment token not yet claimed: register first")
        want_agent, want_conn = b["claimed_by"], b["connection_id"]
        qs = parse_qs(scope.get("query_string", b"").decode())
        if "agent_id" in qs and qs["agent_id"][0] != want_agent:
            return await self._reject(send, "agent_id does not match the enrollment")
        hdr = headers.get("x-alelt-batch")
        if hdr:
            try:
                h = json.loads(hdr)
            except ValueError:
                h = {}
            if h.get("connection_id", want_conn) != want_conn or h.get("agent_id", want_agent) != want_agent:
                return await self._reject(send, "batch connection/agent does not match the enrollment")
        await self.app(scope, receive, send)
        return None

    @staticmethod
    async def _reject(send: Any, msg: str) -> None:
        body = _deny(msg)
        await send({"type": "http.response.start", "status": 403,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})

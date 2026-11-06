"""Product endpoints: version, support bundle; `register_product(app)` wires setup, console static files and these.

- `GET /version`, `GET /api/v1/version` - build + runtime versions (no auth: no secrets, used by installers/LBs).
- `GET /api/v1/support/bundle` - zip for support: versions, redacted config + env, recent logs, metrics snapshot,
  job/batch/agent state, quarantine summary. PlatformAdmin (`hub.view`). Never contains secrets (R9).
"""
from __future__ import annotations

import io
import json
import os
import platform
import re
import sys
import time
import zipfile
from dataclasses import asdict
from importlib import metadata
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import Response
from sqlalchemy import func, inspect, select

from . import __version__, store
from .auth import Principal, require_perm
from .db import agent, audit_event, batch, checkpoint_mirror, connection, get_engine, job, md, quarantine_event
from .metrics import REGISTRY
from .settings import get_settings

router = APIRouter(tags=["product"])
_SECRET = re.compile(r"(PASSWORD|PASSWD|PWD|SECRET|TOKEN|PRIVATE_KEY|PASSPHRASE|CREDENTIAL|API_KEY|KEY_ID|ACCESS_KEY|"
                     r"CONNECTION_STRING|_DSN)", re.I)       # AHS-22: KEY_ID / ACCESS_KEY too
_PKGS = ("fastapi", "uvicorn", "sqlalchemy", "psycopg", "pydantic", "pyarrow", "snowflake-connector-python")


def redact_url(url: str) -> str:
    try:
        u = urlsplit(url)
    except ValueError:
        return "<redacted>"
    if u.password is None:
        return url
    host = u.hostname or ""
    netloc = f"{u.username}:***@{host}" + (f":{u.port}" if u.port else "")
    return urlunsplit((u.scheme, netloc, u.path, u.query, u.fragment))


def redact(key: str, value: Any) -> Any:
    if isinstance(value, (list, tuple)) and _SECRET.search(key):
        return f"<{len(value)} value(s) redacted>"
    if not isinstance(value, str):
        return value
    if _SECRET.search(key) and not key.endswith("_FILE") and not key.endswith("_ENV"):
        return "<redacted>" if value else ""
    if "://" in value:
        return redact_url(value)
    return value


def version_info() -> dict[str, Any]:
    pk: dict[str, str] = {}
    for p in _PKGS:
        try:
            pk[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            pass
    return {"product": "ALELT", "vendor": "Freddy Beltran", "version": os.environ.get("ALELT_BUILD_VERSION")
            or __version__, "hub_version": __version__, "revision": os.environ.get("ALELT_BUILD_REVISION", ""),
            "build_date": os.environ.get("ALELT_BUILD_DATE", ""), "protocol": 1,
            "python": sys.version.split()[0], "platform": f"{platform.system()} {platform.machine()}",
            "packages": pk, "decoders": decoder_info()}


def decoder_info() -> dict[str, Any]:
    """Active decoder backend per source engine (C library by default, Python fallback)."""
    out: dict[str, Any] = {}
    try:
        from alelt_src_db2 import native

        out["db2"] = native.info()
    except Exception as e:  # noqa: BLE001 - connector not installed
        out["db2"] = {"backend": "unavailable", "reason": type(e).__name__}
    return out


@router.get("/version")
@router.get("/api/v1/version")
def version() -> dict[str, Any]:
    v = version_info()
    try:
        with get_engine().connect() as c:
            have = set(inspect(c).get_table_names())
        v["schema"] = {"tables_expected": len(md.tables), "tables_present": len(have & set(md.tables)),
                       "up_to_date": set(md.tables) <= have}
    except Exception as e:  # noqa: BLE001
        v["schema"] = {"error": type(e).__name__}
    return v


def _rows(c: Any, q: Any) -> list[dict[str, Any]]:
    return [{k: (v if isinstance(v, (int, float, bool, type(None), list, dict)) else str(v)) for k, v in r.items()}
            for r in c.execute(q).mappings()]


def _recent_logs(max_bytes: int = 2 << 20) -> dict[str, bytes]:
    d = os.environ.get("ALELT_LOG_DIR")
    out: dict[str, bytes] = {}
    if not d or not Path(d).is_dir():
        return out
    for f in sorted(Path(d).glob("*.log")):
        data = f.read_bytes()[-max_bytes:]
        out[f.name] = b"\n".join(_scrub(line) for line in data.splitlines())
    return out


def _scrub(line: bytes) -> bytes:
    """Mask URL passwords that might appear in a log line."""
    return re.sub(rb"(://[^:/@\s]+:)[^@\s]+@", rb"\1***@", line)


def build_bundle() -> bytes:
    s = get_settings()
    cfg = {k: redact(k.upper(), str(v) if isinstance(v, Path) else v) for k, v in asdict(s).items()}
    env = {k: redact(k, v) for k, v in sorted(os.environ.items()) if k.startswith(("ALELT_", "POSTGRES_"))}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("versions.json", json.dumps(version(), indent=2, default=str))
        z.writestr("config/settings.json", json.dumps(cfg, indent=2, default=str))
        z.writestr("config/env.json", json.dumps(env, indent=2))
        try:                                     # F-18: the active config revisions (not files)
            from . import config_store

            with get_engine().connect() as c:
                for cid in config_store.connections_with_docs(c):
                    rid, doc = config_store.active(c, cid) or (None, {})
                    z.writestr(f"config/active/{cid}.yaml", config_store.export_yaml(doc, rid))
                z.writestr("config/drift.json", json.dumps(config_store.drift(c, s.tables_config), indent=2,
                                                           default=str))
        except Exception as e:  # noqa: BLE001
            z.writestr("config/active/error.txt", f"config unavailable: {type(e).__name__}")
        try:
            z.writestr("metrics.txt", REGISTRY.render())
        except Exception as e:  # noqa: BLE001
            z.writestr("metrics.txt", f"metrics unavailable: {type(e).__name__}")
        try:
            with get_engine().connect() as c:
                z.writestr("state/connections.json", json.dumps(_rows(c, select(connection)), indent=2))
                z.writestr("state/agents.json", json.dumps(_rows(c, select(
                    agent.c.id, agent.c.connection_id, agent.c.version, agent.c.hostname, agent.c.status,
                    agent.c.last_lri, agent.c.durable_pos, agent.c.spool_bytes, agent.c.metrics,
                    agent.c.last_heartbeat)), indent=2))
                z.writestr("state/batches_by_status.json", json.dumps(_rows(c, select(
                    batch.c.connection_id, batch.c.status, func.count().label("n"),
                    func.max(batch.c.received_at).label("last_received")).group_by(
                    batch.c.connection_id, batch.c.status)), indent=2))
                z.writestr("state/checkpoints.json", json.dumps(_rows(c, select(checkpoint_mirror)), indent=2))
                z.writestr("state/jobs.json", json.dumps(_rows(c, select(
                    job.c.kind, job.c.status, func.count().label("n")).group_by(job.c.kind, job.c.status)), indent=2))
                z.writestr("state/jobs_failed_recent.json", json.dumps(_rows(c, select(
                    job.c.id, job.c.kind, job.c.attempts, job.c.error, job.c.created_at).where(
                    job.c.error.is_not(None)).order_by(job.c.id.desc()).limit(50)), indent=2))
                z.writestr("state/quarantine_summary.json", json.dumps(_rows(c, select(
                    quarantine_event.c.connection_id, quarantine_event.c.table_fqn, quarantine_event.c.reason,
                    quarantine_event.c.status, func.count().label("n")).group_by(
                    quarantine_event.c.connection_id, quarantine_event.c.table_fqn, quarantine_event.c.reason,
                    quarantine_event.c.status)), indent=2))
                z.writestr("state/audit_recent.json", json.dumps(_rows(c, select(
                    audit_event.c.at, audit_event.c.actor, audit_event.c.action, audit_event.c.target)
                    .order_by(audit_event.c.at.desc()).limit(200)), indent=2))
        except Exception as e:  # noqa: BLE001 - a bundle must still be produced when the DB is down
            z.writestr("state/error.txt", f"metadata database unavailable: {type(e).__name__}: {e}")
        try:                                   # REQ-15: incident summary, strict redaction, no row values
            from alelt_diagnose.bundle import incidents_summary

            z.writestr("incidents.json", json.dumps(incidents_summary(get_engine(), limit=50), indent=2, default=str))
        except Exception as e:  # noqa: BLE001
            z.writestr("incidents.json", json.dumps({"error": f"{type(e).__name__}: {e}"}))
        for name, data in _recent_logs().items():
            z.writestr(f"logs/{name}", data)
        z.writestr("README.txt", "ALELT support bundle. Secrets are redacted; review before sharing.\n"
                   f"generated_at_epoch={int(time.time())}\n")
    return buf.getvalue()


@router.get("/api/v1/support/bundle")
def support_bundle(p: Principal = Depends(require_perm("hub.view"))) -> Response:
    data = build_bundle()
    with get_engine().begin() as c:
        store.audit(c, p.subject, "support.bundle", None, {"bytes": len(data)}, p.primary_role)
    name = f"alelt-support-{time.strftime('%Y%m%d-%H%M%S')}.zip"
    return Response(data, media_type="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


def register_product(app: FastAPI) -> None:
    """Setup wizard + enrollment, product endpoints, and (last) the console static files."""
    import logging

    from . import setup, webui

    logging.getLogger("alelt_hub.product").info("decoders: %s", json.dumps(decoder_info()),
                                             extra={"component": "alelt_hub"})

    app.include_router(router)
    app.include_router(setup.router)
    app.add_middleware(setup.EnrollmentBindingMiddleware)
    webui.mount(app)

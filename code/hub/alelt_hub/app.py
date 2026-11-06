"""FastAPI application: agent gateway + console API + health + metrics.

Run: uvicorn alelt_hub.app:app --host 0.0.0.0 --port 8080
"""
from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy import Connection, func, select, text

from alelt_common.models import TableRef
from alelt_common.util import setup_logging, utcnow
from alelt_loader import ConnectionPolicy, load_policy

from . import __version__, store
from .console import router as console_router
from .db import agent, batch, checkpoint_mirror, get_engine, init_db, job, source_table

from .gateway import router as agent_router
from .config_api import retention_hours
from .metrics import REGISTRY, Registry
from .settings import get_settings


_CODES = {400: "bad_request", 401: "unauthenticated", 403: "forbidden", 404: "not_found", 409: "conflict",
          413: "too_large", 422: "invalid", 426: "protocol", 503: "unavailable"}
_spool_cache: list[float] = [0.0, 0.0]   # [computed_at, bytes]


def _spool_size() -> float:
    now = time.monotonic()
    if now - _spool_cache[0] > 60:
        from .blobstore import used_bytes   # local dir or S3 bucket (X-46)

        _spool_cache[:] = [now, float(used_bytes())]
    return _spool_cache[1]


_policies: dict[str, tuple[float, ConnectionPolicy]] = {}


def _policy(c: Connection, cid: str) -> ConnectionPolicy:
    hit = _policies.get(cid)
    if hit is None or time.monotonic() - hit[0] > 60:
        hit = (time.monotonic(), store.connection_policy(c, cid, get_settings().tables_config))
        _policies[cid] = hit
    return hit[1]


def _collect(reg: Registry) -> None:
    s = get_settings()
    with get_engine().connect() as c:
        for kind in ("decode", "snapshot_load", "snapshot", "resync", "reprocess", "verify", "replay", "integrity", "maintain",
                     "completion", "lifecycle", "schema"):
            reg.set("alelt_queue_depth", 0, queue=kind)
        for kind, n in c.execute(select(job.c.kind, func.count()).where(job.c.status == "QUEUED")
                                 .group_by(job.c.kind)):
            reg.set("alelt_queue_depth", n, queue=kind)
        _collect_backlog(reg, c)                      # XC-27: unloaded batches, failed / stuck jobs, progress
        for st, cid, o, age, last_commit in c.execute(select(
                checkpoint_mirror.c.stream, checkpoint_mirror.c.connection_id, checkpoint_mirror.c.open_txns,
                checkpoint_mirror.c.oldest_open_s, checkpoint_mirror.c.last_commit_ts)):
            from .lag import effective_lag_s

            lag = effective_lag_s(c, cid, last_commit)               # HC-16: idle + caught up = 0
            reg.set("alelt_txn_open", o, stream=st)
            reg.set("alelt_txn_oldest_open_seconds", age, stream=st)
            if lag is not None:
                reg.set("alelt_capture_lag_seconds", max(0.0, float(lag)), connection_id=cid)
            reg.set("alelt_latency_target_seconds", s.latency_target_s, connection_id=cid)
            # X-129 archive horizon: how far back in the source log the hub may still need to read (capture lag,
            # oldest open transaction) vs the DBA-owned archive retention. horizon < 0 = logs already gone.
            need = max(float(lag or 0.0), float(age or 0))
            ret = retention_hours(c, cid) * 3600.0
            reg.set("alelt_archive_needed_age_seconds", need, connection_id=cid, stream=st)
            reg.set("alelt_archive_retention_seconds", ret, connection_id=cid)
            if ret > 0:
                reg.set("alelt_archive_horizon_seconds", ret - need, connection_id=cid, stream=st)
        for aid, hb, m, spool in c.execute(select(agent.c.id, agent.c.last_heartbeat, agent.c.metrics,
                                                  agent.c.spool_bytes).where(agent.c.last_heartbeat.is_not(None))):
            reg.set("alelt_agent_last_heartbeat_timestamp_seconds", hb.timestamp(), agent_id=aid)
            reg.set("alelt_agent_cpu_percent", float((m or {}).get("cpu_pct", 0)), agent_id=aid)
            reg.set("alelt_agent_mem_mb", float((m or {}).get("mem_mb", 0)), agent_id=aid)
            reg.set("alelt_spool_bytes", float(spool or 0), location="agent", agent_id=aid)
            for k, v in (m or {}).items():           # REQ-11: extra heartbeat metrics with X-16 names
                if k.startswith("alelt_agent_") and isinstance(v, (int, float)):
                    reg.set(k, float(v), agent_id=aid)
        for cid, sch, tab, cap in c.execute(select(source_table.c.connection_id, source_table.c.src_schema,
                                                   source_table.c.src_table, source_table.c.capture_enabled)):
            ref = TableRef(schema=sch, name=tab)
            if _policy(c, cid).table(ref).enabled:
                reg.set("alelt_capture_enabled", 1 if cap else 0, connection_id=cid, table=ref.fqn)
        from . import integrity                     # F-01: held tables + integrity state visible to operators

        integrity.collect(reg, c)
    reg.set("alelt_spool_bytes", _spool_size(), location="hub")
    from .setup import credential_expiries

    for aid, exp in credential_expiries():
        reg.set("alelt_agent_credential_expiry_timestamp_seconds", exp, agent_id=aid)
    for cid, n in _config_drift(s.tables_config):
        reg.set("alelt_config_file_drift", n, connection_id=cid)


def _collect_backlog(reg: Registry, c: Any) -> None:
    now = utcnow()

    def age(ts: Any) -> float:
        if ts is None:
            return 0.0
        ts = ts if ts.tzinfo else ts.replace(tzinfo=now.tzinfo)
        return max(0.0, (now - ts).total_seconds())

    for cid, n, oldest in c.execute(select(batch.c.connection_id, func.count(), func.min(batch.c.received_at))
                                    .where(batch.c.status.in_(("RECEIVED", "DECODED")))
                                    .group_by(batch.c.connection_id)):
        reg.set("alelt_batches_unloaded", n, connection_id=cid)
        reg.set("alelt_batch_oldest_unloaded_age_seconds", age(oldest), connection_id=cid)
    # RC4: decoder behind the edge - batches received and not decoded yet, age of the oldest of them
    lag = {str(cid): (int(n), oldest) for cid, n, oldest in c.execute(
        select(batch.c.connection_id, func.count(), func.min(batch.c.received_at))
        .where(batch.c.status == "RECEIVED").group_by(batch.c.connection_id))}
    for (cid,) in c.execute(select(checkpoint_mirror.c.connection_id).group_by(checkpoint_mirror.c.connection_id)):
        n, oldest = lag.get(str(cid), (0, None))
        reg.set("alelt_decode_lag_batches", n, connection_id=cid)
        reg.set("alelt_decode_lag_seconds", age(oldest), connection_id=cid)
    for kind in ("decode", "snapshot_load", "snapshot", "resync", "reprocess", "verify", "replay", "integrity",
                 "maintain", "completion", "lifecycle", "schema", "attach"):
        reg.set("alelt_jobs_failed", 0, kind=kind)
        reg.set("alelt_job_oldest_failed_age_seconds", 0, kind=kind)
        reg.set("alelt_job_stuck", 0, kind=kind)
    for kind, n, oldest in c.execute(select(job.c.kind, func.count(), func.min(job.c.created_at))
                                     .where(job.c.status == "FAILED").group_by(job.c.kind)):
        reg.set("alelt_jobs_failed", n, kind=kind)
        reg.set("alelt_job_oldest_failed_age_seconds", age(oldest), kind=kind)
    for kind, n in c.execute(select(job.c.kind, func.count()).where(job.c.status == "QUEUED",
                                                                     job.c.error.like("[stuck%"))
                             .group_by(job.c.kind)):
        reg.set("alelt_job_stuck", n, kind=kind)
    for st, seq in c.execute(select(checkpoint_mirror.c.stream, checkpoint_mirror.c.last_seq)):
        reg.set("alelt_checkpoint_seq", float(seq or 0), stream=st)


_drift_cache: list[Any] = [0.0, []]


def _config_drift(path: str | None) -> list[tuple[str, int]]:
    """F-18: 1 per ALELT_TABLES_CONFIG document that differs from the active revision (not applied), cached 60 s."""
    if time.monotonic() - _drift_cache[0] > 60:
        from .config_store import drift

        with get_engine().connect() as c:
            _drift_cache[:] = [time.monotonic(), [(d["connection_id"], 1) for d in drift(c, path)
                                                  if d["state"] != "NOT_IMPORTED"]]
    return list(_drift_cache[1])


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    s = get_settings()
    setup_logging(s.log_level)
    s.spool_dir.mkdir(parents=True, exist_ok=True)
    init_db()                               # versioned migrations / downgrade guard (F-24)
    from .config_store import seed_from_files   # F-18: files are imported once, never a runtime override

    seed_from_files(get_engine(), s.tables_config)
    from .ha import start_node_heartbeat   # X-49

    beat = start_node_heartbeat(get_engine(), "api")
    yield
    beat.set()


def create_app() -> FastAPI:
    app = FastAPI(title="ALELT hub", version=__version__, lifespan=lifespan)
    @app.middleware("http")
    async def correlation(request: Request, call_next: Any) -> Response:
        cid = request.headers.get("x-alelt-correlation-id") or str(uuid.uuid4())
        tok = store.CORRELATION_ID.set(cid)
        try:
            resp: Response = await call_next(request)
        finally:
            store.CORRELATION_ID.reset(tok)
        resp.headers["X-ALELT-Correlation-Id"] = cid
        return resp

    @app.exception_handler(StarletteHTTPException)
    async def api_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        """ApiError body {error:{code,message,correlation_id}} (contract.ts); `detail` kept for agents."""
        d = exc.detail
        if isinstance(d, dict) and "code" in d:
            code, msg = str(d["code"]), str(d.get("message", ""))
        else:
            code = _CODES.get(exc.status_code, str(exc.status_code))
            msg = d if isinstance(d, str) else json.dumps(d)
        return JSONResponse({"error": {"code": code, "message": msg,
                                       "correlation_id": request.headers.get("x-alelt-correlation-id")},
                             "detail": d}, status_code=exc.status_code, headers=getattr(exc, "headers", None))

    from .security import install_validation_handler   # AHS-22: 422 never echoes the submitted input

    install_validation_handler(app)
    app.include_router(agent_router)
    app.include_router(console_router)
    from .importer import router as importer_router   # migration importer + pipeline wizards + prod guard

    app.include_router(importer_router)
    from .config_api import router as config_router   # config revisions <-> YAML (F-18)

    app.include_router(config_router)
    from .diagnose_api import install as install_diagnose   # incidents, explain, AI assistant settings

    install_diagnose(app)
    from .ha import router as lease_router            # capture lease (agent active/passive, X-47)
    from .topology import router as topology_router   # console Topology & HA (X-50)

    app.include_router(lease_router)
    app.include_router(topology_router)
    REGISTRY.collector(_collect)

    @app.get("/healthz", tags=["ops"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["ops"])
    def readyz() -> JSONResponse:
        try:
            with get_engine().connect() as c:
                c.execute(text("SELECT 1"))
            from .blobstore import ready as spool_ready

            ok = spool_ready()
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"status": "not ready", "error": type(e).__name__}, status_code=503)
        return JSONResponse({"status": "ready" if ok else "no spool"}, status_code=200 if ok else 503)

    @app.get("/metrics", tags=["ops"], response_class=PlainTextResponse)
    def metrics() -> str:
        return REGISTRY.render()

    from .product import register_product   # setup wizard, enrollment, version, support bundle, console static

    register_product(app)
    from .security import PeerCaptureMiddleware, SecurityMiddleware

    app.add_middleware(SecurityMiddleware)           # CORS / CSRF / rate limits / headers
    app.add_middleware(PeerCaptureMiddleware)        # outermost: real TCP peer, then X-Forwarded-For (F-14)
    return app


app = create_app()

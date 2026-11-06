"""Console REST API /api/v1 — paths, shapes and permissions per console/API_CONTRACT.md + src/api/contract.ts.

Server-side rules: permission matrix (auth.ROLE_PERMISSIONS), `reason` on every mutation (400 REASON_REQUIRED),
`confirm_text` = "<ACTION> <target>" on destructive calls (400 CONFIRM_MISMATCH), two-person rule on switches
(409 TWO_PERSON_RULE), one audit_event per mutation.
"""
from __future__ import annotations

import csv
import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy import Connection, Select, delete, func, or_, select, update

from alelt_common.models import CommandName, Limits, PROBES, TableRef
from alelt_common.util import utcnow
from alelt_loader import load_policy
from alelt_loader.policy import ConnectionPolicy

from . import certification as cert_evidence, config_store, store
from .dialect import insert, seconds_since
from .auth import Principal, current_principal, require_perm
from .db import (
    agent,
    agent_command,
    app_user,
    audit_event,
    batch,
    checkpoint_mirror,
    column_config,
    config_revision,
    connection,
    get_engine,
    group_mapping,
    hub_setting,
    import_run,
    job,
    quarantine_event,
    recon_result,
    recon_run,
    role_assignment,
    schema_change_event,
    signoff,
    source_column,
    source_table,
    switch_request,
    switch_state,
    table_config,
    wave,
)
from .settings import get_settings

router = APIRouter(prefix="/api/v1", tags=["console"])
_LADDER = Path(__file__).resolve().parents[2] / "tests" / "ladder"
SYNC_MODES = {"SOFT_DELETE": "LOG", "LOG": "LOG", "HISTORY": "HISTORY", "SNAPSHOT_ONLY": "SNAPSHOT_ONLY"}
CERT_CRITERIA = ("ROW_COUNT_PARITY", "PK_SET_PARITY", "COLUMN_CHECKSUM_PARITY", "DELETE_PARITY", "LATENCY_P95",
                 "DBT_SHADOW_DIFF", "CLEAN_DAYS_7")
CHECK_TO_CRITERION = {"ROW_COUNT": "ROW_COUNT_PARITY", "KEY_SET": "PK_SET_PARITY",
                      "COLUMN_CHECKSUM": "COLUMN_CHECKSUM_PARITY", "DELETES": "DELETE_PARITY",
                      "DBT_SHADOW": "DBT_SHADOW_DIFF"}
RECON_CHECKS = ("ROW_COUNT", "KEY_SET", "COLUMN_CHECKSUM", "DELETES", "LEGACY_PARITY")
ACTIVE_SOURCES = ("LEGACY", "ALELT")
SWITCH_TTL = timedelta(hours=4)
RETENTION_DEFAULT = {"spool_cap_bytes": 50 << 30, "raw_archive_days": 30}


# ------------------------------------------------------------------ helpers
def err(code: int, name: str, message: str) -> HTTPException:
    """HTTPException carrying a contract error code (app.py renders {"error": {code, message, correlation_id}})."""
    return HTTPException(code, {"code": name, "message": message})


def need_reason(reason: str | None) -> str:
    if not reason or not reason.strip():
        raise err(400, "REASON_REQUIRED", "a non-empty reason is required")
    return reason.strip()


def confirm_text(action: str, target: str) -> str:
    """Same rule as console expectedConfirmText()."""
    return f"{action.upper()} {target}"


def need_confirm(action: str, target: str, given: str | None) -> None:
    want = confirm_text(action, target)
    if (given or "").strip() != want:
        raise err(400, "CONFIRM_MISMATCH", f"confirm_text must be '{want}'")


def _iso(v: datetime | None) -> str | None:
    return v.isoformat().replace("+00:00", "Z") if v else None


def _pos(engine: str | None, text: str | None) -> dict[str, str] | None:
    return {"engine": engine or "", "text": text} if text else None


def _uuid(v: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(v))
    except ValueError:
        raise err(404, "NOT_FOUND", f"unknown id {v}") from None


def _page(stmt: Select[Any], cursor: str | None, limit: int) -> tuple[list[dict[str, Any]], int, str | None]:
    off = int(cursor or 0)
    with get_engine().connect() as c:
        total = c.execute(select(func.count()).select_from(stmt.order_by(None).subquery())).scalar_one()
        rows = [dict(r) for r in c.execute(stmt.offset(off).limit(limit)).mappings().all()]
    return rows, int(total), (str(off + limit) if off + limit < total else None)


_pol_cache: dict[str, tuple[float, ConnectionPolicy]] = {}


def _policy(cid: str, conn: Connection | None = None) -> ConnectionPolicy:
    """Cached policy; pass the caller's open connection (SQLite: a nested connection would wait on the caller's
    BEGIN IMMEDIATE lock)."""
    hit = _pol_cache.get(cid)
    if hit is None or time.monotonic() - hit[0] > 5:
        if conn is not None:
            hit = (time.monotonic(), store.connection_policy(conn, cid, get_settings().tables_config))
        else:
            with get_engine().connect() as c:
                hit = (time.monotonic(), store.connection_policy(c, cid, get_settings().tables_config))
        _pol_cache[cid] = hit
    return hit[1]


def _setting(c: Connection, key: str, default: dict[str, Any]) -> dict[str, Any]:
    v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar()
    return {**default, **(v or {})}


class Mutation(BaseModel):
    reason: str | None = None


class Destructive(Mutation):
    confirm_text: str | None = None


# ------------------------------------------------------------------ identity
@router.get("/me")
def me(p: Principal = Depends(current_principal)) -> dict[str, Any]:
    return {"oid": p.subject, "name": p.name or p.subject, "email": p.email or p.subject, "dev": p.dev,
            "roles": sorted(r.value for r in p.roles)}


# ------------------------------------------------------------------ overview / ladder
def _ladder() -> list[dict[str, Any]]:
    """Formal levels only, each flagged `stale` / `evidence` (alelt_hub.ladder); supporting files are not levels."""
    from .ladder import index

    return index(_LADDER)


@router.get("/ladder")
def ladder(_: Principal = Depends(require_perm("view.dashboard"))) -> list[dict[str, Any]]:
    return _ladder()


@router.get("/overview")
def overview(_: Principal = Depends(require_perm("view.dashboard"))) -> dict[str, Any]:
    s = get_settings()
    with get_engine().connect() as c:
        last = c.execute(select(batch.c.id, batch.c.received_at, batch.c.to_lri, batch.c.engine)
                         .order_by(batch.c.seq.desc()).limit(1)).first()
        from .lag import lags as _eff_lags

        _all = [v for v in _eff_lags(c, c.execute(select(checkpoint_mirror.c.connection_id,
                                                         checkpoint_mirror.c.last_commit_ts)).all()).values()
                if v is not None]
        lag = max(_all) if _all else None
        conn_id = c.execute(select(connection.c.id).order_by(connection.c.created_at).limit(1)).scalar()
        failed = c.execute(select(func.count()).select_from(job).where(job.c.status == "FAILED")).scalar_one()
        q_open = c.execute(select(func.count()).select_from(quarantine_event)
                           .where(quarantine_event.c.status == "OPEN")).scalar_one()
        n_tables = c.execute(select(func.count()).select_from(table_config)
                             .where(table_config.c.enabled.is_(True))).scalar_one()
        recon = c.execute(select(recon_result.c.table_id, recon_result.c.outcome)).all()
        p95 = cert_evidence.summary(c, conn_id)["latency_p95_seconds"] if conn_id else None   # F-19
    lag_s = float(lag or 0.0)
    bad = {t for t, o in recon if o != "MATCH"}
    checked = {t for t, _ in recon}
    pol = _policy(conn_id) if conn_id else load_policy("", s.tables_config)
    health = "DOWN" if failed else "DEGRADED" if (q_open or lag_s > s.latency_target_s) else "OK" if last else "UNKNOWN"
    return {
        "pipeline_health": health,
        "capture_lag_seconds": lag_s,
        "capture_lag_lri_distance": None,
        "last_batch": {"batch_id": str(last[0]), "received_at": _iso(last[1]), "to_pos": _pos(last[3], last[2])}
        if last else None,
        "latency_p95_seconds": p95,            # recorded commit -> apply p95 (24 h); None = no evidence
        "latency_target_seconds": s.latency_target_s,
        "open_alerts": [],                     # alerts are owned by Alertmanager (deploy/hub/observability)
        "parity_score": (len(checked - bad) / len(checked)) if checked else 0.0,
        "tables_total": int(n_tables),
        "tables_in_sync": int(n_tables) if last and lag_s <= s.latency_target_s else 0,
        "output_profile": pol.output_profile,
        "system_columns": {"synced": pol.system.synced, "deleted": pol.system.deleted},
        "ladder": _ladder(),
    }


# ------------------------------------------------------------------ tables
def _table_stmt() -> Select[Any]:
    return (select(source_table.c.id, source_table.c.connection_id, source_table.c.src_schema,
                   source_table.c.src_table, source_table.c.dest_name, source_table.c.capture_enabled,
                   table_config.c.enabled, table_config.c.sync_mode, table_config.c.revision,
                   switch_state.c.active_source)
            .join(table_config, table_config.c.table_id == source_table.c.id, isouter=True)
            .join(switch_state, switch_state.c.table_id == source_table.c.id, isouter=True)
            .order_by(source_table.c.src_schema, source_table.c.src_table))


def _lags(c: Connection | None = None) -> dict[str, float | None]:
    """Capture lag per connection; pass the caller's connection when inside a transaction."""
    if c is None:
        with get_engine().connect() as own:
            return _lags(own)
    from .lag import lags as _eff_lags

    return _eff_lags(c, c.execute(select(checkpoint_mirror.c.connection_id, checkpoint_mirror.c.last_commit_ts)).all())


def _certified_ids(c: Connection) -> set[uuid.UUID]:
    return {r[0] for r in c.execute(select(signoff.c.table_id).where(signoff.c.criterion == "ALL"))}


def _integrity(c: Connection | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    """(connection, fqn) -> integrity state + holds (alelt_hub.integrity, F-01): why a table stopped progressing."""
    from . import integrity

    if c is None:
        with get_engine().connect() as own:
            return integrity.table_states(own)
    return integrity.table_states(c)


def _summary(r: dict[str, Any], lag: dict[str, float | None], certified: set[uuid.UUID],
             integ: dict[tuple[str, str], dict[str, Any]] | None = None) -> dict[str, Any]:
    s = get_settings()
    ref = TableRef(schema=r["src_schema"], name=r["src_table"])
    pol = _policy(r["connection_id"])
    enabled = bool(r["enabled"]) if r["revision"] is not None else pol.table(ref).enabled
    lg = lag.get(r["connection_id"])
    state = ("DISABLED" if not enabled else "ERROR" if not r["capture_enabled"] else "NEW" if lg is None
             else "IN_SYNC" if lg <= s.latency_target_s else "CATCHING_UP")
    it = (integ or {}).get((r["connection_id"], ref.fqn)) or {}
    conn_it = (integ or {}).get((r["connection_id"], "*")) or {}
    istate = it.get("integrity") or "IN_SYNC"
    if enabled and (istate != "IN_SYNC" or conn_it.get("held_by")):
        state = "ERROR"                       # integrity hold / suspect: never shown as IN_SYNC
    elif enabled and "snapshot" in (it.get("held_by") or []):
        state = "SNAPSHOTTING"
    elif enabled and it.get("held_by"):
        state = "PAUSED"                      # schedule window closed / deferred round pending
    integrity_view = {"state": istate, "reason": it.get("reason"), "since": it.get("since"),
                      "incident_id": it.get("incident_id"), "step": it.get("step"),
                      "connection_state": conn_it.get("integrity") or "IN_SYNC",
                      "connection_reason": conn_it.get("reason")}
    return {"id": str(r["id"]), "table": {"schema": ref.schema_, "name": ref.name},
            "integrity": integrity_view, "held_by": it.get("held_by") or [], "holds": it.get("holds") or [],
            "phase": it.get("phase"), "pending_completions": int(it.get("pending_completions") or 0),
            "dest_name": r["dest_name"] or pol.dest_name(ref), "enabled": enabled,
            "sync_mode": SYNC_MODES.get(r["sync_mode"] or pol.table(ref).sync_mode, "LOG"), "state": state,
            "lag_seconds": lg, "rows_per_day": 0, "active_source": r["active_source"] or "LEGACY",
            "certification": "CERTIFIED" if r["id"] in certified else "NOT_STARTED",
            "capture_enabled": bool(r["capture_enabled"])}


@router.get("/tables")
def tables(connection_id: str | None = None, cursor: str | None = None, limit: int = Query(200, le=1000),
           _: Principal = Depends(require_perm("view.tables"))) -> dict[str, Any]:
    stmt = _table_stmt()
    if connection_id:
        stmt = stmt.where(source_table.c.connection_id == connection_id)
    rows, total, nxt = _page(stmt, cursor, limit)
    lag = _lags()
    with get_engine().connect() as c:
        cert = _certified_ids(c)
        integ = _integrity(c)
    return {"items": [_summary(r, lag, cert, integ) for r in rows], "total": total, "next_cursor": nxt}


def _recon(x: Any, ref: dict[str, str] | None) -> dict[str, Any]:
    return {"id": str(x["id"]), "run_id": str(x["run_id"]), "table_id": str(x["table_id"]), "table": ref,
            "check_kind": x["check_kind"], "outcome": x["outcome"], "detail": x["detail"] or "",
            "at": _iso(x["created_at"]), "drill": x.get("drill")}


def _detail(table_id: uuid.UUID) -> dict[str, Any]:
    with get_engine().connect() as c:
        r = c.execute(_table_stmt().where(source_table.c.id == table_id)).mappings().first()
        if r is None:
            raise err(404, "NOT_FOUND", "unknown table")
        cols = c.execute(select(source_column, column_config.c.enabled, column_config.c.hashed,
                                column_config.c.revision)
                         .join(column_config, column_config.c.column_id == source_column.c.id, isouter=True)
                         .where(source_column.c.table_id == table_id).order_by(source_column.c.ordinal)).mappings().all()
        ck = c.execute(select(checkpoint_mirror.c.position).where(
            checkpoint_mirror.c.connection_id == r["connection_id"])).scalar()
        hist = c.execute(select(schema_change_event).where(schema_change_event.c.table_id == table_id)
                         .order_by(schema_change_event.c.created_at.desc()).limit(50)).mappings().all()
        rec = c.execute(select(recon_result, recon_run.c.check_kind)
                        .join(recon_run, recon_run.c.id == recon_result.c.run_id, isouter=True)
                        .where(recon_result.c.table_id == table_id)
                        .order_by(recon_result.c.created_at.desc()).limit(50)).mappings().all()
        eng = c.execute(select(connection.c.engine).where(connection.c.id == r["connection_id"])).scalar()
        cert = _certified_ids(c)
    d = dict(r)
    ref = {"schema": d["src_schema"], "name": d["src_table"]}
    tp = _policy(d["connection_id"]).table(TableRef(schema=d["src_schema"], name=d["src_table"]))

    def flag(x: Any, name: str) -> bool:
        if x["revision"] is not None and x[name] is not None:
            return bool(x[name])                       # console override
        cp = tp.columns.get(x["name"])
        return bool(getattr(cp, name)) if cp is not None else name == "enabled"
    out = _summary(d, _lags(), cert, _integrity())
    out.update({
        "revision": d["revision"] or 0,
        "columns": [{"name": x["name"], "ordinal": x["ordinal"], "engine_type": x["engine_type"],
                     "canonical_type": x["canonical_type"], "length": x["length"], "scale": x["scale"],
                     "nullable": bool(x["nullable"]), "key_seq": x["key_seq"],
                     "enabled": flag(x, "enabled"), "hashed": flag(x, "hashed")} for x in cols],
        "schema_history": [_schema_change(h, ref, eng) for h in hist],
        "recon": [_recon(x, ref) for x in rec],
        "checkpoint": _pos(eng, ck),
    })
    return out


@router.get("/tables/{table_id}")
def table_detail(table_id: str, _: Principal = Depends(require_perm("view.tables"))) -> dict[str, Any]:
    return _detail(_uuid(table_id))


class ColumnUpdate(BaseModel):
    name: str
    enabled: bool
    hashed: bool


class TableConfigUpdate(Mutation):
    enabled: bool | None = None
    sync_mode: str | None = Field(default=None, pattern="^(LOG|SNAPSHOT_ONLY|HISTORY)$")
    columns: list[ColumnUpdate] = Field(default_factory=list)


def _config_state(c: Connection) -> dict[str, Any]:
    """Console overrides currently in force (rows that carry a revision), keyed 'S.T'."""
    out: dict[str, Any] = {}
    for s, t, en, sm in c.execute(select(source_table.c.src_schema, source_table.c.src_table, table_config.c.enabled,
                                         table_config.c.sync_mode)
                                  .join(table_config, table_config.c.table_id == source_table.c.id)
                                  .where(table_config.c.revision.is_not(None))):
        out.setdefault(f"{s}.{t}", {}).update({"enabled": en, "sync_mode": sm})
    for s, t, n, en, h in c.execute(select(source_table.c.src_schema, source_table.c.src_table, source_column.c.name,
                                           column_config.c.enabled, column_config.c.hashed)
                                    .join(source_column, source_column.c.table_id == source_table.c.id)
                                    .join(column_config, column_config.c.column_id == source_column.c.id)
                                    .where(column_config.c.revision.is_not(None))):
        out.setdefault(f"{s}.{t}", {}).setdefault("columns", {})[n] = {"enabled": en, "hashed": h}
    return out


def _new_revision(c: Connection, author: str, reason: str, source: str, change: dict[str, Any]) -> int:
    rid = c.execute(insert(config_revision).values(author=author, reason=reason,
                                                   body={"source": source, "change": change, "state": {}})
                    .returning(config_revision.c.id)).scalar_one()
    return int(rid)


def _commit_table_doc(c: Connection, rev: int, tid: uuid.UUID, change: dict[str, Any]) -> None:
    """F-18: the console edit becomes the connection's new full config document in revision `rev`."""
    r = c.execute(select(source_table.c.connection_id, source_table.c.src_schema, source_table.c.src_table)
                  .where(source_table.c.id == tid)).one()
    doc = config_store.apply_table_change(config_store.base_doc(c, r[0], get_settings().tables_config),
                                          r[1], r[2], change)
    try:
        config_store.put_doc(c, rev, r[0], doc)
    except config_store.ConfigError as e:
        raise err(409, "INVALID_CONFIG", str(e)) from None
    _pol_cache.pop(r[0], None)


def _seal_revision(c: Connection, rid: int) -> None:
    """Store the full override state after the change, so diff/rollback/export work from any revision."""
    body = c.execute(select(config_revision.c.body).where(config_revision.c.id == rid)).scalar_one()
    c.execute(update(config_revision).where(config_revision.c.id == rid).values(
        body={**body, "state": _config_state(c)}))


@router.put("/tables/{table_id}/config")
def put_table_config(table_id: str, body: TableConfigUpdate,
                     p: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    tid = _uuid(table_id)
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        t = c.execute(select(source_table.c.capture_enabled).where(source_table.c.id == tid)).first()
        if t is None:
            raise err(404, "NOT_FOUND", "unknown table")
        if body.enabled and not t[0]:
            raise err(409, "CAPTURE_DISABLED", "change capture is not enabled on this source table")
        cols = {r[0]: (r[1], r[2]) for r in c.execute(select(source_column.c.name, source_column.c.id,
                                                             source_column.c.key_seq)
                                                      .where(source_column.c.table_id == tid))}
        for cu in body.columns:
            if cu.name not in cols:
                raise err(404, "NOT_FOUND", f"unknown column {cu.name}")
            if cols[cu.name][1] and (not cu.enabled or cu.hashed):
                raise err(409, "KEY_COLUMN", f"key column {cu.name} cannot be disabled or hashed")
        if body.sync_mode == "HISTORY":
            raise err(409, "UNSUPPORTED", "HISTORY sync mode is out of scope for this release (X-127)")
        change = {"table_id": str(tid), **body.model_dump(exclude={"reason"})}
        rev = _new_revision(c, p.subject, reason, "console", change)
        _commit_table_doc(c, rev, tid, change)
        tv: dict[str, Any] = {"revision": rev}
        if body.enabled is not None:
            tv["enabled"] = body.enabled
        if body.sync_mode is not None:
            tv["sync_mode"] = "SOFT_DELETE" if body.sync_mode == "LOG" else body.sync_mode
        c.execute(update(table_config).where(table_config.c.table_id == tid).values(**tv))
        for cu in body.columns:
            c.execute(update(column_config).where(column_config.c.column_id == cols[cu.name][0]).values(
                enabled=cu.enabled, hashed=cu.hashed, revision=rev))
        _seal_revision(c, rev)
        store.audit(c, p.subject, "table.config.update", str(tid), {"revision": rev, **change, "reason": reason},
                    p.primary_role)
    return _detail(tid)


def _table_fqn(c: Connection, tid: uuid.UUID) -> tuple[str, str]:
    r = c.execute(select(source_table.c.src_schema, source_table.c.src_table, source_table.c.connection_id)
                  .where(source_table.c.id == tid)).first()
    if r is None:
        raise err(404, "NOT_FOUND", "unknown table")
    return f"{r[0]}.{r[1]}", str(r[2])


_DEDUP_FIELDS = ("table_id", "table_ids", "connection_id", "from_pos", "to_pos", "check_kind")


def _pending_like(c: Connection, kind: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """A QUEUED / RUNNING job of the same kind for the same target (HC-28: a double click is one job)."""
    want = {k: payload.get(k) for k in _DEDUP_FIELDS}
    for jid, pl in c.execute(select(job.c.id, job.c.payload).where(job.c.kind == kind,
                                                                  job.c.status.in_(("QUEUED", "RUNNING")))):
        if isinstance(pl, dict) and {k: pl.get(k) for k in _DEDUP_FIELDS} == want:
            return str(jid), pl
    return None


def _enqueue(c: Connection, kind: str, payload: dict[str, Any]) -> str:
    same = _pending_like(c, kind, payload)
    if same is not None:
        return same[0]
    return str(c.execute(insert(job).values(kind=kind, payload=payload).returning(job.c.id)).scalar_one())


@router.post("/jobs/{job_id}/retry")
def retry_job(job_id: int, body: Mutation, p: Principal = Depends(require_perm("table.reprocess"))) -> dict[str, Any]:
    """Run a FAILED / backed-off job now with a fresh attempt budget (XC-10)."""
    reason = need_reason(body.reason)
    from . import jobs as _jobs

    if not _jobs.retry(get_engine(), job_id):
        raise err(404, "NOT_FOUND", "no FAILED or QUEUED job with this id")
    with get_engine().begin() as c:
        store.audit(c, p.subject, "job.retry", str(job_id), {"reason": reason}, p.primary_role)
    return {"job_id": str(job_id), "status": "QUEUED"}


@router.post("/tables/{table_id}/resync")
def resync(table_id: str, body: Destructive, p: Principal = Depends(require_perm("table.resync"))) -> dict[str, str]:
    tid = _uuid(table_id)
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        fqn, cid = _table_fqn(c, tid)
        need_confirm("resync", fqn, body.confirm_text)
        jid = _enqueue(c, "resync", {"table_id": str(tid), "table": fqn, "connection_id": cid})  # alelt_snapshot
        store.audit(c, p.subject, "table.resync", fqn, {"job_id": jid, "reason": reason}, p.primary_role)
    return {"job_id": jid}


class ReprocessRequest(Destructive):
    from_pos: str | None = None
    to_pos: str | None = None


@router.post("/tables/{table_id}/reprocess")
def reprocess(table_id: str, body: ReprocessRequest,
              p: Principal = Depends(require_perm("table.reprocess"))) -> dict[str, str]:
    tid = _uuid(table_id)
    reason = need_reason(body.reason)
    if not body.from_pos or not body.to_pos:
        raise err(400, "POSITION_REQUIRED", "from_pos and to_pos are required")
    with get_engine().begin() as c:
        fqn, cid = _table_fqn(c, tid)
        need_confirm("reprocess", fqn, body.confirm_text)
        jid = _enqueue(c, "reprocess", {"table_id": str(tid), "table": fqn, "connection_id": cid,
                                        "from_pos": body.from_pos, "to_pos": body.to_pos})
        store.audit(c, p.subject, "table.reprocess", fqn, {"job_id": jid, "reason": reason,
                                                            "from_pos": body.from_pos, "to_pos": body.to_pos},
                    p.primary_role)
    return {"job_id": jid}


# ------------------------------------------------------------------ batches & quarantine
@router.get("/batches")
def batches(connection_id: str | None = None, status_: str | None = Query(None, alias="status"),
            cursor: str | None = None, limit: int = Query(100, le=1000),
            _: Principal = Depends(require_perm("view.recon"))) -> dict[str, Any]:
    stmt = select(batch).order_by(batch.c.seq.desc())
    if connection_id:
        stmt = stmt.where(batch.c.connection_id == connection_id)
    if status_:
        stmt = stmt.where(batch.c.status == status_)
    rows, total, nxt = _page(stmt, cursor, limit)
    return {"items": [{
        "batch_id": str(b["id"]), "agent_id": b["agent_id"], "connection_id": b["connection_id"],
        "from_pos": _pos(b["engine"], b["from_lri"]), "to_pos": _pos(b["engine"], b["to_lri"]),
        "rec_count": b["rec_count"], "raw_bytes": b["raw_bytes"], "sha256": b["sha256"], "status": b["status"],
        "received_at": _iso(b["received_at"]),
        "loaded_at": _iso(b["updated_at"]) if b["status"] == "LOADED" else None} for b in rows],
        "total": total, "next_cursor": nxt}


def _tref(fqn: str | None) -> dict[str, str] | None:
    if not fqn:
        return None
    s, _, n = fqn.partition(".")
    return {"schema": s, "name": n}


@router.get("/quarantine")
def quarantine(status_: str | None = Query("OPEN", alias="status"), cursor: str | None = None,
               limit: int = Query(100, le=1000), _: Principal = Depends(require_perm("view.recon"))) -> dict[str, Any]:
    stmt = select(quarantine_event, batch.c.engine).join(batch, batch.c.id == quarantine_event.c.batch_id,
                                                         isouter=True).order_by(quarantine_event.c.created_at.desc())
    if status_:
        stmt = stmt.where(quarantine_event.c.status == status_)
    rows, total, nxt = _page(stmt, cursor, limit)
    return {"items": [{"id": str(q["id"]), "position": _pos(q["engine"], q["lri"]), "table": _tref(q["table_fqn"]),
                       "reason": q["reason"], "raw_ref": q["raw_ref"], "raw_hex": q["raw_hex"],
                       "at": _iso(q["created_at"]), "status": q["status"]} for q in rows],
            "total": total, "next_cursor": nxt}


class QuarantineAction(Mutation):
    status: str = Field(pattern="^(REPROCESSED|IGNORED)$")


@router.post("/quarantine/{qid}/status")
def set_quarantine_status(qid: str, body: QuarantineAction,
                          p: Principal = Depends(require_perm("table.reprocess"))) -> dict[str, bool]:
    """Hub extension (not in contract v1): mark a quarantine item handled."""
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        r = c.execute(update(quarantine_event).where(quarantine_event.c.id == _uuid(qid)).values(status=body.status))
        if not r.rowcount:
            raise err(404, "NOT_FOUND", "unknown quarantine event")
        store.audit(c, p.subject, "quarantine.status", qid, {"status": body.status, "reason": reason}, p.primary_role)
    return {"ok": True}


# ------------------------------------------------------------------ agents
@router.get("/agents")
def agents(_: Principal = Depends(require_perm("agent.view"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        rows = c.execute(select(agent, connection.c.engine, connection.c.db_database)
                         .join(connection, connection.c.id == agent.c.connection_id).order_by(agent.c.id)
                         ).mappings().all()
        gov = c.execute(select(audit_event.c.target, audit_event.c.at, audit_event.c.detail)
                        .where(audit_event.c.action.like("agent.event.GOVERNOR%"))
                        .order_by(audit_event.c.at.desc()).limit(200)).all()
    out = []
    for a in rows:
        m = a["metrics"] or {}
        eng = a["engine"] or ""
        out.append({
            "id": a["id"], "connection_id": a["connection_id"], "engine": eng, "hostname": a["hostname"] or "",
            "database": a["db_database"] or "", "version": a["version"] or "", "state": a["status"],
            "last_heartbeat_at": _iso(a["last_heartbeat"]), "last_read_pos": _pos(eng, a["last_lri"]),
            "durable_pos": _pos(eng, a["durable_pos"]), "spool_bytes": a["spool_bytes"] or 0,
            "spool_capacity_bytes": int(m.get("spool_capacity_bytes", 0)), "cpu_pct": m.get("cpu_pct", 0.0),
            "mem_mb": m.get("mem_mb", 0.0), "limits": a["limits"] or Limits().model_dump(),
            "cert_thumbprint": a["cert_thumbprint"] or "", "cert_expires_at": None,
            "governor_events": [{"at": _iso(at), "kind": (d or {}).get("kind", "THROTTLE"),
                                 "detail": json.dumps(d or {})} for tgt, at, d in gov if tgt == a["id"]][:20]})
    return out


DESTRUCTIVE_COMMANDS = {CommandName.PAUSE: "pause", CommandName.SHUTDOWN: "shutdown"}


class AgentCommandRequest(Destructive):
    name: str
    args: dict[str, Any] = Field(default_factory=dict)


@router.post("/agents/{agent_id}/commands")
def post_command(agent_id: str, body: AgentCommandRequest,
                 p: Principal = Depends(require_perm("agent.command"))) -> dict[str, Any]:
    try:
        name = CommandName(body.name)
    except ValueError:
        raise err(400, "COMMAND_NOT_WHITELISTED", f"{body.name} is not a whitelisted command (R5)") from None
    reason = need_reason(body.reason)
    if name in DESTRUCTIVE_COMMANDS:
        need_confirm(DESTRUCTIVE_COMMANDS[name], agent_id, body.confirm_text)
    args = dict(body.args)
    if name is CommandName.PROBE and args.get("name") not in PROBES:
        raise err(400, "COMMAND_NOT_WHITELISTED", f"probe must be one of {PROBES}")
    if name is CommandName.START_CAPTURE:
        # REQ-10: explicit position, or {"resume": true} = agent resumes (local/hub durable position, else log end)
        if args.get("resume") is True and "from_pos" not in args:
            args = {}
        elif not isinstance(args.get("from_pos"), str) or not args["from_pos"].strip():
            raise err(400, "INVALID_ARGS", "START_CAPTURE needs args.from_pos (string) or args.resume=true")
        if "discard_spool" in args:
            # AHS-05: operator recovery of a spool blocked by a 409 gap -- drops unacknowledged edge batches
            if not isinstance(args["discard_spool"], bool) or "from_pos" not in args:
                raise err(400, "INVALID_ARGS", "discard_spool must be a boolean and needs args.from_pos")
            if args["discard_spool"]:
                need_confirm("discard spool", agent_id, body.confirm_text)
    with get_engine().begin() as c:
        cur = c.execute(select(agent.c.limits, connection.c.engine).join(
            connection, connection.c.id == agent.c.connection_id, isouter=True).where(agent.c.id == agent_id)).first()
        if cur is None:
            raise err(404, "NOT_FOUND", "unknown agent")
        if name is CommandName.START_CAPTURE and "from_pos" in args:
            # XC-17: the position is stored in the engine's canonical text (agents normalise before X-ALELT-Reposition)
            src = store.source_connector(cur[1]) if cur[1] else None
            if src is not None:
                try:
                    args["from_pos"] = src.parse_position(args["from_pos"].strip()).text
                except Exception as e:  # noqa: BLE001 - engine parser message
                    raise err(400, "INVALID_ARGS", f"from_pos is not a valid {cur[1]} position: {e}") from None
        if name is CommandName.SET_LIMITS:
            args = Limits(**{**(cur[0] or {}), **args}).model_dump()
            c.execute(update(agent).where(agent.c.id == agent_id).values(limits=args))
        cmd = store.issue_command(c, agent_id, name, args, p.subject, p.primary_role, reason=reason)
    return {"command_id": str(cmd.command_id), "status": "QUEUED"}


@router.get("/agents/{agent_id}/commands")
def agent_commands(agent_id: str, _: Principal = Depends(require_perm("agent.view"))) -> list[dict[str, Any]]:
    """Hub extension: command history of one agent."""
    with get_engine().connect() as c:
        return [{**dict(r), "id": str(r["id"])} for r in c.execute(
            select(agent_command).where(agent_command.c.agent_id == agent_id)
            .order_by(agent_command.c.issued_at.desc()).limit(200)).mappings().all()]


# ------------------------------------------------------------------ revisions
def _revision(r: Any, current_id: int | None) -> dict[str, Any]:
    return {"id": r["id"], "author": r["author"], "created_at": _iso(r["created_at"]), "reason": r["reason"] or "",
            "source": (r["body"] or {}).get("source", "console"), "current": r["id"] == current_id}


@router.get("/revisions")
def revisions(_: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        rows = c.execute(select(config_revision).order_by(config_revision.c.id.desc()).limit(500)).mappings().all()
    cur = rows[0]["id"] if rows else None
    return [_revision(r, cur) for r in rows]


def _flatten(d: Any, prefix: str = "tables") -> dict[str, Any]:
    if not isinstance(d, dict) or not d:
        return {prefix: d} if not isinstance(d, dict) else {}
    out: dict[str, Any] = {}
    for k, v in d.items():
        out.update(_flatten(v, f"{prefix}.{k}"))
    return out


def diff(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    """DiffEntry list (path like tables.S.T.columns.C.hashed)."""
    a, b = _flatten(before), _flatten(after)
    out = []
    for path in sorted(set(a) | set(b)):
        if path not in a:
            out.append({"path": path, "op": "add", "before": None, "after": b[path]})
        elif path not in b:
            out.append({"path": path, "op": "remove", "before": a[path], "after": None})
        elif a[path] != b[path]:
            out.append({"path": path, "op": "change", "before": a[path], "after": b[path]})
    return out


def _rev_body(c: Connection, rid: int) -> dict[str, Any]:
    body = c.execute(select(config_revision.c.body).where(config_revision.c.id == rid)).scalar()
    if body is None:
        raise err(404, "NOT_FOUND", "unknown revision")
    return dict(body)


@router.get("/revisions/{rid}/diff")
def revision_diff(rid: int, against: str = "current",
                  _: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    """Changes from the current config (against=current) or from the previous revision to revision `rid`."""
    with get_engine().connect() as c:
        target = _rev_body(c, rid).get("state", {})
        if against == "current":
            base = _config_state(c)
        else:
            prev = c.execute(select(config_revision.c.body).where(config_revision.c.id < rid)
                             .order_by(config_revision.c.id.desc()).limit(1)).scalar()
            base = (prev or {}).get("state", {})
    return diff(base, target)


def _apply_state(c: Connection, state: dict[str, Any], rev: int) -> None:
    """Make `state` the console overrides in force: listed rows get its values, others revert to defaults."""
    c.execute(update(table_config).values(revision=None, enabled=True, sync_mode="SOFT_DELETE"))
    c.execute(update(column_config).values(revision=None, enabled=True, hashed=False))
    for fqn, t in state.items():
        s, _, n = fqn.partition(".")
        tid = c.execute(select(source_table.c.id).where(source_table.c.src_schema == s,
                                                        source_table.c.src_table == n)).scalar()
        if tid is None:
            continue
        tv: dict[str, Any] = {"revision": rev}
        for k in ("enabled", "sync_mode"):
            if t.get(k) is not None:
                tv[k] = t[k]
        c.execute(update(table_config).where(table_config.c.table_id == tid).values(**tv))
        for col, cv in (t.get("columns") or {}).items():
            cid = c.execute(select(source_column.c.id).where(source_column.c.table_id == tid,
                                                             source_column.c.name == col)).scalar()
            if cid is not None:
                c.execute(update(column_config).where(column_config.c.column_id == cid).values(
                    enabled=bool(cv.get("enabled", True)), hashed=bool(cv.get("hashed", False)), revision=rev))


@router.post("/revisions/{rid}/rollback")
def revision_rollback(rid: int, body: Destructive,
                      p: Principal = Depends(require_perm("revision.rollback"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    need_confirm("revision_rollback", str(rid), body.confirm_text)
    with get_engine().begin() as c:
        state = _rev_body(c, rid).get("state", {})
        new = _new_revision(c, p.subject, reason, "rollback", {"rollback_to": rid})
        _apply_state(c, state, new)
        for cid in config_store.connections_with_docs(c):     # F-18: documents as they were after `rid`
            old_doc = config_store.doc_at(c, cid, rid)
            if old_doc is not None and old_doc != config_store.active_doc(c, cid):
                config_store.put_doc(c, new, cid, old_doc, validate=False)
                _pol_cache.pop(cid, None)
        _seal_revision(c, new)
        store.audit(c, p.subject, "revision.rollback", str(rid), {"new_revision": new, "reason": reason},
                    p.primary_role)
        row = c.execute(select(config_revision).where(config_revision.c.id == new)).mappings().one()
    return _revision(row, new)


def _export_doc(c: Connection, state: dict[str, Any]) -> dict[str, Any]:
    """config/tables.yaml document (config/DESIGN.md) for the first connection with `state` overrides applied."""
    cid = c.execute(select(connection.c.id).order_by(connection.c.created_at).limit(1)).scalar() or ""
    eng = c.execute(select(connection.c.engine).where(connection.c.id == cid)).scalar()
    pol = _policy(cid, c) if cid else load_policy("", get_settings().tables_config)
    schemas: dict[str, Any] = {}
    for s, n, tid in c.execute(select(source_table.c.src_schema, source_table.c.src_table, source_table.c.id)
                               .where(source_table.c.connection_id == cid)):
        ref = TableRef(schema=s, name=n)
        tp = pol.table(ref)
        ov = state.get(ref.fqn, {})
        cols: dict[str, Any] = {}
        for name, key in c.execute(select(source_column.c.name, source_column.c.key_seq)
                                   .where(source_column.c.table_id == tid)):
            cp = tp.columns.get(name)
            cv = (ov.get("columns") or {}).get(name, {})
            en = cv.get("enabled", cp.enabled if cp else True)
            hs = cv.get("hashed", cp.hashed if cp else False)
            if not en or hs or key:
                cols[name] = {"enabled": en, "hashed": hs, "is_primary_key": bool(key)}
        schemas.setdefault(s, {"enabled": True, "name_in_destination": s, "tables": {}})["tables"][n] = {
            "enabled": ov.get("enabled", tp.enabled), "name_in_destination": pol.dest_name(ref),
            "sync_mode": ov.get("sync_mode") or tp.sync_mode, "columns": cols}
    return {"connection_id": cid, "source_engine": eng, "output_profile": pol.output_profile,
            "landing_schema": pol.dest_schema, "schemas": schemas}


@router.get("/revisions/{rid}/export", response_class=PlainTextResponse)
def revision_export(rid: int, connection_id: str | None = None,
                    _: Principal = Depends(require_perm("table.configure"))) -> PlainTextResponse:
    import yaml

    with get_engine().connect() as c:
        body = _rev_body(c, rid)
        cids = config_store.revision_connections(c, rid) or config_store.connections_with_docs(c)
        cid = connection_id or (cids[0] if cids else None)
        doc = config_store.doc_at(c, cid, rid) if cid else None
        if doc is not None:                  # F-18: the stored document of that revision (round-trips on import)
            return PlainTextResponse(config_store.export_yaml(doc, rid), media_type="text/yaml")
        doc = _export_doc(c, body.get("state", {}))
    return PlainTextResponse(yaml.safe_dump(doc, sort_keys=True, allow_unicode=True), media_type="text/yaml")


# ------------------------------------------------------------------ schema import (normalized tables.yaml)
def _import_run(r: Any) -> dict[str, Any]:
    return {"id": str(r["id"]), "started_at": _iso(r["started_at"]), "finished_at": _iso(r["finished_at"]),
            "status": r["status"], "source_ref": r["source_ref"], "diff": r["diff"] or [], "message": r["message"]}


def _desired_state(doc: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for s, sd in (doc.get("schemas") or {}).items():
        for t, td in ((sd or {}).get("tables") or {}).items():
            td = td or {}
            entry: dict[str, Any] = {"enabled": bool(td.get("enabled", True))}
            cols = {c: {"enabled": bool((v or {}).get("enabled", True)), "hashed": bool((v or {}).get("hashed", False))}
                    for c, v in (td.get("columns") or {}).items()}
            if cols:
                entry["columns"] = cols
            out[f"{s}.{t}"] = entry
    return out


@router.post("/import/runs")
def start_import(body: Mutation, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    """Load a normalized tables.yaml (ALELT_IMPORT_FILE, written by `alelt import ...`) and diff it against the
    current config; accept applies it as a new revision."""
    reason = need_reason(body.reason)
    src = os.environ.get("ALELT_IMPORT_FILE", "")
    status_, message, desired, d = "FAILED", None, None, []
    if not src or not Path(src).exists():
        message = "no import source: set ALELT_IMPORT_FILE to a tables.yaml produced by `alelt import`"
    else:
        try:
            import yaml

            desired = yaml.safe_load(Path(src).read_text(encoding="utf-8")) or {}
            with get_engine().connect() as c:
                d = diff(_config_state(c), _desired_state(desired))
            status_ = "DONE"
        except Exception as e:  # noqa: BLE001
            message = f"{type(e).__name__}: {e}"
    with get_engine().begin() as c:
        row = c.execute(insert(import_run).values(
            id=uuid.uuid4(), status=status_, source_ref=src, diff=d, body=desired, message=message,
            started_by=p.subject, finished_at=utcnow()).returning(*import_run.c)).mappings().one()
        store.audit(c, p.subject, "import.run", str(row["id"]), {"status": status_, "reason": reason}, p.primary_role)
    return _import_run(row)


@router.get("/import/runs")
def list_imports(_: Principal = Depends(require_perm("import.run"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return [_import_run(r) for r in c.execute(select(import_run).order_by(import_run.c.started_at.desc())
                                                  .limit(100)).mappings().all()]


@router.post("/import/runs/{run_id}/accept")
def accept_import(run_id: str, body: Mutation, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        r = c.execute(select(import_run).where(import_run.c.id == _uuid(run_id)).with_for_update()).mappings().first()
        if r is None:
            raise err(404, "NOT_FOUND", "unknown import run")
        if r["status"] != "DONE":
            raise err(409, "NOT_DONE", f"import run is {r['status']}")
        rev = _new_revision(c, p.subject, reason, "alelt_import", {"import_run": run_id})
        doc = dict(r["body"] or {})
        cid = str(doc.get("connection_id") or "")
        if not cid:
            raise err(409, "INVALID_CONFIG", "imported document has no connection_id")
        try:                                   # F-18: the imported document becomes the active revision
            config_store.put_doc(c, rev, cid, doc)
        except config_store.ConfigError as e:
            raise err(409, "INVALID_CONFIG", str(e)) from None
        _pol_cache.pop(cid, None)
        _apply_state(c, _desired_state(doc), rev)
        _seal_revision(c, rev)
        c.execute(update(import_run).where(import_run.c.id == r["id"]).values(status="ACCEPTED"))
        store.audit(c, p.subject, "import.accept", run_id, {"revision": rev, "reason": reason}, p.primary_role)
        row = c.execute(select(config_revision).where(config_revision.c.id == rev)).mappings().one()
    return _revision(row, rev)


# ------------------------------------------------------------------ schema changes
def _schema_change(h: Any, ref: dict[str, str] | None, engine: str | None) -> dict[str, Any]:
    return {"id": str(h["id"]), "table": ref, "kind": h["kind"],
            "detail": h["detail"] if isinstance(h["detail"], str) else json.dumps(h["detail"] or {}),
            "detected_at": _iso(h["created_at"]), "position": _pos(engine, h["position"]),
            "policy": h["policy"], "status": h["status"]}


def _schema_changes(c: Connection, where: Any = None) -> list[dict[str, Any]]:
    stmt = (select(schema_change_event, source_table.c.src_schema, source_table.c.src_table, connection.c.engine)
            .join(source_table, source_table.c.id == schema_change_event.c.table_id, isouter=True)
            .join(connection, connection.c.id == source_table.c.connection_id, isouter=True)
            .order_by(schema_change_event.c.created_at.desc()).limit(500))
    if where is not None:
        stmt = stmt.where(where)
    return [_schema_change(h, {"schema": h["src_schema"], "name": h["src_table"]} if h["src_schema"] else None,
                           h["engine"]) for h in c.execute(stmt).mappings().all()]


@router.get("/schema-changes")
def schema_changes(_: Principal = Depends(require_perm("schema.decide"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return _schema_changes(c)


class SchemaDecision(Mutation):
    decision: str = Field(pattern="^(APPROVE|REJECT)$")


@router.post("/schema-changes/{sid}/decision")
def schema_decision(sid: str, body: SchemaDecision,
                    p: Principal = Depends(require_perm("schema.decide"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    key = _uuid(sid)
    with get_engine().begin() as c:
        cur = c.execute(select(schema_change_event.c.status).where(schema_change_event.c.id == key)
                        .with_for_update()).scalar()
        if cur is None:
            raise err(404, "NOT_FOUND", "unknown schema change")
        if cur != "PENDING":
            raise err(409, "NOT_PENDING", f"schema change is {cur}")
        new = "APPROVED" if body.decision == "APPROVE" else "REJECTED"
        c.execute(update(schema_change_event).where(schema_change_event.c.id == key).values(status=new))
        det = c.execute(select(schema_change_event.c.detail).where(schema_change_event.c.id == key)).scalar()
        cid_ = det.get("connection_id") if isinstance(det, dict) else None
        if cid_:                                # HC-10: the decode job applies the decision and releases the hold
            from . import jobs

            jobs.enqueue(c, "decode", {"connection_id": cid_}, dedup_key=f"schema-decision:{sid}")
        store.audit(c, p.subject, "schema.decision", sid, {"decision": body.decision, "reason": reason},
                    p.primary_role)
        return _schema_changes(c, schema_change_event.c.id == key)[0]


# ------------------------------------------------------------------ reconciliation
def _recon_runs(c: Connection, where: Any = None) -> list[dict[str, Any]]:
    stmt = select(recon_run).order_by(recon_run.c.started_at.desc()).limit(50)
    if where is not None:
        stmt = stmt.where(where)
    runs = c.execute(stmt).mappings().all()
    res = c.execute(select(recon_result, source_table.c.src_schema, source_table.c.src_table)
                    .join(source_table, source_table.c.id == recon_result.c.table_id, isouter=True)
                    .where(recon_result.c.run_id.in_([r["id"] for r in runs]))).mappings().all()
    kinds = {r["id"]: r["check_kind"] for r in runs}
    by_run: dict[Any, list[dict[str, Any]]] = {}
    for x in res:
        by_run.setdefault(x["run_id"], []).append(_recon(
            {**dict(x), "check_kind": kinds.get(x["run_id"])}, {"schema": x["src_schema"], "name": x["src_table"]}))
    return [{"id": str(r["id"]), "check_kind": r["check_kind"], "started_at": _iso(r["started_at"]),
             "finished_at": _iso(r["finished_at"]), "results": by_run.get(r["id"], [])} for r in runs]


@router.get("/recon")
def recon(_: Principal = Depends(require_perm("view.recon"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return _recon_runs(c)


class ReconRunRequest(Mutation):
    table_ids: list[str] = Field(default_factory=list)
    check_kind: str


@router.post("/recon/runs")
def start_recon(body: ReconRunRequest, p: Principal = Depends(require_perm("recon.run"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    if body.check_kind not in RECON_CHECKS:
        raise err(400, "INVALID_CHECK", f"check_kind must be one of {RECON_CHECKS}")
    ids = [str(_uuid(t)) for t in body.table_ids]
    with get_engine().begin() as c:
        same = _pending_like(c, "verify", {"table_ids": ids, "check_kind": body.check_kind})
        if same is not None and same[1].get("run_id"):            # HC-28: the same run is still queued / running
            got = _recon_runs(c, recon_run.c.id == uuid.UUID(str(same[1]["run_id"])))
            if got:
                return got[0]
        rid = c.execute(insert(recon_run).values(id=uuid.uuid4(), check_kind=body.check_kind)
                        .returning(recon_run.c.id)).scalar_one()
        jid = _enqueue(c, "verify", {"run_id": str(rid), "table_ids": ids, "check_kind": body.check_kind})  # alelt_verify
        store.audit(c, p.subject, "recon.run", str(rid), {"job_id": jid, "table_ids": ids, "reason": reason},
                    p.primary_role)
        return _recon_runs(c, recon_run.c.id == rid)[0]


# ------------------------------------------------------------------ certification
def _certification(c: Connection, tid: uuid.UUID | None = None) -> list[dict[str, Any]]:
    s = get_settings()
    stmt = _table_stmt()
    if tid is not None:
        stmt = stmt.where(source_table.c.id == tid)
    lag = _lags(c)
    out = []
    for t in c.execute(stmt).mappings().all():
        ref = TableRef(schema=t["src_schema"], name=t["src_table"])
        enabled = bool(t["enabled"]) if t["revision"] is not None else _policy(t["connection_id"], c).table(ref).enabled
        if not enabled:
            continue
        res = c.execute(select(recon_run.c.check_kind, recon_result.c.outcome, recon_result.c.created_at,
                               recon_result.c.detail)
                        .join(recon_run, recon_run.c.id == recon_result.c.run_id)
                        .where(recon_result.c.table_id == t["id"]).order_by(recon_result.c.created_at)).all()
        latest: dict[str, tuple[str, str]] = {}
        for kind, outcome, at, detail in res:
            latest[kind] = (outcome, detail or "")
        target = getattr(_policy(t["connection_id"], c).table(ref), "latency_target_s", None) or s.latency_target_s
        # XC-11 / F-19: one evaluator (alelt_verify.certification.evaluate) for the clean-day streak
        clean_st, clean_val, clean = cert_evidence.clean_days_criterion(
            c, t["connection_id"], ref.fqn, float(target), ((k, o, at) for k, o, at, _d in res))
        keyless = cert_evidence.is_keyless(c, t["id"])
        crit = []
        for name in CERT_CRITERIA:
            kind = next((k for k, v in CHECK_TO_CRITERION.items() if v == name), None)
            if kind:
                o = latest.get(kind)
                st, val = ("PENDING", "") if o is None else ("PASS" if o[0] == "MATCH" else "FAIL", o[1])
                if keyless and name == "PK_SET_PARITY":       # F-10: multiset parity, never a key set
                    val = f"keyless: row multiset parity{'; ' + val if val else ''}"
            elif name == "LATENCY_P95":            # F-19: recorded commit -> apply latency, never the lag proxy
                target = getattr(_policy(t["connection_id"], c).table(ref), "latency_target_s", None) or s.latency_target_s
                st, val = cert_evidence.latency_criterion(c, t["connection_id"], ref.fqn, float(target))
            else:                              # CLEAN_DAYS_7
                st, val = clean_st, clean_val
            crit.append({"criterion": name, "status": st, "value": val, "evidence_url": None})
        sig = c.execute(select(signoff).where(signoff.c.table_id == t["id"]).order_by(signoff.c.signed_at)).mappings().all()
        certified = any(x["criterion"] == "ALL" for x in sig)
        out.append({"table_id": str(t["id"]), "table": {"schema": t["src_schema"], "name": t["src_table"]},
                    "status": "CERTIFIED" if certified else "IN_PROGRESS" if res else "NOT_STARTED",
                    "clean_days": clean, "criteria": crit,
                    "signoffs": [{"id": str(x["id"]), "user": x["user_name"] or "", "signed_at": _iso(x["signed_at"]),
                                  "comment": x["comment"] or ""} for x in sig],
                    "active_source": t["active_source"] or "LEGACY"})
    return out


@router.get("/certification")
def certification(_: Principal = Depends(require_perm("view.certification"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return _certification(c)


@router.post("/certification/{table_id}/signoff")
def cert_signoff(table_id: str, body: Mutation, p: Principal = Depends(require_perm("cert.signoff"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    tid = _uuid(table_id)
    with get_engine().begin() as c:
        cert = _certification(c, tid)
        if not cert:
            raise err(404, "NOT_FOUND", "unknown or disabled table")
        failing = [x["criterion"] for x in cert[0]["criteria"] if x["status"] != "PASS"]
        if failing:
            raise err(409, "CRITERIA_NOT_MET", f"criteria not PASS: {failing}")
        _integrity_gate(c, [tid])
        c.execute(insert(signoff).values(id=uuid.uuid4(), table_id=tid, criterion="ALL", user_name=p.subject,
                                         comment=reason))
        store.audit(c, p.subject, "cert.signoff", str(tid), {"reason": reason}, p.primary_role)
        return _certification(c, tid)[0]


def _integrity_gate(c: Connection, tids: list[uuid.UUID]) -> None:
    """F-01: certification and switch are refused while a table (or its connection) is not integrity IN_SYNC."""
    from . import integrity

    bad = []
    for cid, sch, tab in c.execute(select(source_table.c.connection_id, source_table.c.src_schema,
                                          source_table.c.src_table).where(source_table.c.id.in_(tids))):
        why = integrity.blocking(c, cid, f"{sch}.{tab}")
        if why is None:                        # ADR-013: pending out-of-row completions are a soft gate
            from .completion import blocking as completion_blocking

            why = completion_blocking(get_engine(), cid, f"{sch}.{tab}")
        if why:
            bad.append(f"{sch}.{tab}: {why}")
    if bad:
        raise err(409, "TABLE_NOT_IN_SYNC", "; ".join(bad)[:1000])


def _wave(r: Any) -> dict[str, Any]:
    return {"id": str(r["id"]), "name": r["name"], "table_ids": r["table_ids"], "planned_at": _iso(r["planned_at"]),
            "status": r["status"]}


@router.get("/certification/waves")
def waves(_: Principal = Depends(require_perm("view.certification"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return [_wave(r) for r in c.execute(select(wave).order_by(wave.c.created_at)).mappings().all()]


class WaveCreate(Mutation):
    name: str = Field(min_length=1)
    table_ids: list[str]
    planned_at: datetime | None = None


@router.post("/certification/waves")
def create_wave(body: WaveCreate, p: Principal = Depends(require_perm("wave.plan"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    ids = [str(_uuid(t)) for t in body.table_ids]
    with get_engine().begin() as c:
        found = c.execute(select(func.count()).select_from(source_table)
                          .where(source_table.c.id.in_([uuid.UUID(i) for i in ids]))).scalar_one()
        if found != len(set(ids)):
            raise err(404, "NOT_FOUND", "unknown table id in wave")
        if c.execute(select(wave.c.id).where(wave.c.name == body.name)).scalar() is not None:
            raise err(409, "DUPLICATE", f"wave {body.name} exists")
        row = c.execute(insert(wave).values(id=uuid.uuid4(), name=body.name, table_ids=ids, planned_at=body.planned_at,
                                            created_by=p.subject).returning(*wave.c)).mappings().one()
        store.audit(c, p.subject, "wave.plan", body.name, {"table_ids": ids, "reason": reason}, p.primary_role)
    return _wave(row)


# ------------------------------------------------------------------ switch (two-person rule)
def _switch(r: Any) -> dict[str, Any]:
    return {"id": str(r["id"]), "scope": r["scope"], "scope_label": r["scope_label"], "target": r["target"],
            "requested_by": r["requested_by"], "requested_at": _iso(r["requested_at"]), "reason": r["reason"],
            "status": r["status"], "approved_by": r["approved_by"], "executed_at": _iso(r["executed_at"]),
            "expires_at": _iso(r["expires_at"])}


def _scope_tables(c: Connection, scope: dict[str, Any]) -> tuple[str, list[uuid.UUID], uuid.UUID | None]:
    if scope.get("table_id"):
        tid = _uuid(scope["table_id"])
        fqn, _ = _table_fqn(c, tid)
        return fqn, [tid], None
    if scope.get("wave_id"):
        wid = _uuid(scope["wave_id"])
        w = c.execute(select(wave.c.name, wave.c.table_ids).where(wave.c.id == wid)).first()
        if w is None:
            raise err(404, "NOT_FOUND", "unknown wave")
        return str(w[0]), [uuid.UUID(t) for t in w[1]], wid
    raise err(400, "INVALID_SCOPE", "scope needs table_id or wave_id")


def _ladder_gate() -> None:
    """Production switches need the previous ladder level (L6, lab) green - 409 LADDER_GATE."""
    if get_settings().env != "prod":
        return
    f = _LADDER / "L6" / "result.json"
    ok = f.exists() and json.loads(f.read_text(encoding="utf-8")).get("status") == "PASS"
    if not ok:
        raise err(409, "LADDER_GATE", "previous ladder level (L6) is not PASS")


def _switch_action(target: str) -> str:
    return "switch" if target == "ALELT" else "rollback"


class SwitchRequestCreate(Destructive):
    scope: dict[str, str]
    target: str


@router.post("/switch")
def request_switch(body: SwitchRequestCreate, p: Principal = Depends(require_perm("switch.request"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    if body.target not in ACTIVE_SOURCES:
        raise err(400, "INVALID_TARGET", f"target must be one of {ACTIVE_SOURCES}")
    _ladder_gate()
    with get_engine().begin() as c:
        label, tids, _ = _scope_tables(c, body.scope)
        need_confirm(_switch_action(body.target), label, body.confirm_text)
        if body.target == "ALELT":
            cert = _certified_ids(c)
            missing = [t for t in tids if t not in cert]
            if missing:
                raise err(409, "NOT_CERTIFIED", f"{len(missing)} table(s) in scope are not certified")
            _integrity_gate(c, tids)
        row = c.execute(insert(switch_request).values(
            id=uuid.uuid4(), scope=body.scope, scope_label=label, target=body.target, requested_by=p.subject,
            reason=reason, expires_at=utcnow() + SWITCH_TTL).returning(*switch_request.c)).mappings().one()
        store.audit(c, p.subject, "switch.request", label, {"request_id": str(row["id"]), "target": body.target,
                                                            "reason": reason}, p.primary_role)
    return _switch(row)


@router.get("/switch/requests")
def switch_requests(_: Principal = Depends(require_perm("view.certification"))) -> list[dict[str, Any]]:
    with get_engine().begin() as c:
        c.execute(update(switch_request).where(switch_request.c.status == "PENDING_APPROVAL",
                                               switch_request.c.expires_at <= utcnow()).values(status="EXPIRED"))
        return [_switch(r) for r in c.execute(select(switch_request).order_by(switch_request.c.requested_at.desc())
                                              .limit(200)).mappings().all()]


def _locked_request(c: Connection, rid: str) -> Any:
    r = c.execute(select(switch_request).where(switch_request.c.id == _uuid(rid)).with_for_update()).mappings().first()
    if r is None:
        raise err(404, "NOT_FOUND", "unknown switch request")
    if r["status"] != "PENDING_APPROVAL":
        raise err(409, "NOT_PENDING", f"request is {r['status']}")
    return r


@router.post("/switch/requests/{rid}/approve")
def approve_switch(rid: str, body: Destructive, p: Principal = Depends(require_perm("switch.approve"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        r = _locked_request(c, rid)
        if r["expires_at"] <= utcnow():
            expired = True
        else:
            expired = False
            if r["requested_by"].strip().lower() == p.subject.strip().lower():
                raise err(409, "TWO_PERSON_RULE", "approver must differ from requester")
            need_confirm(_switch_action(r["target"]), r["scope_label"], body.confirm_text)
            _, tids, wid = _scope_tables(c, r["scope"])
            if r["target"] == "ALELT":
                cert = _certified_ids(c)
                if any(t not in cert for t in tids):
                    raise err(409, "NOT_CERTIFIED", "a table in scope is not certified")
                _integrity_gate(c, tids)
            now = utcnow()
            for t in tids:
                # TODO(destination stream): invoke the destination switch procedure (SWITCH_SOURCE) for `t` here.
                c.execute(insert(switch_state).values(table_id=t, active_source=r["target"], switched_at=now)
                          .on_conflict_do_update(index_elements=["table_id"],
                                                 set_={"active_source": r["target"], "switched_at": now}))
            if wid is not None:
                c.execute(update(wave).where(wave.c.id == wid).values(
                    status="SWITCHED" if r["target"] == "ALELT" else "ROLLED_BACK"))
            c.execute(update(switch_request).where(switch_request.c.id == r["id"]).values(
                status="EXECUTED", approved_by=p.subject, executed_at=now))
            store.audit(c, p.subject, "switch.approve", r["scope_label"],
                        {"request_id": rid, "target": r["target"], "requested_by": r["requested_by"],
                         "reason": reason}, p.primary_role)
            row = c.execute(select(switch_request).where(switch_request.c.id == r["id"])).mappings().one()
    if expired:                                # persist EXPIRED outside the rolled-back error path
        with get_engine().begin() as c:
            c.execute(update(switch_request).where(switch_request.c.id == _uuid(rid)).values(status="EXPIRED"))
        raise err(409, "EXPIRED", "switch request expired")
    return _switch(row)


@router.post("/switch/requests/{rid}/reject")
def reject_switch(rid: str, body: Mutation, p: Principal = Depends(require_perm("switch.approve"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        r = _locked_request(c, rid)
        c.execute(update(switch_request).where(switch_request.c.id == r["id"]).values(status="REJECTED",
                                                                                     approved_by=p.subject))
        store.audit(c, p.subject, "switch.reject", r["scope_label"], {"request_id": rid, "reason": reason},
                    p.primary_role)
        row = c.execute(select(switch_request).where(switch_request.c.id == r["id"])).mappings().one()
    return _switch(row)


# ------------------------------------------------------------------ hub health & retention
def _spool_used() -> int:
    from .blobstore import used_bytes   # configured spool store, local or S3 (X-46)

    return used_bytes()


@router.get("/hub/health")
def hub_health(_: Principal = Depends(require_perm("hub.view"))) -> dict[str, Any]:
    s = get_settings()
    with get_engine().connect() as c:
        q = [(k, n, seconds_since(oldest)) for k, n, oldest in c.execute(
            select(job.c.kind, func.count(), func.min(job.c.created_at))
            .where(job.c.status == "QUEUED").group_by(job.c.kind)).all()]
        failed = c.execute(select(func.count()).select_from(job).where(job.c.status == "FAILED")).scalar_one()
        cap = _setting(c, "retention", RETENTION_DEFAULT)["spool_cap_bytes"]
    return {"services": [{"name": "hub-api", "status": "OK", "detail": ""},
                         {"name": "metadata-db", "status": "OK", "detail": "reachable"},
                         {"name": "worker", "status": "DEGRADED" if failed else "OK",
                          "detail": f"{failed} failed job(s)" if failed else ""}],
            "queues": [{"name": k, "depth": n, "oldest_age_seconds": float(a or 0)} for k, n, a in q],
            "spool_used_bytes": _spool_used(), "spool_capacity_bytes": int(cap),
            "destination": {"engine": s.destination.split(".")[0], "account": "", "warehouse": "",
                            "credits_used_today": None, "credit_quota_month": None, "credits_used_month": None},
            "credentials": []}


def wal_archiving_ok(c: Connection) -> bool | None:
    """XC-28 / F-19: observed, never assumed. PostgreSQL metadata DB: archive_mode on and the archiver's last result
    is not a failure newer than its last success (pg_stat_archiver); None when unknown (SQLite, no privilege)."""
    if c.dialect.name != "postgresql":
        return None
    try:
        from sqlalchemy import text as _t

        with c.begin_nested():
            mode = c.execute(_t("SHOW archive_mode")).scalar()
            if str(mode).lower() not in ("on", "always"):
                return False
            ok_at, fail_at = c.execute(_t("SELECT last_archived_time, last_failed_time FROM pg_stat_archiver")).one()
    except Exception:  # noqa: BLE001 - no privilege / not observable
        return None
    if ok_at is None:
        return False
    return fail_at is None or fail_at <= ok_at


def _retention(c: Connection) -> dict[str, Any]:
    r = _setting(c, "retention", RETENTION_DEFAULT)
    return {"spool_cap_bytes": int(r["spool_cap_bytes"]), "raw_archive_days": int(r["raw_archive_days"]),
            "postgres_backups": None, "wal_archiving_ok": wal_archiving_ok(c)}


@router.get("/retention")
def get_retention(_: Principal = Depends(require_perm("retention.edit"))) -> dict[str, Any]:
    with get_engine().connect() as c:
        return _retention(c)


class RetentionUpdate(Mutation):
    spool_cap_bytes: int | None = Field(default=None, gt=0)
    raw_archive_days: int | None = None


@router.put("/retention")
def put_retention(body: RetentionUpdate, p: Principal = Depends(require_perm("retention.edit"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    if body.raw_archive_days is not None and not 30 <= body.raw_archive_days <= 90:
        raise err(status.HTTP_422_UNPROCESSABLE_ENTITY, "OUT_OF_RANGE", "raw_archive_days must be 30..90 (ADR-007)")
    with get_engine().begin() as c:
        cur = _setting(c, "retention", RETENTION_DEFAULT)
        new = {**cur, **body.model_dump(exclude={"reason"}, exclude_none=True)}
        c.execute(insert(hub_setting).values(key="retention", value=new).on_conflict_do_update(
            index_elements=["key"], set_={"value": new, "updated_at": utcnow()}))
        store.audit(c, p.subject, "retention.update", "retention", {"before": cur, "after": new, "reason": reason},
                    p.primary_role)
        return _retention(c)


# ------------------------------------------------------------------ users & roles
def _users(c: Connection) -> dict[str, Any]:
    maps = [dict(r) for r in c.execute(select(group_mapping).order_by(group_mapping.c.group_name)).mappings().all()]
    users = []
    for u in c.execute(select(app_user).order_by(app_user.c.email)).mappings().all():
        roles = [r[0] for r in c.execute(select(role_assignment.c.role).where(role_assignment.c.user_id == u["id"]))]
        users.append({"id": str(u["id"]), "email": u["email"] or "", "name": u["email"] or "", "roles": roles,
                      "last_seen": _iso(u["created_at"])})
    return {"group_mappings": maps, "users": users}


@router.get("/users")
def users(_: Principal = Depends(require_perm("users.manage"))) -> dict[str, Any]:
    with get_engine().connect() as c:
        return _users(c)


class GroupMappingIn(BaseModel):
    group_id: str = Field(min_length=1)
    group_name: str
    role: str = Field(pattern="^(Viewer|DataOperator|PlatformAdmin|Auditor)$")


class GroupMappingsUpdate(Mutation):
    group_mappings: list[GroupMappingIn]


@router.put("/users/group-mappings")
def put_group_mappings(body: GroupMappingsUpdate, p: Principal = Depends(require_perm("users.manage"))) -> dict[str, Any]:
    from . import auth as _auth

    _auth._groups_cache[0] = 0.0                 # mapping applies to the next login (F-15)
    reason = need_reason(body.reason)
    if not any(m.role == "PlatformAdmin" for m in body.group_mappings):
        raise err(409, "LOCKOUT", "at least one PlatformAdmin group mapping must remain")
    with get_engine().begin() as c:
        before = [dict(r) for r in c.execute(select(group_mapping)).mappings().all()]
        c.execute(delete(group_mapping))
        for m in body.group_mappings:
            c.execute(insert(group_mapping).values(**m.model_dump()))
        store.audit(c, p.subject, "users.group_mappings", "group_mappings",
                    {"before": before, "after": [m.model_dump() for m in body.group_mappings], "reason": reason},
                    p.primary_role)
        return _users(c)


# ------------------------------------------------------------------ audit
def _audit_stmt(q: str | None, from_: datetime | None, to: datetime | None, actor: str | None,
                action: str | None) -> Select[Any]:
    stmt = select(audit_event).order_by(audit_event.c.at.desc())
    if from_:
        stmt = stmt.where(audit_event.c.at >= from_)
    if to:
        stmt = stmt.where(audit_event.c.at <= to)
    if actor:
        stmt = stmt.where(audit_event.c.actor == actor)
    if action:
        stmt = stmt.where(audit_event.c.action.like(f"{action}%"))
    if q:
        like = f"%{q}%"
        stmt = stmt.where(or_(audit_event.c.action.ilike(like), audit_event.c.target.ilike(like),
                              audit_event.c.actor.ilike(like)))
    return stmt


def _audit_item(r: dict[str, Any]) -> dict[str, Any]:
    role = r["role"] or ("agent" if r["actor"].startswith("agent:") else "system")
    return {"id": str(r["id"]), "at": _iso(r["at"]), "actor": r["actor"], "role": role, "action": r["action"],
            "target": r["target"] or "", "detail": r["detail"] or {},
            "correlation_id": (r["detail"] or {}).get("correlation_id")}


@router.get("/audit")
def audit_log(q: str | None = None, from_: datetime | None = Query(None, alias="from"), to: datetime | None = None,
              actor: str | None = None, action: str | None = None, cursor: str | None = None,
              limit: int = Query(200, le=5000), _: Principal = Depends(require_perm("audit.read"))) -> dict[str, Any]:
    rows, total, nxt = _page(_audit_stmt(q, from_, to, actor, action), cursor, limit)
    return {"items": [_audit_item(r) for r in rows], "total": total, "next_cursor": nxt}


@router.get("/audit/export")
def audit_export(format_: str = Query("json", alias="format", pattern="^(csv|json)$"),
                 from_: datetime | None = Query(None, alias="from"), to: datetime | None = None,
                 _: Principal = Depends(require_perm("audit.export"))) -> Response:
    with get_engine().connect() as c:
        items = [_audit_item(dict(r)) for r in c.execute(_audit_stmt(None, from_, to, None, None).limit(100_000))
                 .mappings().all()]
    if format_ == "json":
        return Response(json.dumps(items, default=str), media_type="application/json",
                        headers={"Content-Disposition": "attachment; filename=audit.json"})
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "at", "actor", "role", "action", "target", "correlation_id", "detail"])
    for i in items:
        w.writerow([i["id"], i["at"], i["actor"], i["role"], i["action"], i["target"], i["correlation_id"] or "",
                    json.dumps(i["detail"], default=str)])
    return Response(buf.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=audit.csv"})


# ------------------------------------------------------------------ table integrity (F-01): held tables + actions
@router.get("/integrity")
def integrity_view(connection_id: str | None = None,
                   _: Principal = Depends(require_perm("view.tables"))) -> list[dict[str, Any]]:
    """Every table that is held or not integrity IN_SYNC, with the reason, recovery step, incident and holds."""
    with get_engine().connect() as c:
        st = _integrity(c)
    return [{"connection_id": cid, "table": fqn, **v} for (cid, fqn), v in sorted(st.items())
            if (connection_id is None or cid == connection_id) and (v["integrity"] != "IN_SYNC" or v["held_by"])]


class IntegrityAction(Mutation):
    connection_id: str
    table: str = "*"
    action: str = Field(pattern="^(retry|resync|acknowledge|resume_stream)$")


@router.post("/integrity/actions")
def integrity_action(body: IntegrityAction, p: Principal = Depends(require_perm("table.resync"))) -> dict[str, Any]:
    """retry: next recovery step now; resync: snapshot the table; acknowledge: accept a SUSPECT (not held) table or
    connection after own verification; resume_stream: continue a stop_stream connection (tables -> hold_table)."""
    from . import holds, integrity

    reason = need_reason(body.reason)
    eng = get_engine()
    r = integrity.row(eng, body.connection_id, body.table)
    if r is None:
        raise err(404, "NOT_FOUND", "no integrity record for this table")
    out: Any
    if body.action == "resume_stream":
        out = integrity.resume_stream(eng, body.connection_id, p.subject)
    elif body.action == "acknowledge":
        if not integrity.acknowledge(eng, body.connection_id, body.table, p.subject, reason):
            raise err(409, "HELD", "a held table is released by recovery (retry / resync), not acknowledged")
        out = True
    else:
        if body.action == "resync":
            holds.set_body(eng, body.connection_id, body.table, integrity.KIND, step="resync")
        elif (r["body"] or {}).get("step") == "manual":
            holds.set_body(eng, body.connection_id, body.table, integrity.KIND, step="catalog")
        integrity.enqueue(eng, body.connection_id, body.table)
        out = "queued"
    with eng.begin() as c:
        store.audit(c, p.subject, f"integrity.{body.action}", f"{body.connection_id}:{body.table}",
                    {"reason": reason}, p.primary_role)
    return {"result": out, "integrity": integrity.state_of(eng, body.connection_id, body.table)}

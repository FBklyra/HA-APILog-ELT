"""Evidence collector - read-only (R1): hub metadata, logs, metrics, optional destination checkpoint probe.

Output is always passed through redact.redact() before it is stored, returned or sent to a model.
"""
from __future__ import annotations

import concurrent.futures
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Connection, Engine, func, select

from alelt_common.models import Position
from alelt_common.util import utcnow
from alelt_hub.db import (
    agent,
    audit_event,
    batch,
    catalog_version,
    checkpoint_mirror,
    config_revision,
    job,
    quarantine_event,
    recon_result,
    schema_change_event,
    source_table,
)

from . import logbuf
from .models import EvidencePack, Trigger
from .redact import hex_head, redact

log = logging.getLogger("alelt_diagnose")


def _u(v: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(v)) if v else None
    except ValueError:
        return None


def _iso(v: Any) -> str | None:
    return v.isoformat() if isinstance(v, datetime) else (str(v) if v else None)


def _aware(v: datetime | None) -> datetime | None:
    return v.replace(tzinfo=timezone.utc) if v is not None and v.tzinfo is None else v


def _age_s(v: datetime | None) -> float | None:
    v = _aware(v)
    return round((utcnow() - v).total_seconds(), 1) if v else None


def _split(fqn: str | None) -> tuple[str, str] | None:
    if not fqn or "." not in fqn:
        return None
    s, _, n = fqn.partition(".")
    return s, n


def _job(c: Connection, t: Trigger) -> dict[str, Any] | None:
    if t.job_id is None:
        return None
    r = c.execute(select(job).where(job.c.id == t.job_id)).mappings().first()
    if r is None:
        return None
    return {"id": r["id"], "kind": r["kind"], "status": r["status"], "attempts": r["attempts"], "error": r["error"],
            "payload_keys": sorted((r["payload"] or {}).keys()), "created_at": _iso(r["created_at"]),
            "traceback": t.detail.get("traceback")}


def _batch(c: Connection, t: Trigger) -> dict[str, Any] | None:
    bid = _u(t.batch_id)
    if bid is None:
        return None
    r = c.execute(select(batch).where(batch.c.id == bid)).mappings().first()
    if r is None:
        return None
    return {"id": str(r["id"]), "seq": r["seq"], "agent_id": r["agent_id"], "from_pos": r["from_lri"],
            "to_pos": r["to_lri"], "next_pos": r["next_lri"], "rec_count": r["rec_count"], "raw_bytes": r["raw_bytes"],
            "codec": r["codec"], "status": r["status"], "error": r["error"], "events": r["events"],
            "catalog_version_id": r["catalog_version_id"], "received_at": _iso(r["received_at"])}


def _quarantine(c: Connection, t: Trigger, level: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    q = select(quarantine_event).order_by(quarantine_event.c.created_at.desc()).limit(10)
    bid = _u(t.batch_id)
    if bid is not None:
        q = q.where(quarantine_event.c.batch_id == bid)
    elif t.table_fqn:
        q = q.where(quarantine_event.c.table_fqn == t.table_fqn)
    elif t.connection_id:
        q = q.where(quarantine_event.c.connection_id == t.connection_id)
    elif t.kind.value != "QUARANTINE":
        return [], {}
    rows = c.execute(q).mappings().all()
    samples = [{"id": str(r["id"]), "reason": r["reason"], "position": r["lri"], "table": r["table_fqn"],
                "raw_ref": r["raw_ref"], "raw_hex_head": hex_head(r["raw_hex"], level), "status": r["status"],
                "at": _iso(r["created_at"])} for r in rows]
    counts: dict[str, int] = {}
    agg = select(quarantine_event.c.reason, func.count()).group_by(quarantine_event.c.reason)
    if t.connection_id:
        agg = agg.where(quarantine_event.c.connection_id == t.connection_id)
    for reason, n in c.execute(agg.limit(20)):
        counts[str(reason)[:160]] = int(n)
    return samples, counts


def _catalog(c: Connection, t: Trigger, used: int | None) -> dict[str, Any]:
    if not t.connection_id:
        return {}
    rows = c.execute(select(catalog_version.c.id, catalog_version.c.valid_from_pos, catalog_version.c.taken_at,
                            catalog_version.c.received_at, catalog_version.c.sha256, catalog_version.c.body)
                     .where(catalog_version.c.connection_id == t.connection_id)
                     .order_by(catalog_version.c.id.desc()).limit(6)).all()
    versions, layouts = [], {}
    ref = _split(t.table_fqn)
    for vid, vpos, taken, recv, sha, body in rows:
        skew = None
        if taken and recv:
            skew = round((_aware(recv) - _aware(taken)).total_seconds(), 1)  # type: ignore[operator]
        versions.append({"id": vid, "valid_from_pos": vpos, "taken_at": _iso(taken), "received_at": _iso(recv),
                         "sha256": sha[:12], "receive_skew_s": skew, "codepage": (body or {}).get("database_codepage")})
        if ref:
            for tb in (body or {}).get("tables", []):
                tr = tb.get("table") or {}
                if (tr.get("schema"), tr.get("name")) == ref:
                    layouts[str(vid)] = {
                        "engine_ids": tb.get("engine_ids"), "capture_enabled": tb.get("capture_enabled"),
                        "compression": tb.get("compression"),
                        "columns": [{k: col.get(k) for k in ("name", "ordinal", "engine_type", "canonical_type",
                                                             "length", "scale", "codepage", "key_seq")}
                                    for col in tb.get("columns", [])]}
    out: dict[str, Any] = {"versions": versions, "used_version_id": used, "table_layouts": layouts}
    if ref:
        cap = c.execute(select(source_table.c.capture_enabled, source_table.c.compression).where(
            source_table.c.connection_id == t.connection_id, source_table.c.src_schema == ref[0],
            source_table.c.src_table == ref[1])).first()
        if cap is not None:
            out["capture_enabled"], out["compression"] = bool(cap[0]), cap[1]
    return out


def _schema_changes(c: Connection, t: Trigger) -> list[dict[str, Any]]:
    q = (select(schema_change_event, source_table.c.src_schema, source_table.c.src_table)
         .join(source_table, source_table.c.id == schema_change_event.c.table_id, isouter=True)
         .order_by(schema_change_event.c.created_at.desc()).limit(10))
    if t.connection_id:
        q = q.where(source_table.c.connection_id == t.connection_id)
    return [{"kind": r["kind"], "status": r["status"], "table": f"{r['src_schema']}.{r['src_table']}",
             "position": r["position"], "detail": r["detail"], "at": _iso(r["created_at"])}
            for r in c.execute(q).mappings().all()]


def _agent(c: Connection, t: Trigger) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    q = select(agent)
    if t.agent_id:
        q = q.where(agent.c.id == t.agent_id)
    elif t.connection_id:
        q = q.where(agent.c.connection_id == t.connection_id)
    else:
        return None, []
    r = c.execute(q.limit(1)).mappings().first()
    if r is None:
        return None, []
    a = {"id": r["id"], "status": r["status"], "version": r["version"], "last_read_pos": r["last_lri"],
         "durable_pos": r["durable_pos"], "spool_bytes": r["spool_bytes"], "metrics": r["metrics"],
         "limits": r["limits"], "heartbeat_age_s": _age_s(r["last_heartbeat"]),
         "cert_pinned": bool(r["cert_thumbprint"])}
    evs = c.execute(select(audit_event.c.action, audit_event.c.at, audit_event.c.detail)
                    .where(audit_event.c.target == r["id"], audit_event.c.action.like("agent.%"))
                    .order_by(audit_event.c.at.desc()).limit(15)).all()
    events = []
    for action, at, d in evs:
        d = d or {}
        skew = None
        sent = d.get("at")
        if isinstance(sent, str) and at is not None:
            try:
                skew = round((_aware(at) - datetime.fromisoformat(sent.replace("Z", "+00:00"))).total_seconds(), 1)  # type: ignore[operator]
            except ValueError:
                skew = None
        events.append({"type": str(action).removeprefix("agent.event.").removeprefix("agent."), "at": _iso(at),
                       "clock_skew_s": skew, "detail": d.get("detail", {k: v for k, v in d.items() if k != "at"})})
    return a, events


def _revisions(c: Connection) -> list[dict[str, Any]]:
    rows = c.execute(select(config_revision).order_by(config_revision.c.id.desc()).limit(3)).mappings().all()
    out = []
    for r in rows:
        body = r["body"] or {}
        paths = sorted(_paths(body))[:30]
        out.append({"id": r["id"], "at": _iso(r["created_at"]), "reason": r["reason"], "changed_paths": paths})
    return out


def _paths(d: Any, prefix: str = "") -> set[str]:
    if not isinstance(d, dict):
        return {prefix} if prefix else set()
    out: set[str] = set()
    for k, v in d.items():
        out |= _paths(v, f"{prefix}.{k}" if prefix else str(k))
    return out


def _positions(c: Connection, t: Trigger, probe_destination: bool) -> dict[str, Any]:
    if not t.connection_id:
        return {}
    m = c.execute(select(checkpoint_mirror).where(checkpoint_mirror.c.connection_id == t.connection_id)
                  .limit(1)).mappings().first()
    last = c.execute(select(batch.c.to_lri, batch.c.next_lri, batch.c.engine).where(
        batch.c.connection_id == t.connection_id).order_by(batch.c.seq.desc()).limit(1)).first()
    out: dict[str, Any] = {
        "mirror_position": m["position"] if m else None, "mirror_last_seq": m["last_seq"] if m else None,
        "open_txns": m["open_txns"] if m else None, "lag_s": _age_s(m["last_commit_ts"]) if m else None,
        "last_batch_to": last[0] if last else None, "last_batch_next": last[1] if last else None}
    if probe_destination and m is not None and last is not None:
        out.update(_dest_checkpoint(t.connection_id, str(m["stream"]), str(last[2]), m["position"]))
    return out


def _dest_checkpoint(cid: str, stream: str, engine: str, mirror: str | None) -> dict[str, Any]:
    """Read the destination checkpoint (5 s cap). Failure text is evidence (destination down / auth)."""
    def read() -> tuple[str | None, int | None]:
        from alelt_hub import store
        from alelt_hub.pipeline import make_destination
        from alelt_hub.db import get_engine
        from alelt_hub.settings import get_settings

        s = get_settings()
        src = store.source_connector(engine)
        if src is None:
            return None, None
        with get_engine().connect() as c:            # F-18: active config revision
            pol = store.connection_policy(c, cid, s.tables_config)
        dest = make_destination(s, pol, src)
        ck = dest.read_checkpoint(stream)
        cmp = None
        if ck and mirror:
            cmp = src.compare(Position(engine=engine, text=ck.text), Position(engine=engine, text=mirror))
        return (ck.text if ck else None), cmp
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        text, cmp = ex.submit(read).result(timeout=5)
        return {"destination_checkpoint": text, "destination_vs_mirror": cmp}
    except concurrent.futures.TimeoutError:
        return {"destination_error": "destination checkpoint read timed out after 5 s"}
    except Exception as e:  # noqa: BLE001 - the error itself is evidence
        return {"destination_error": f"{type(e).__name__}: {e}"[:600]}
    finally:
        ex.shutdown(wait=False)


def _metrics(c: Connection, t: Trigger) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for kind, n in c.execute(select(job.c.kind, func.count()).where(job.c.status == "QUEUED").group_by(job.c.kind)):
        out[f"queue_depth.{kind}"] = int(n)
    out["jobs_failed"] = int(c.execute(select(func.count()).select_from(job).where(job.c.status == "FAILED"))
                             .scalar_one())
    if t.connection_id:
        out["unloaded_batches"] = int(c.execute(select(func.count()).select_from(batch).where(
            batch.c.connection_id == t.connection_id, batch.c.status.in_(["RECEIVED", "DECODED"]))).scalar_one())
        since = utcnow() - timedelta(minutes=15)
        out["quarantined_15m"] = int(c.execute(select(func.count()).select_from(quarantine_event).where(
            quarantine_event.c.connection_id == t.connection_id, quarantine_event.c.created_at >= since)).scalar_one())
    try:
        from alelt_hub.metrics import REGISTRY

        counters = getattr(REGISTRY, "_counters", {})
        for name in ("alelt_decode_errors_total", "alelt_quarantine_total", "alelt_batches_received_total"):
            for labels, v in list(counters.get(name, {}).items())[:20]:
                out[f"{name}{{{','.join(f'{a}={b}' for a, b in labels)}}}"] = v
    except Exception:  # noqa: BLE001
        pass
    return out


def _recon(c: Connection, t: Trigger) -> list[dict[str, Any]]:
    ref = _split(t.table_fqn)
    if not ref:
        return []
    tid = c.execute(select(source_table.c.id).where(source_table.c.src_schema == ref[0],
                                                    source_table.c.src_table == ref[1]).limit(1)).scalar()
    if tid is None:
        return []
    rows = c.execute(select(recon_result).where(recon_result.c.table_id == tid)
                     .order_by(recon_result.c.created_at.desc()).limit(5)).mappings().all()
    return [{"outcome": r["outcome"], "detail": r["detail"], "drill": r["drill"], "at": _iso(r["created_at"])}
            for r in rows]


def collect(engine: Engine, t: Trigger, level: str = "strict", probe_destination: bool = False) -> EvidencePack:
    """Assemble and redact the evidence pack for a trigger (read-only)."""
    with engine.connect() as c:
        jb = _job(c, t)
        bt = _batch(c, t)
        if bt and not t.connection_id:
            t = t.model_copy(update={"connection_id": c.execute(select(batch.c.connection_id).where(
                batch.c.id == _u(t.batch_id))).scalar()})
        samples, counts = _quarantine(c, t, level)
        if not t.table_fqn and samples and samples[0].get("table"):
            t = t.model_copy(update={"table_fqn": samples[0]["table"]})
        cat = _catalog(c, t, bt["catalog_version_id"] if bt else None)
        sc = _schema_changes(c, t)
        ag, evs = _agent(c, t)
        revs = _revisions(c)
        pos = _positions(c, t, probe_destination)
        met = _metrics(c, t)
        rec = _recon(c, t)
    logs = logbuf.recent(batch_id=t.batch_id, table=t.table_fqn, connection_id=t.connection_id)
    raw = {"collected_at": utcnow(), "redaction": level, "trigger": t.model_dump(mode="json"), "job": jb, "batch": bt,
           "quarantine": samples, "quarantine_reasons": counts, "catalog": cat, "schema_changes": sc, "agent": ag,
           "agent_events": evs, "config_revisions": revs, "positions": pos, "metrics": met, "logs": logs,
           "recon": rec}
    red = redact({k: v for k, v in raw.items() if k != "collected_at"}, level)
    # redact() masks the `value(s)`-like keys; quarantine samples carry raw_hex_head (already cut) - keep as is
    for q, orig in zip(red["quarantine"], samples, strict=True):
        q["raw_hex_head"] = orig["raw_hex_head"]
    return EvidencePack.model_validate({**red, "collected_at": raw["collected_at"]})

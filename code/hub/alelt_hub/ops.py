"""Consumers for the operator jobs the console / incident actions enqueue (REQ-16, X-33 deviation closed):

- `resync`    {table, connection_id}: alelt_snapshot resync of the table (snapshot + catch-up + verify), alelt_hub.snapshots.
- `reprocess` {table, connection_id, from_pos, to_pos}: re-decode the table's batches from the hub spool (raw archive)
               starting at from_pos and re-apply them up to what the main stream applied (a decoder fix, a source
               policy change). Replays never go beyond the main stream, so the table converges to the current state.
- `verify`    {run_id, table_ids, check_kind}: alelt_verify default suite (counts + aggregates per key range + sampled
               rows + key diff on mismatch; hash buckets only with ALELT_VERIFY_HASH_BUCKETS>0). Remote sources are read
               live from the hub; edge sources are verified ON the edge by the whitelisted VERIFY_TABLE command
               (alelt_hub.edge_verify, R4/R5): aggregates per key range computed by the source, keys / rows only where
               something differs. A mismatch while the destination is still behind the edge read is retried later
               (ALELT_VERIFY_SETTLE_ATTEMPTS, default 3, 30 s apart) before it is recorded. Edge recon runs wait for the
               table's verify/snapshot window unless `force` (post-snapshot reconciliation).
               LEGACY_PARITY: reference-landing compare in the same destination (alelt_verify.reference, X-124):
               reference schema = tables document `legacy_schema`, mapping from the output profiles or an extension
               (`config/extensions.yaml: reference_mappings`).
Results go to recon_result (outcome MATCH | MISMATCH | ERROR, detail, drill = located failures).
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

from sqlalchemy import Engine, select, update

from alelt_common.canonical_jsonl import Undecodable
from alelt_common.models import TableRef
from alelt_common.util import utcnow
from alelt_verify import ListSide, MemoryReconStore, Verifier
from alelt_verify.runner import DestinationSide

from . import store
from .db import recon_result, recon_run, source_table
from .dialect import insert

log = logging.getLogger("alelt_hub.ops")


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


def verify_table(engine: Engine, pipeline: Any, fqn: str, check_kind: str = "COLUMN_CHECKSUM") -> Any:
    """Default suite source vs destination. Remote engines are read live from the hub; edge engines through the
    agent's VERIFY_TABLE (alelt_hub.edge_verify). Returns alelt_verify SuiteResult."""
    from alelt_capture.config import connections

    if getattr(pipeline.source, "capture_mode", "edge") != "remote":
        from .edge_verify import verify_edge_table

        return verify_edge_table(engine, pipeline, fqn, check_kind)
    ref = _ref(fqn)
    with engine.connect() as c:
        vid = store.latest_catalog_id(c, pipeline.cid)
        cat = store.load_catalog(c, vid) if vid else None
    info = store.table_infos(cat).get(ref) if cat else None
    if info is None:
        raise LookupError(f"{fqn} not in catalog")
    rc = connections(engine).get(pipeline.cid)
    if rc is None:
        raise LookupError(f"{pipeline.cid}: no live source settings")
    pol = pipeline.policy
    dinfo = pol.project(info)
    cols = [c.name for c in sorted(dinfo.columns, key=lambda c: c.ordinal)]
    keys = [c.name for c in dinfo.key_columns()]
    src = rc.factory()()
    try:
        before = None
        try:                          # HC-15: consistency point - the destination must hold everything before it
            p0 = src.current_position()
            before = p0.text if p0 is not None else None
        except Exception:  # noqa: BLE001 - no position: the result is reported as not settled
            before = None
        raw: list[dict[str, Any]] = []
        for page in src.snapshot_rows(info, None, 10_000):
            raw.extend(page)
    finally:
        src.close()
    undecodable = sum(1 for r in raw if any(isinstance(v, Undecodable) for v in r.values()))
    ref_rows = [pol.apply_row(ref, r) or {} for r in raw if not any(isinstance(v, Undecodable) for v in r.values())]
    fetch = getattr(pipeline.dest, "fetch_rows", None)
    if fetch is None:
        raise RuntimeError(f"destination {type(pipeline.dest).__name__} has no row access for verification")
    rs = ListSide(lambda _t: ref_rows).rows(fqn)
    cs = ListSide(lambda _t: fetch(pol.dest_name(ref)), deleted=pol.system.deleted).rows(fqn)
    v = Verifier(DestinationSide(None), DestinationSide(None), MemoryReconStore())
    hb = int(os.environ.get("ALELT_VERIFY_HASH_BUCKETS", "0") or 0)
    out = v.default_suite(fqn, rs, cs, keys, cols, hash_buckets=hb,
                          key_diff="always" if check_kind in ("KEY_SET", "DELETES") else "on_mismatch")
    out.summary["undecodable_source_rows"] = undecodable
    from .edge_verify import settled

    out.summary["position_before"] = before
    out.summary["settled"] = settled(engine, pipeline, fqn, before)   # unsettled MISMATCH is retried, not recorded
    return out


def _reference_mapping(pipeline: Any, doc: dict[str, Any]) -> Any:
    """Mapping for LEGACY_PARITY: an extension's mapping for the output profile, else the generic profile mapping."""
    from alelt_verify.reference import mapping_from_profiles

    prof_name = str(doc.get("output_profile") or "")
    try:
        from alelt_schema.config import load_extensions

        from alelt_common.util import load
        from alelt_import.plugin import REPO

        for ext in load_extensions(REPO):
            ref = (getattr(ext, "reference_mappings", None) or {}).get(prof_name)
            if ref:
                return load(ref)(cand_profile=getattr(pipeline.policy, "system", None))
    except Exception as e:  # noqa: BLE001 - fall back to the generic mapping, recorded in the summary
        log.warning("reference mapping extension unavailable: %s", e, extra={"component": "alelt_hub"})
    ref_profile = None
    if prof_name:
        try:
            from alelt_loader.policy import _profile_files
            from alelt_schema.config import load_profile

            ref_profile = load_profile(_profile_files(""), prof_name)
        except Exception:  # noqa: BLE001
            ref_profile = None
    sysc = getattr(pipeline.policy, "system", None)
    cand = {"synced_column": getattr(sysc, "synced", None), "deleted_column": getattr(sysc, "deleted", None),
            "id_column": getattr(sysc, "row_id", None)} if sysc is not None else None
    return mapping_from_profiles(ref_profile, cand)


def legacy_parity(engine: Engine, pipeline: Any, fqn: str) -> Any:
    """X-124: reference landing (legacy_schema) vs our landing, same destination, settled window."""
    from datetime import timedelta

    from alelt_verify.edge import factory
    from alelt_verify.reference import run_reference_suite, schema_rows

    with engine.connect() as c:
        doc = (store.pipeline_doc(c, pipeline.cid) or {}).get("tables_doc") or {}
    legacy = doc.get("legacy_schema") or os.environ.get("ALELT_LEGACY_SCHEMA")
    if not legacy:
        raise RuntimeError("LEGACY_PARITY: no reference landing (tables document legacy_schema)")
    ref = _ref(fqn)
    with engine.connect() as c:
        vid = store.latest_catalog_id(c, pipeline.cid)
        cat = store.load_catalog(c, vid) if vid else None
    info = store.table_infos(cat).get(ref) if cat else None
    if info is None:
        raise LookupError(f"{fqn} not in catalog")
    keys = [c.name for c in info.key_columns()]
    pol = pipeline.policy
    dname = pol.dest_name(ref)
    m = _reference_mapping(pipeline, doc)
    rows = schema_rows(pipeline.dest)
    ref_rows = rows(str(legacy), dname)
    fetch = getattr(pipeline.dest, "fetch_rows", None)
    if fetch is None:
        raise RuntimeError(f"destination {type(pipeline.dest).__name__} has no row access for verification")
    cand_rows = fetch(dname)
    settle = utcnow() - timedelta(seconds=float(os.environ.get("ALELT_PARITY_SETTLE_S", "3600")))
    return run_reference_suite(factory(fqn), ref_rows, cand_rows, keys, m, settled_before=settle,
                               hash_buckets=int(os.environ.get("ALELT_VERIFY_HASH_BUCKETS", "0") or 0))


def consume_verify(engine: Engine, worker: Any, payload: dict[str, Any]) -> int:
    rid = uuid.UUID(str(payload["run_id"]))
    kind = str(payload.get("check_kind") or "COLUMN_CHECKSUM")
    with engine.connect() as c:
        q = select(source_table.c.id, source_table.c.connection_id, source_table.c.src_schema, source_table.c.src_table)
        ids = [uuid.UUID(str(t)) for t in payload.get("table_ids") or []]
        tabs = c.execute(q.where(source_table.c.id.in_(ids)) if ids else q).all()
    n = 0
    requeued = False
    for tid, cid, s, t in tabs:
        fqn = f"{s}.{t}"
        try:
            p = worker.pipeline(str(cid))
            if not p.policy.table(_ref(fqn)).enabled:
                continue
            if kind == "LEGACY_PARITY":
                res = legacy_parity(engine, p, fqn)
            else:
                from .edge_verify import in_window, is_edge

                if is_edge(p) and not payload.get("force"):
                    ok, wait = in_window(p)
                    if not ok:                     # edge reads wait for the off-peak window
                        _requeue(engine, payload, tid, wait, "window")
                        requeued = True
                        continue
                res = verify_table(engine, p, fqn, kind)
                att = int(payload.get("attempt") or 0)
                if (res.outcome == "MISMATCH" and res.summary.get("settled") is False
                        and att + 1 < int(os.environ.get("ALELT_VERIFY_SETTLE_ATTEMPTS", "3"))):
                    _requeue(engine, payload, tid, 30.0, f"settle{att + 1}", attempt=att + 1)
                    requeued = True
                    continue
            outcome = res.outcome
            detail = json.dumps(res.summary, default=str)
            drill = [json.loads(r.model_dump_json()) for r in res.results if r.status.value != "PASS"][:200]
        except Exception as e:  # noqa: BLE001 - recorded as evidence
            from .edge_verify import AgentUnavailable, agent_unavailable

            outcome, detail, drill = "ERROR", f"{type(e).__name__}: {e}"[:2000], []
            if isinstance(e, AgentUnavailable):     # bounded wait elapsed / agent offline: incident + retry later
                att = int(payload.get("agent_attempt") or 0) + 1
                agent_unavailable(engine, str(cid), fqn, e, att)
                if att < int(os.environ.get("ALELT_VERIFY_AGENT_RETRIES", "5")):
                    _requeue(engine, payload, tid, 60.0 * att, f"agent{att}", agent_attempt=att)
                    requeued = True
        with engine.begin() as c:
            c.execute(insert(recon_result).values(id=uuid.uuid4(), run_id=rid, table_id=tid, outcome=outcome,
                                                  detail=detail, drill=drill))
        n += 1
        if outcome == "MISMATCH" and kind == "POST_SNAPSHOT":
            _post_snapshot_mismatch(engine, worker, str(cid), fqn, detail)
        if outcome == "MATCH":
            try:
                from .snapshots import SnapshotService

                SnapshotService(engine, worker.pipeline(str(cid))).reconciled(fqn, outcome, detail)
            except Exception:  # noqa: BLE001
                log.exception("snapshot state update after recon failed", extra={"component": "alelt_hub"})
    if not requeued:
        with engine.begin() as c:
            c.execute(update(recon_run).where(recon_run.c.id == rid).values(finished_at=utcnow()))
    return n


def _requeue(engine: Engine, payload: dict[str, Any], tid: Any, delay_s: float, tag: str, **extra: Any) -> None:
    """Verify one table again later (outside the window / destination not settled yet)."""
    from . import jobs

    body = {**payload, "table_ids": [str(tid)], **extra}
    with engine.begin() as c:
        jobs.enqueue(c, "verify", body, dedup_key=f"verify:{payload['run_id']}:{tid}:{tag}", delay_s=delay_s)


def _post_snapshot_mismatch(engine: Engine, worker: Any, cid: str, fqn: str, detail: str) -> None:
    """A settled post-snapshot MISMATCH moves the table to RESYNC (bounded by ALELT_LIFECYCLE_MAX_RESYNC, default 2;
    beyond it the table stays unverified for the operator - certification needs `verified_at`)."""
    from . import lifecycle

    r = lifecycle.get(engine, cid, fqn)
    if r is None:
        return
    if int(r["resync_count"] or 0) >= int(os.environ.get("ALELT_LIFECYCLE_MAX_RESYNC", "2")):
        lifecycle.move(engine, cid, fqn, str(r["phase"]), body={"verify": f"MISMATCH (resync limit): {detail}"[:300]})
        return
    try:
        lifecycle.resync(engine, worker, cid, fqn, "post-snapshot reconciliation MISMATCH")
    except Exception:  # noqa: BLE001 - recorded; the operator can resync from the console
        log.exception("resync after post-snapshot mismatch failed", extra={"component": "alelt_hub", "table": fqn})


def consume_resync(engine: Engine, worker: Any, payload: dict[str, Any]) -> list[str]:
    from .snapshots import SnapshotService

    cid, fqn = str(payload["connection_id"]), str(payload["table"])
    return SnapshotService(engine, worker.pipeline(cid)).start([fqn], resync=True,
                                                              reason=str(payload.get("source") or "console resync"))


def consume_reprocess(engine: Engine, worker: Any, payload: dict[str, Any], job_id: Any) -> int:
    """Under the pipeline lock (caller): re-apply the table from from_pos (inclusive) into its own checkpoint stream."""
    from . import integrity

    cid, fqn = str(payload["connection_id"]), str(payload["table"])
    if integrity.blocks_replay(engine, cid, fqn):   # held for integrity: its recovery re-decodes (never past the record)
        integrity.enqueue(engine, cid, fqn)
        return 0
    p = worker.pipeline(cid)
    p.process_pending()                        # main stream first: the replay stops where it stands
    # raw spool decoded again (a decoder fix / source policy change is the reason for a reprocess, HC-02); `to_pos`
    # is informational: a replay never goes beyond what the main stream applied (HC-28)
    return int(p.replay_table(_ref(fqn), str(payload["from_pos"]), f"{p.stream}#reprocess:{fqn}:{job_id}",
                              inclusive=True, bypass_cache=True))

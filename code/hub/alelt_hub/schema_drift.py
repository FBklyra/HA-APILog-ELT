"""Schema drift on the data path (HC-10, alelt_schema wired into the runtime).

When the catalog version used for a batch differs from the one used for the previous batch, `on_catalog_change`
diffs the two versions (alelt_schema.drift) BEFORE the batch is applied and acts per `schema_change_handling`
(tables.yaml: ALLOW_ALL | ALLOW_COLUMNS (default) | BLOCK_ALL, alelt_schema.policy):

- APPLY (additive, allowed): COLUMN_ADDED -> `dest.add_column` (projected through the column policy) now; a new table
  is created by the loader on its first change (`allow_new_tables`); recorded as `schema_change_event` APPLIED.
- PENDING (needs approval: altered type / length, key change, drop, BLOCK_ALL, unmapped type): the table is HELD
  (`table_hold` kind `schema`, at the last flushed position) so a widened value never reaches a MERGE that would fail
  and stall the stream; every other table keeps flowing. `schema_change_event` PENDING (console approve / reject).
- Decisions are applied by the decode job (`apply_decisions`, pipeline lock): APPROVED -> destination change
  (`alter_column` when the destination has it, else `ensure_table` = create-or-evolve additively; COLUMN_ADDED ->
  add_column) -> status APPLIED -> the hold is released through an attach pass (its changes replayed, exact);
  REJECTED -> COLUMN_ADDED: column disabled (console override, revisioned) and released; other kinds stay held with
  the reason (operator: resync or disable the table).
- Informational (no hold): COLUMN_DROPPED (landing keeps the column), CAPTURE_ENABLED, COMPRESSION_CHANGED;
  CAPTURE_LOST / TABLE_DROPPED raise an incident (alelt_diagnose) and are recorded.
- Views: after an applied change, `dest.publish_view(view_schema, table, landing, columns)` when the tables document
  names a `view_schema` and the destination implements it.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import select, update

from alelt_common.models import CatalogSnapshot, ColumnInfo, TableRef
from alelt_common.util import utcnow
from alelt_schema.config import Policy
from alelt_schema.drift import DriftEvent, DriftKind, diff_catalogs
from alelt_schema.policy import Action, decide

from . import holds
from .db import schema_change_event, source_table
from .dialect import insert
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub.schema_drift")
KIND = "schema"
INFO_ONLY = {DriftKind.COLUMN_DROPPED, DriftKind.CAPTURE_ENABLED, DriftKind.COMPRESSION_CHANGED}
INCIDENT = {DriftKind.CAPTURE_LOST, DriftKind.TABLE_DROPPED}


def _policy_value(p: Any) -> Policy:
    v = getattr(p.policy, "schema_change_handling", None) or "ALLOW_COLUMNS"
    try:
        return Policy(v)
    except ValueError:
        return Policy.ALLOW_COLUMNS


def _action(ev: DriftEvent, p: Any) -> Action:
    pol = _policy_value(p)
    if ev.kind == DriftKind.TABLE_ADDED:          # same rule as the loader's allow_new_tables
        return Action.PENDING if pol == Policy.BLOCK_ALL else Action.APPLY
    if ev.kind in INFO_ONLY or ev.kind in INCIDENT:
        return Action.IGNORE
    return decide(ev, pol, p.policy.table(ev.table).enabled)


def _table_id(c: Any, cid: str, ref: TableRef) -> Any:
    return c.execute(select(source_table.c.id).where(source_table.c.connection_id == cid,
                                                     source_table.c.src_schema == ref.schema_,
                                                     source_table.c.src_table == ref.name)).scalar()


def _record(p: Any, ev: DriftEvent, status: str, position: str | None) -> str:
    sid = uuid.uuid4()
    with p.engine.begin() as c:
        c.execute(insert(schema_change_event).values(
            id=sid, table_id=_table_id(c, p.cid, ev.table), kind=ev.kind.value, status=status,
            detail={"table": ev.table.fqn, "column": ev.column, "old": ev.old, "new": ev.new,
                    "connection_id": p.cid}, position=position, policy=_policy_value(p).value))
    REGISTRY.inc("alelt_schema_changes_total", connection_id=p.cid, kind=ev.kind.value, status=status)
    return str(sid)


def on_catalog_change(p: Any, old: CatalogSnapshot | None, new: CatalogSnapshot, b: dict[str, Any],
                      held: set[str]) -> list[str]:
    """Diff the catalog versions of two consecutive batches; act before batch `b` is applied. Returns actions."""
    if old is None:
        return []
    acts: list[str] = []
    pos = p.flushed_position()
    for ev in diff_catalogs(old, new):
        if ev.kind != DriftKind.TABLE_ADDED and not p.policy.table(ev.table).enabled:
            continue
        a = _action(ev, p)
        if ev.kind in INCIDENT:
            _incident(p, ev, b)
        if a == Action.APPLY:
            if ev.kind == DriftKind.COLUMN_ADDED and ev.new is not None:
                _add_column(p, ev)
            _record(p, ev, "APPLIED", pos)
            _publish_view(p, new, ev.table)
            acts.append(f"apply:{ev.describe()}")
        elif a == Action.PENDING:
            sid = _record(p, ev, "PENDING", pos)
            _hold(p, ev, sid, held)
            acts.append(f"hold:{ev.describe()}")
        else:
            _record(p, ev, "APPLIED", pos)                    # informational, visible in the console history
            acts.append(f"info:{ev.describe()}")
    if acts:
        log.warning("schema drift", extra={"component": "alelt_hub", "batch_id": str(b["id"]),
                                           "correlation_id": f"{p.cid}: {'; '.join(acts)[:500]}"})
    return acts


def _hold(p: Any, ev: DriftEvent, sid: str, held: set[str]) -> None:
    from . import integrity

    fqn = ev.table.fqn
    r = holds.get(p.engine, p.cid, fqn, KIND)
    if r is not None and r["held"]:
        ids = list(((r["body"] or {}).get("events")) or [])
        holds.modify(p.engine, p.cid, fqn, KIND, events=[*ids, sid])
        return
    start = integrity.watermark_for_new_hold(p, fqn)
    rseq = integrity.restart_for_new_hold(p, fqn)
    body: dict[str, Any] = {"events": [sid], "reason": f"schema change needs approval: {ev.describe()}",
                            "since": utcnow().isoformat()}
    if rseq is not None:
        body["restart_seq"] = rseq
    holds.put(p.engine, p.cid, fqn, KIND, "PENDING", True, start, body)
    held.add(fqn)


def _add_column(p: Any, ev: DriftEvent) -> None:
    col = ColumnInfo.model_validate(ev.new)
    cp = p.policy.table(ev.table).columns.get(col.name)
    if cp is not None and not cp.enabled:
        return
    info = p.tables.get(ev.table)
    if info is not None:
        proj = p.policy.project(info.model_copy(update={"columns": [*info.columns, col]}))
        col = next((c for c in proj.columns if c.name == col.name), col)
    p.dest.add_column(p.policy.dest_name(ev.table), col)


def _publish_view(p: Any, cat: CatalogSnapshot, ref: TableRef) -> None:
    from . import config_store

    with p.engine.connect() as c:
        doc = config_store.base_doc(c, p.cid, p.s.tables_config) or {}
    vs = doc.get("view_schema") if isinstance(doc, dict) else None
    info = next((t for t in cat.tables if t.table == ref), None)
    if not vs or info is None:
        return
    cols = [c.name for c in sorted(p.policy.project(info).columns, key=lambda x: x.ordinal)]
    cols += [p.policy.system.synced, p.policy.system.deleted]
    try:
        p.dest.publish_view(str(vs), p.policy.dest_name(ref), p.policy.dest_schema or p.cid, cols)
    except NotImplementedError:
        pass


def _incident(p: Any, ev: DriftEvent, b: dict[str, Any]) -> None:
    try:
        from alelt_diagnose.incidents import open_incident
        from alelt_diagnose.models import IncidentKind, Trigger

        kind = IncidentKind.CAPTURE_LOST if ev.kind == DriftKind.CAPTURE_LOST else IncidentKind.TABLE_OUT_OF_SYNC
        open_incident(p.engine, Trigger(kind=kind, connection_id=p.cid, table_fqn=ev.table.fqn, batch_id=str(b["id"]),
                                        stage="decode", error=ev.describe()))
    except Exception:  # noqa: BLE001
        log.exception("schema incident failed", extra={"component": "alelt_hub"})


def apply_decisions(p: Any, held: set[str]) -> list[str]:
    """Control lane (`schema` job): act on console decisions for schema-held tables of this connection (held meanwhile:
    the main stream does not apply them, so no pipeline lock is needed)."""
    from . import replay

    acts: list[str] = []
    for r in holds.rows(p.engine, p.cid, KIND):
        if not r["held"] or replay.pass_of(r):
            continue
        ids = [uuid.UUID(x) for x in ((r["body"] or {}).get("events") or [])]
        with p.engine.connect() as c:
            evs = [dict(x) for x in c.execute(select(schema_change_event).where(schema_change_event.c.id.in_(ids)))
                   .mappings()]
        if not evs or any(e["status"] == "PENDING" for e in evs):
            continue
        blocked = False
        for e in evs:
            d = e["detail"] or {}
            ref = TableRef(schema=r["table_fqn"].partition(".")[0], name=r["table_fqn"].partition(".")[2])
            if e["status"] == "APPROVED":
                _apply_approved(p, ref, e["kind"], d)
                _set_status(p, e["id"], "APPLIED")
            elif e["status"] == "REJECTED":
                if e["kind"] == DriftKind.COLUMN_ADDED.value and d.get("column"):
                    _disable_column(p, ref, str(d["column"]))
                    _set_status(p, e["id"], "REJECTED")
                else:
                    blocked = True                    # stays held: resync or disable the table (operator)
        if blocked:
            holds.modify(p.engine, p.cid, r["table_fqn"], KIND, state="REJECTED",
                         reason="schema change rejected: resync or disable the table")
            continue
        p.refresh_policy()
        replay.mark_pass(p, [(r["table_fqn"], KIND)], "attach")
        acts.append(f"release:{r['table_fqn']}")
    return acts


def _apply_approved(p: Any, ref: TableRef, kind: str, d: dict[str, Any]) -> None:
    info = p.tables.get(ref)
    name = p.policy.dest_name(ref)
    if kind == DriftKind.COLUMN_ADDED.value and d.get("new"):
        _add_column(p, DriftEvent(kind=DriftKind.COLUMN_ADDED, table=ref, column=d.get("column"), new=d["new"]))
    elif kind in (DriftKind.COLUMN_ALTERED.value, DriftKind.TABLE_ADDED.value, DriftKind.KEY_CHANGED.value) \
            and info is not None:
        alter = getattr(p.dest, "alter_column", None)
        col = next((c for c in p.policy.project(info).columns if c.name == d.get("column")), None)
        if callable(alter) and col is not None and kind == DriftKind.COLUMN_ALTERED.value:
            alter(name, col)
        else:
            p.dest.ensure_table(p.policy.project(info), name, None)   # create-or-evolve additively


def _disable_column(p: Any, ref: TableRef, col: str) -> None:
    """Rejected new column: disabled in a new config revision (F-18 store), so the policy excludes it everywhere."""
    from . import config_store

    with p.engine.begin() as c:
        doc = config_store.base_doc(c, p.cid, p.s.tables_config)
        new = config_store.apply_table_change(doc, ref.schema_, ref.name,
                                              {"columns": [{"name": col, "enabled": False, "hashed": False}]})
        config_store.commit(c, p.cid, new, "system:schema", f"schema change rejected: {ref.fqn}.{col}", "schema",
                            {"table": ref.fqn, "column": col})


def _set_status(p: Any, sid: Any, status: str) -> None:
    with p.engine.begin() as c:
        c.execute(update(schema_change_event).where(schema_change_event.c.id == sid).values(status=status))


__all__ = ["KIND", "apply_decisions", "on_catalog_change"]

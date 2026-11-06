"""Metadata operations shared by gateway, console and worker."""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextvars import ContextVar
from datetime import timedelta
from typing import Any

from sqlalchemy import Connection, func, select, update

from alelt_common.models import CatalogSnapshot, Command, CommandName, TableInfo, TableRef
from alelt_common.spi import SourceConnector
from alelt_common.util import load, sha256_hex, utcnow

from .db import (
    agent_command,
    audit_event,
    catalog_version,
    column_config,
    source_column,
    source_table,
    table_config,
)
from .dialect import insert
from .settings import get_settings

log = logging.getLogger("alelt_hub")


# ------------------------------------------------------------------ plugins
_SRC_OK: dict[str, SourceConnector] = {}
_SRC_FAIL: dict[str, float] = {}          # engine -> monotonic time of the last failed load
SOURCE_RETRY_S = 60.0


def source_connector(engine: str) -> SourceConnector | None:
    """Instantiate the configured SourceConnector for `engine`; None if unavailable (logged). A successful load is
    cached for the process; a failed one is retried after SOURCE_RETRY_S (HC-31: a transient import failure, e.g. a
    driver installed after start, never disables the engine for the process lifetime)."""
    hit = _SRC_OK.get(engine)
    if hit is not None:
        return hit
    failed = _SRC_FAIL.get(engine)
    if failed is not None and time.monotonic() - failed < SOURCE_RETRY_S:
        return None
    ref = get_settings().sources.get(engine)
    if not ref:
        return None
    try:
        obj = load(ref)()
    except Exception as e:  # noqa: BLE001
        _SRC_FAIL[engine] = time.monotonic()
        log.warning("source connector %s unavailable: %s", ref, e, extra={"component": "alelt_hub"})
        return None
    _SRC_FAIL.pop(engine, None)
    _SRC_OK[engine] = obj
    return obj  # type: ignore[no-any-return]


def _source_cache_clear() -> None:
    _SRC_OK.clear()
    _SRC_FAIL.clear()


source_connector.cache_clear = _source_cache_clear  # type: ignore[attr-defined]


# ------------------------------------------------------------------ audit
CORRELATION_ID: ContextVar[str | None] = ContextVar("alelt_correlation_id", default=None)


def audit(c: Connection, actor: str, action: str, target: str | None = None,
          detail: dict[str, Any] | None = None, role: str | None = None) -> None:
    d = dict(detail or {})
    if CORRELATION_ID.get():
        d.setdefault("correlation_id", CORRELATION_ID.get())
    c.execute(insert(audit_event).values(id=uuid.uuid4(), actor=actor, role=role, action=action,
                                         target=target, detail=d))


# ------------------------------------------------------------------ catalog
def ingest_catalog(c: Connection, snap: CatalogSnapshot) -> tuple[int, bool]:
    """Store a catalog version (dedup by content hash) and refresh source_table/source_column.

    Returns (version_id, created)."""
    src = source_connector(snap.engine)
    if src is not None:
        for t in snap.tables:
            for col in t.columns:
                col.canonical_type = src.canonical_type(col)
    body = json.loads(snap.model_dump_json(by_alias=True))
    digest = sha256_hex(json.dumps(body["tables"], sort_keys=True).encode())
    # dedup against the LATEST version only: content A -> B -> A stores A again with its new valid_from_pos, so the
    # pipeline (newest valid_from <= batch position) never picks B for batches after the change back.
    latest = c.execute(select(catalog_version.c.id, catalog_version.c.body)
                       .where(catalog_version.c.connection_id == snap.connection_id)
                       .order_by(catalog_version.c.id.desc()).limit(1)).first()
    if latest is not None and sha256_hex(json.dumps(latest[1]["tables"], sort_keys=True).encode()) == digest:
        vid, created = int(latest[0]), False
    else:
        row_sha = digest
        if c.execute(select(catalog_version.c.id).where(catalog_version.c.connection_id == snap.connection_id,
                                                        catalog_version.c.sha256 == digest)).first() is not None:
            row_sha = sha256_hex(f"{digest}|{body.get('position')}|{snap.taken_at.isoformat()}".encode())
        valid_from = snap.position.text if snap.position else None
        if valid_from is None:   # F-28: a catalog must carry a position; default = hub durable position at receipt
            from .db import batch

            last = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == snap.connection_id)
                             .order_by(batch.c.seq.desc()).limit(1)).first()
            valid_from = (last[0] or last[1]) if last is not None else None   # None only before the first batch
        ins = insert(catalog_version).values(
            connection_id=snap.connection_id, valid_from_pos=valid_from,
            taken_at=snap.taken_at, sha256=row_sha, body=body).on_conflict_do_nothing(
            index_elements=["connection_id", "sha256"]).returning(catalog_version.c.id)
        vid = c.execute(ins).scalar()
        created = vid is not None
        if vid is None:
            vid = c.execute(select(catalog_version.c.id).where(
                catalog_version.c.connection_id == snap.connection_id,
                catalog_version.c.sha256 == row_sha)).scalar_one()
    for t in snap.tables:
        tid = c.execute(insert(source_table).values(
            id=uuid.uuid4(), connection_id=snap.connection_id, src_schema=t.table.schema_, src_table=t.table.name,
            capture_enabled=t.capture_enabled, compression=t.compression, engine_ids=t.engine_ids,
        ).on_conflict_do_update(
            index_elements=["connection_id", "src_schema", "src_table"],
            set_={"capture_enabled": t.capture_enabled, "compression": t.compression, "engine_ids": t.engine_ids},
        ).returning(source_table.c.id)).scalar_one()
        c.execute(insert(table_config).values(id=uuid.uuid4(), table_id=tid).on_conflict_do_nothing(
            index_elements=["table_id"]))
        for col in t.columns:
            vals = dict(engine_type=col.engine_type, canonical_type=col.canonical_type.value, ordinal=col.ordinal,
                        length=col.length, scale=col.scale, nullable=col.nullable, key_seq=col.key_seq,
                        codepage=col.codepage)
            cid = c.execute(insert(source_column).values(id=uuid.uuid4(), table_id=tid, name=col.name, **vals)
                            .on_conflict_do_update(index_elements=["table_id", "name"], set_=vals)
                            .returning(source_column.c.id)).scalar_one()
            c.execute(insert(column_config).values(id=uuid.uuid4(), column_id=cid).on_conflict_do_nothing(
                index_elements=["column_id"]))
    from . import lifecycle

    lifecycle.sync_catalog(c, snap.connection_id)          # every enabled table gets a lifecycle row (RC2)
    return int(vid), created


def latest_catalog_id(c: Connection, connection_id: str) -> int | None:
    return c.execute(select(catalog_version.c.id).where(catalog_version.c.connection_id == connection_id)
                     .order_by(catalog_version.c.id.desc()).limit(1)).scalar()


def load_catalog(c: Connection, version_id: int) -> CatalogSnapshot:
    body = c.execute(select(catalog_version.c.body).where(catalog_version.c.id == version_id)).scalar_one()
    return CatalogSnapshot.model_validate(body)


def table_infos(snap: CatalogSnapshot) -> dict[TableRef, TableInfo]:
    return {t.table: t for t in snap.tables}


def config_overrides(c: Connection, connection_id: str) -> dict[str, dict[str, Any]]:
    """Console edits (table_config/column_config rows that carry a revision) as policy overrides."""
    out: dict[str, dict[str, Any]] = {}
    rows = c.execute(
        select(source_table.c.src_schema, source_table.c.src_table, table_config.c.enabled, table_config.c.revision)
        .join(table_config, table_config.c.table_id == source_table.c.id)
        .where(source_table.c.connection_id == connection_id, table_config.c.revision.is_not(None))).all()
    for s, t, en, _ in rows:
        out.setdefault(f"{s}.{t}", {})["enabled"] = en
    cols = c.execute(
        select(source_table.c.src_schema, source_table.c.src_table, source_column.c.name,
               column_config.c.enabled, column_config.c.hashed)
        .join(source_column, source_column.c.table_id == source_table.c.id)
        .join(column_config, column_config.c.column_id == source_column.c.id)
        .where(source_table.c.connection_id == connection_id, column_config.c.revision.is_not(None))).all()
    for s, t, n, en, h in cols:
        out.setdefault(f"{s}.{t}", {}).setdefault("columns", {})[n] = {"enabled": en, "hashed": h}
    return out


def pipeline_doc(c: Connection, connection_id: str) -> dict[str, Any] | None:
    """`pipeline_definition` row of a wizard/import pipeline (REQ-14), or None."""
    from .db import pipeline_definition

    r = c.execute(select(pipeline_definition).where(pipeline_definition.c.connection_id == connection_id)).mappings().first()
    return dict(r) if r else None


def connection_policy(c: Connection, connection_id: str, tables_config: str | None = None) -> Any:
    """Loader policy of one connection from its active config revision (F-18: the metadata DB is the single source
    of truth; console, wizard, importer, worker and diagnosis all read it here).

    Fallback only while a connection has no revision yet (before the first start seeded it): the 0.1 resolution
    (pipeline tables_doc, else the ALELT_TABLES_CONFIG document, plus console overrides). A file never overrides an
    existing revision - differences are reported as drift (config_store.drift)."""
    from alelt_loader import load_policy

    from .config_store import active_doc

    doc = active_doc(c, connection_id)
    if doc is not None:
        return load_policy(connection_id, None, None, doc=doc)
    d = pipeline_doc(c, connection_id)
    doc = (d or {}).get("tables_doc") or None
    return load_policy(connection_id, tables_config, config_overrides(c, connection_id), doc=doc)


# ------------------------------------------------------------------ commands (R5)
def issue_command(c: Connection, agent_id: str, name: CommandName, args: dict[str, Any], actor: str,
                  role: str | None = None, reason: str | None = None) -> Command:
    cmd = Command(command_id=uuid.uuid4(), name=name, args=args, issued_at=utcnow())  # validates whitelist
    c.execute(insert(agent_command).values(id=cmd.command_id, agent_id=agent_id, name=cmd.name.value,
                                           args=cmd.args, issued_by=actor, issued_at=cmd.issued_at))
    audit(c, actor, f"agent.command.{cmd.name.value}", agent_id,
          {"command_id": str(cmd.command_id), "args": args, "reason": reason}, role)
    return cmd


COMMAND_REDELIVER_S = float(os.environ.get("ALELT_COMMAND_REDELIVER_S", "120"))
COMMAND_MAX_DELIVERIES = int(os.environ.get("ALELT_COMMAND_MAX_DELIVERIES", "3"))


def pending_commands(c: Connection, agent_id: str) -> list[Command]:
    """Fetch the commands to deliver to an agent and mark them SENT: PENDING ones, and SENT ones not acknowledged
    COMMAND_REDELIVER_S after their last delivery (HC-12: a lost long-poll response / WebSocket frame no longer leaves
    a command SENT forever; agents treat a repeated command_id as the same command). After COMMAND_MAX_DELIVERIES
    unacknowledged deliveries the command is EXPIRED (audited) instead of re-sent."""
    now = utcnow()
    stale = now - timedelta(seconds=COMMAND_REDELIVER_S)
    unacked = (agent_command.c.status == "SENT") & (agent_command.c.sent_at.is_not(None)) &         (agent_command.c.sent_at < stale)
    gone = c.execute(update(agent_command).where(agent_command.c.agent_id == agent_id, unacked,
                                                 func.coalesce(agent_command.c.deliveries, 1) >= COMMAND_MAX_DELIVERIES)
                     .values(status="EXPIRED", acked_at=now).returning(agent_command.c.id, agent_command.c.name)).all()
    for cid_, name in gone:
        audit(c, "hub", "command.expired", str(cid_), {"agent_id": agent_id, "name": name,
                                                       "deliveries": COMMAND_MAX_DELIVERIES})
    rows = c.execute(select(agent_command).where(agent_command.c.agent_id == agent_id,
                                                 (agent_command.c.status == "PENDING") | unacked)
                     .order_by(agent_command.c.issued_at).with_for_update(skip_locked=True)).mappings().all()
    if rows:
        c.execute(update(agent_command).where(agent_command.c.id.in_([r["id"] for r in rows])).values(
            status="SENT", sent_at=now, deliveries=func.coalesce(agent_command.c.deliveries, 0) + 1))
    return [Command(command_id=r["id"], name=CommandName(r["name"]), args=r["args"] or {}, issued_at=r["issued_at"])
            for r in rows]


def ack_command(c: Connection, agent_id: str, command_id: uuid.UUID, ok: bool, result: dict[str, Any] | None) -> bool:
    r = c.execute(update(agent_command).where(agent_command.c.id == command_id, agent_command.c.agent_id == agent_id)
                  .values(status="ACKED" if ok else "FAILED", result=result, acked_at=utcnow()))
    if r.rowcount:
        audit(c, f"agent:{agent_id}", "command.ack", str(command_id), {"ok": ok, "result": result})
        touch_agent(c, agent_id)
    return bool(r.rowcount)


def touch_agent(c: Connection, agent_id: str, min_gap_s: float = 5.0) -> None:
    """Agent liveness (RC6): a batch POST, a snapshot chunk, a command poll or a command ack is a sign of life like a
    heartbeat, so an agent busy with a long command is never declared stale (ALELT_VERIFY_AGENT_STALE_S). At most one
    write per `min_gap_s` per agent."""
    from .db import agent

    now = utcnow()
    c.execute(update(agent).where(agent.c.id == agent_id, (agent.c.last_heartbeat.is_(None)) |
                                  (agent.c.last_heartbeat < now - timedelta(seconds=min_gap_s)))
              .values(last_heartbeat=now))

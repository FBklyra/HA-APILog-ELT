"""Edge-side verification through the whitelisted VERIFY_TABLE command (F-03, X-123, R4/R5).

The hub never connects to an edge source: it issues VERIFY_TABLE{table, ranges, checks, ...} to the connection's
agent (store.issue_command validates the args against alelt_common.models.VerifyTableArgs) and reads the result from
the command ack (agent_command.result = VerifyTableResult). Consumers:

- `verify_edge_table`: the alelt_verify edge suite (alelt_verify.edge) for recon runs and the mandatory post-snapshot
  reconciliation (lifecycle.caught_up queues it; MATCH sets the lifecycle `verified_at`);
- `EdgeHooks`: integrity recovery hooks (alelt_hub.integrity): `verify_table` after a recovery;
- `issue` / `poll`: the transport of the out-of-row completion fetcher (alelt_hub.completion.EdgeFetcher:
  VERIFY_TABLE{checks:[rows], key_values, columns}, rows read by primary key WITH CS on the edge).

Bounded waits: a command is answered within `ALELT_VERIFY_EDGE_TIMEOUT_S` (default 120 s; per call `timeout_s`) or it
is EXPIRED and `AgentUnavailable` is raised; an agent without a heartbeat / registration in the last
`ALELT_VERIFY_AGENT_STALE_S` (default 180 s) or without the `verify_table` capability fails fast before anything is
issued. `ops.consume_verify` records that as an ERROR (never a PASS), opens an AGENT_OFFLINE incident and retries later.

Pacing: one command at a time per verification, results bounded by the edge caps (ranges <= 256, keys <= 10 000,
rows <= 1 000 per command, 8 MiB); the agent waits while its governor pauses. Off-peak window: recon runs on edge
tables wait for the table's snapshot window (tables.yaml `snapshot_window`, or `verify_window`) unless forced.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.models import CommandName, Position, TableRef, VerifyTableResult
from alelt_common.util import utcnow

from . import holds, jobs, store
from .db import agent, agent_command, batch, source_table

log = logging.getLogger("alelt_hub.edge_verify")
ACTOR = "system:verify"


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


def is_edge(pipeline: Any) -> bool:
    return getattr(pipeline.source, "capture_mode", "edge") != "remote"


class AgentUnavailable(RuntimeError):
    """The edge did not (or cannot) answer VERIFY_TABLE: offline, too old, or no reply in time."""

    def __init__(self, msg: str, agent_id: str | None = None) -> None:
        super().__init__(msg)
        self.agent_id = agent_id


def timeout_default() -> float:
    return float(os.environ.get("ALELT_VERIFY_EDGE_TIMEOUT_S", "120") or 120)


def _stale_s() -> float:
    return float(os.environ.get("ALELT_VERIFY_AGENT_STALE_S", "180") or 180)


def agent_ready(engine: Engine, cid: str) -> str:
    """Agent id that can answer VERIFY_TABLE now, else AgentUnavailable (fail fast)."""
    from datetime import timedelta

    with engine.connect() as c:
        r = c.execute(select(agent.c.id, agent.c.last_heartbeat, agent.c.registered_at, agent.c.capabilities)
                      .where(agent.c.connection_id == cid, ~agent.c.id.like("capture:%"))
                      .order_by(agent.c.last_heartbeat.desc()).limit(1)).first()
    if r is None:
        raise AgentUnavailable(f"{cid}: no registered agent for VERIFY_TABLE")
    aid, hb, reg, caps = str(r[0]), r[1], r[2], r[3] or []
    if caps and "verify_table" not in caps:
        raise AgentUnavailable(f"agent {aid} does not support VERIFY_TABLE (capabilities {caps}): upgrade it", aid)
    seen = max([t if t.tzinfo else t.replace(tzinfo=utcnow().tzinfo) for t in (hb, reg) if t is not None],
               default=None)
    if seen is None or utcnow() - seen > timedelta(seconds=_stale_s()):
        raise AgentUnavailable(f"agent {aid} offline (last seen {seen.isoformat() if seen else 'never'})", aid)
    return aid


def expire(engine: Engine, command_id: str) -> None:
    """An unanswered command is EXPIRED (a late ack still lands, it is just no longer awaited)."""
    from sqlalchemy import update

    with engine.begin() as c:
        c.execute(update(agent_command).where(agent_command.c.id == uuid.UUID(command_id),
                                              agent_command.c.status.in_(("PENDING", "SENT")))
                  .values(status="EXPIRED", acked_at=utcnow()))


def edge_agent(engine: Engine, cid: str) -> str | None:
    with engine.connect() as c:
        r = c.execute(select(agent.c.id).where(agent.c.connection_id == cid, ~agent.c.id.like("capture:%"))
                      .order_by(agent.c.last_heartbeat.desc()).limit(1)).first()
    return str(r[0]) if r else None


# ------------------------------------------------------------------ transport
def issue(engine: Engine, cid: str, args: dict[str, Any], reason: str) -> str:
    aid = agent_ready(engine, cid)
    with engine.begin() as c:
        cmd = store.issue_command(c, aid, CommandName.VERIFY_TABLE, args, ACTOR, reason=reason)
    return str(cmd.command_id)


def poll(engine: Engine, command_id: str) -> tuple[str, dict[str, Any] | None]:
    """('pending' | 'ok' | 'failed', result)."""
    with engine.connect() as c:
        r = c.execute(select(agent_command.c.status, agent_command.c.result)
                      .where(agent_command.c.id == uuid.UUID(command_id))).first()
    if r is None:
        return "failed", {"message": "command vanished"}
    if r[0] == "ACKED":
        return "ok", r[1] or {}
    if r[0] == "FAILED":
        return "failed", r[1] or {}
    return "pending", None


def _message(res: dict[str, Any] | None) -> str:
    res = res or {}
    return str(res.get("message") or res.get("error") or res)[:500]


def caller(engine: Engine, cid: str, reason: str, timeout_s: float | None = None) -> Any:
    """Blocking transport for alelt_verify.edge: issue, wait for the ack (poll, bounded), validate the result shape."""
    wait = float(timeout_s if timeout_s is not None else timeout_default())

    def call(args: dict[str, Any]) -> dict[str, Any]:
        cmd = issue(engine, cid, args, reason)
        deadline = time.monotonic() + wait
        while True:
            st, res = poll(engine, cmd)
            if st == "ok":
                return VerifyTableResult.model_validate(res).model_dump(by_alias=True, mode="json")
            if st == "failed":
                raise RuntimeError(f"agent refused VERIFY_TABLE: {_message(res)}")
            if time.monotonic() >= deadline:
                expire(engine, cmd)
                raise AgentUnavailable(f"VERIFY_TABLE {cmd}: no answer from the agent within {wait:.0f} s "
                                       "(command expired)")
            time.sleep(min(0.5, max(0.05, wait / 20)))
    return call


# ------------------------------------------------------------------ settled window
def applied_next_pos(engine: Engine, pipeline: Any) -> str | None:
    """next position of the last batch the main stream made durable (everything before it is applied)."""
    from . import replay

    seq, _ = replay.mirror(pipeline)
    if not seq:
        return None
    with engine.connect() as c:
        r = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == pipeline.cid,
                                                                     batch.c.seq == seq)).first()
    return (r[0] or r[1]) if r else None


def settled(engine: Engine, pipeline: Any, fqn: str, position_before: str | None) -> bool:
    """The destination holds every change committed before the edge read started (and the table is not held)."""
    if not position_before:
        return False
    for kind in ("integrity", "schedule", "snapshot"):
        h = holds.get(engine, pipeline.cid, fqn, kind)
        if h and h.get("held"):
            return False
    from .completion import blocking as completion_blocking

    if completion_blocking(engine, pipeline.cid, fqn):     # out-of-row values still being completed (ADR-013)
        return False
    nxt = applied_next_pos(engine, pipeline)
    if not nxt:
        return False
    eng = pipeline.engine_name
    try:
        return pipeline.source.compare(Position(engine=eng, text=nxt), Position(engine=eng, text=position_before)) >= 0
    except Exception:  # noqa: BLE001 - unparseable position: not settled
        return False


def in_window(pipeline: Any, now: datetime | None = None) -> tuple[bool, float]:
    """(inside the verify window, seconds until it opens)."""
    from alelt_snapshot import OffPeakWindow

    w = getattr(pipeline.policy, "verify_window", None) or pipeline.policy.snapshot_window
    if not w:
        return True, 0.0
    win = OffPeakWindow.parse(str(w["start"]), str(w["end"]), str(w.get("tz") or "UTC"))
    now = now or utcnow()
    return (True, 0.0) if win.contains(now) else (False, max(5.0, (win.next_open(now) - now).total_seconds()))


# ------------------------------------------------------------------ suite
def _table_info(engine: Engine, pipeline: Any, fqn: str) -> Any:
    with engine.connect() as c:
        vid = store.latest_catalog_id(c, pipeline.cid)
        cat = store.load_catalog(c, vid) if vid else None
    info = store.table_infos(cat).get(_ref(fqn)) if cat else None
    if info is None:
        raise LookupError(f"{fqn} not in catalog")
    return info


def verify_edge_table(engine: Engine, pipeline: Any, fqn: str, check_kind: str = "COLUMN_CHECKSUM",
                      call: Any = None, timeout_s: float | None = None) -> Any:
    """Edge source (VERIFY_TABLE on the agent) vs destination. Returns a alelt_verify SuiteResult; summary carries
    `settled` (destination caught up to the edge read) so callers can retry an unsettled mismatch later."""
    from alelt_verify import ListSide
    from alelt_verify.edge import factory, run_edge_suite

    ref = _ref(fqn)
    info = _table_info(engine, pipeline, fqn)
    pol = pipeline.policy
    dinfo = pol.project(info)
    tp = pol.table(ref)
    hashed = {n for n, cp in (getattr(tp, "columns", None) or {}).items() if getattr(cp, "hashed", False)}
    cols = [c.name for c in sorted(dinfo.columns, key=lambda c: c.ordinal) if c.name not in hashed]
    keys = [c.name for c in info.key_columns()]
    types = {c.name: c.canonical_type for c in info.columns}
    fetch = getattr(pipeline.dest, "fetch_rows", None)
    if fetch is None:
        raise RuntimeError(f"destination {type(pipeline.dest).__name__} has no row access for verification")
    cand = ListSide(lambda _t: fetch(pol.dest_name(ref)), deleted=pol.system.deleted).rows(fqn)
    mk = factory(fqn)
    if call is None:
        agent_ready(engine, pipeline.cid)                      # fail fast before reading the destination
    out = run_edge_suite(mk, call or caller(engine, pipeline.cid, f"verify {fqn} ({check_kind})", timeout_s), ref, cand,
                         keys, cols, types,
                         range_rows=int(os.environ.get("ALELT_VERIFY_EDGE_RANGE_ROWS", "10000")),
                         sample=int(os.environ.get("ALELT_VERIFY_EDGE_SAMPLE", "50")))
    out.summary["settled"] = settled(engine, pipeline, fqn, out.summary.get("position_before"))
    out.summary["hashed_columns_excluded"] = sorted(hashed)
    return out


def agent_unavailable(engine: Engine, cid: str, fqn: str, err: AgentUnavailable, attempt: int) -> None:
    """Clear state when the edge cannot answer: AGENT_OFFLINE incident (stage verify) + the reason on the table's
    snapshot hold (post-snapshot reconciliation pending). Never raises."""
    try:
        holds.set_body(engine, cid, fqn, "snapshot", verify=f"edge verification waiting for the agent: {err}"[:500],
                       verify_attempt=attempt)
    except Exception:  # noqa: BLE001
        pass
    try:
        from alelt_diagnose.incidents import open_incident
        from alelt_diagnose.models import IncidentKind, Trigger

        open_incident(t=Trigger(kind=IncidentKind.AGENT_OFFLINE, connection_id=cid, table_fqn=fqn,
                                agent_id=err.agent_id, stage="verify", error=str(err)[:500],
                                detail={"attempt": attempt, "command": "VERIFY_TABLE"}), engine=engine)
    except Exception:  # noqa: BLE001 - diagnosis never breaks the verify path
        log.exception("verify incident failed", extra={"component": "alelt_hub"})


def request_post_snapshot_verify(engine: Engine, cid: str, fqn: str) -> None:
    """Mandatory post-snapshot reconciliation of an edge table (X-123): a recon run of the table; its MATCH moves the
    snapshot job to IN_SYNC (ops.consume_verify -> SnapshotService.reconciled)."""
    from .db import recon_run
    from .dialect import insert

    s, _, n = fqn.partition(".")
    with engine.begin() as c:
        tid = c.execute(select(source_table.c.id).where(source_table.c.connection_id == cid,
                                                        source_table.c.src_schema == s,
                                                        source_table.c.src_table == n)).scalar()
        if tid is None:
            log.warning("post-snapshot verify: %s not in source_table", fqn, extra={"component": "alelt_hub"})
            return
        rid = uuid.uuid4()
        c.execute(insert(recon_run).values(id=rid, check_kind="POST_SNAPSHOT"))
        jobs.enqueue(c, "verify", {"run_id": str(rid), "table_ids": [str(tid)], "check_kind": "POST_SNAPSHOT",
                                   "force": True}, dedup_key=f"postsnap:{cid}:{fqn}:{rid}")


# ------------------------------------------------------------------ integrity hooks (alelt_hub.integrity)
def _hooks_base() -> Any:
    from .integrity import RecoveryHooks

    return RecoveryHooks


class EdgeHooks(_hooks_base()):  # type: ignore[misc]
    """Integrity recovery hooks for edge streams; remote streams fall back to the defaults. Engine streams (e.g. the
    Db2 decoder stream) subclass this and override refresh_catalog etc."""

    def verify_table(self, engine: Engine, pipeline: Any, fqn: str) -> str | None:
        if not is_edge(pipeline):
            return super().verify_table(engine, pipeline, fqn)
        out = verify_edge_table(engine, pipeline, fqn, "INTEGRITY")
        if out.outcome == "MISMATCH" and not out.summary.get("settled"):
            return "pending"                                   # destination behind the edge read: look again later
        return str(out.outcome)


EDGE_ENGINES = ("db2", "postgres", "mysql", "mariadb", "sqlserver", "oracle")


def register() -> None:
    """Edge hooks for every engine that can run behind an agent, unless an engine stream registered its own."""
    from . import integrity

    for e in EDGE_ENGINES:
        if e not in integrity._HOOKS:
            integrity.register_hooks(e, EdgeHooks())


register()

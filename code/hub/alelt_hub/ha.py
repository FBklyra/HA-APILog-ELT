"""High availability, hub side (X-47, X-49).

Capture lease (agent active/passive): exactly one agent per connection holds the lease and may ship batches.
  POST /agent/v1/lease/acquire   free / expired / own lease -> granted; holder change => epoch + 1, takeover=true
  POST /agent/v1/lease/renew     holder + lease_id + epoch must match AND the lease must be live (not expired, not
                                 released), else 409 lease_lost - a released lease is never revived (HA-01)
  POST /agent/v1/lease/release   clears the holder now (clean shutdown / planned switchover / operator failover);
                                 the next grant always carries epoch + 1. For one TTL the released agent may not
                                 re-acquire while another candidate is polling, so a forced failover goes to the standby.
  GET  /agent/v1/lease           current state for a connection
Fencing: when a connection has an active/passive lease row, `POST /agent/v1/blocks` must come from the holder with
`X-ALELT-Lease-Epoch` = current epoch (checked inside the batch transaction, which locks the same row) -> else 423.
`/catalog` and `/snapshot` from a known agent that is not the live holder -> 423 as well (HA-04); a non-holder
heartbeat that claims CAPTURING is recorded as STANDBY. The duplicate check of `/blocks` runs before the fence, so a
stale holder re-sending an already stored batch gets the (harmless) duplicate ACK (HA-27).
A connection without a lease row, or whose row was cleared (`mode = standalone`, HA-14), is standalone; the header
is ignored. Clearing keeps the row so epochs stay monotonic for the life of the connection. Re-acquiring after the
own lease expired, with no other holder in between, keeps the epoch (HA-13: nobody else could have written).

Clock (HA-12): lease times come from the metadata database clock on PostgreSQL (`now()`: one clock for every API
replica, replica skew cannot cause a takeover). SQLite metadata is single-host (one clock). Agents self-fence with
their monotonic clock only; NTP is still required on hub hosts for audit times (deploy/HA.md).

Hub nodes: every api / worker process upserts a `hub_node` row every 15 s (console *Topology & HA*); rows not seen
for 24 h are deleted (HA-20).
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import Connection, Engine, delete, select, text, update

from alelt_common.models import Position
from alelt_common.util import utcnow

from . import __version__, store
from .auth import agent_identity, require_binding
from .db import agent, agent_command, batch, connection, get_engine
from .dialect import insert
from .ha_models import LeaseGrant, LeaseRequest, capture_lease, hub_node
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub.ha")
router = APIRouter(prefix="/agent/v1/lease", tags=["agent", "ha"])

NODE_BEAT_S = 15.0
NODE_ALIVE_S = 45.0
NODE_GC_S = 24 * 3600.0
STANDALONE = "standalone"


def default_ttl() -> int:
    return max(5, min(300, int(os.environ.get("ALELT_LEASE_TTL_S", "30"))))


def db_now(c: Connection) -> datetime:
    """Lease clock (HA-12): the PostgreSQL server clock (same for every replica), local UTC on SQLite."""
    if c.dialect.name == "postgresql":
        v = c.execute(text("SELECT now()")).scalar()
        if isinstance(v, datetime) and v.tzinfo is not None:
            return v
    return utcnow()


# ------------------------------------------------------------------ lease core (also used by tests / topology)
def _resume_from(c: Connection, cid: str) -> Position | None:
    last = c.execute(select(batch.c.next_lri, batch.c.to_lri, batch.c.engine).where(batch.c.connection_id == cid)
                     .order_by(batch.c.seq.desc()).limit(1)).first()
    return Position(engine=last[2], text=last[0] or last[1]) if last else None


CAPTURE_CONTROL = ("START_CAPTURE", "RESUME", "PAUSE")


def capture_running(c: Connection, cid: str) -> bool | None:
    """AHS-24: is capture of this connection meant to run? Decided by the newest capture-control command issued to
    any agent of the connection (not FAILED): PAUSE -> False, START_CAPTURE / RESUME -> True, none -> None. The new
    holder of a takeover never received a PAUSE sent to the previous holder, so the grant carries it."""
    row = c.execute(select(agent_command.c.name).select_from(agent_command.join(agent, agent.c.id ==
                                                                                agent_command.c.agent_id))
                    .where(agent.c.connection_id == cid, agent_command.c.name.in_(CAPTURE_CONTROL),
                           agent_command.c.status != "FAILED")
                    .order_by(agent_command.c.issued_at.desc()).limit(1)).first()
    return None if row is None else row[0] != "PAUSE"


def _lock_row(c: Connection, cid: str) -> dict[str, Any]:
    c.execute(insert(capture_lease).values(connection_id=cid, epoch=0, ttl_s=default_ttl())
              .on_conflict_do_nothing(index_elements=["connection_id"]))
    return dict(c.execute(select(capture_lease).where(capture_lease.c.connection_id == cid)
                          .with_for_update()).mappings().one())


def _alive(row: Any, now: datetime) -> bool:
    return bool(row["holder"]) and row["expires_at"] is not None and row["expires_at"] > now


def _grant(row: dict[str, Any], granted: bool, takeover: bool = False, resume: Position | None = None,
           retry: float = 0.0) -> LeaseGrant:
    return LeaseGrant(granted=granted, connection_id=row["connection_id"], holder=row["holder"],
                      lease_id=row["lease_id"], epoch=int(row["epoch"] or 0), ttl_s=int(row["ttl_s"] or 0),
                      expires_at=row["expires_at"], takeover=takeover, resume_from=resume, retry_after_s=retry)


def _check_agent(c: Connection, req: LeaseRequest) -> None:
    cid = c.execute(select(agent.c.connection_id).where(agent.c.id == req.agent_id)).scalar()
    if cid is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not registered")
    if cid != req.connection_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "agent not registered for this connection")


def _ts(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except (TypeError, ValueError):
        return None


def _other_candidate(cand: dict[str, Any], me: str, now: datetime, ttl: int) -> bool:
    for a, at in cand.items():
        t = _ts(at)
        if a != me and t is not None and (now - t).total_seconds() <= ttl:
            return True
    return False


def acquire(c: Connection, req: LeaseRequest) -> LeaseGrant:
    row = _lock_row(c, req.connection_id)
    now = db_now(c)
    ttl = req.ttl_s or default_ttl()
    old_cand = dict(row["candidates"] or {})
    cand = dict(old_cand)
    seen = _ts(cand.get(req.agent_id))
    if seen is None or (now - seen).total_seconds() >= max(1.0, ttl / 3):   # HA-15: no row write on every poll
        cand[req.agent_id] = now.isoformat()
    deny_retry = None
    if _alive(row, now) and row["holder"] != req.agent_id:
        left = (row["expires_at"] - now).total_seconds()
        deny_retry = max(1.0, min(float(ttl) / 3, left))
    elif (not row["holder"] and row["last_holder"] == req.agent_id and row["released_at"] is not None
          and (now - row["released_at"]).total_seconds() < ttl and _other_candidate(cand, req.agent_id, now, ttl)):
        deny_retry = max(1.0, float(ttl) / 3)                    # HA-01: a released holder yields to a standby
    if deny_retry is not None:
        if cand != old_cand:
            c.execute(update(capture_lease).where(capture_lease.c.connection_id == req.connection_id)
                      .values(candidates=cand))
        return _grant(row, False, retry=deny_retry)
    prev = row["holder"] or row["last_holder"]
    takeover = bool(prev) and prev != req.agent_id
    new = row["holder"] != req.agent_id or row["lease_id"] is None
    vals: dict[str, Any] = dict(holder=req.agent_id, ttl_s=ttl, renewed_at=now, expires_at=now + timedelta(seconds=ttl),
                                candidates=cand, mode=None)
    if new:
        vals.update(epoch=int(row["epoch"] or 0) + 1, lease_id=uuid.uuid4(), acquired_at=now, last_holder=None,
                    released_at=None)
    c.execute(update(capture_lease).where(capture_lease.c.connection_id == req.connection_id).values(**vals))
    row.update(vals)
    if new:
        store.audit(c, f"agent:{req.agent_id}", "agent.lease.takeover" if takeover else "agent.lease.acquire",
                    req.connection_id, {"epoch": row["epoch"], "previous_holder": prev})
        if takeover:
            REGISTRY.inc("alelt_lease_takeovers_total", connection_id=req.connection_id)
        log.info("lease granted", extra={"component": "alelt_hub", "correlation_id": f"{req.connection_id}:{row['epoch']}"})
    g = _grant(row, True, takeover=takeover, resume=_resume_from(c, req.connection_id))
    g.capturing = capture_running(c, req.connection_id)
    return g


def renew(c: Connection, req: LeaseRequest) -> LeaseGrant:
    got = c.execute(select(capture_lease).where(capture_lease.c.connection_id == req.connection_id)
                    .with_for_update()).mappings().first()
    now = db_now(c)
    row = dict(got) if got else {"holder": None, "epoch": 0, "lease_id": None, "expires_at": None, "mode": None}
    lost = (got is None or row["mode"] == STANDALONE or row["holder"] != req.agent_id
            or (req.epoch is not None and int(row["epoch"]) != req.epoch)
            or (req.lease_id is not None and row["lease_id"] != req.lease_id)
            or not _alive(row, now))                      # HA-01: expired / released leases are never revived
    if lost:
        log.warning("lease renew refused (lost)", extra={"component": "alelt_hub", "correlation_id":
                    f"{req.connection_id}:{req.agent_id}:{req.epoch}->{row['holder']}:{row['epoch']}"})
        raise HTTPException(status.HTTP_409_CONFLICT, {"error": "lease_lost", "holder": row["holder"],
                                                        "epoch": int(row["epoch"] or 0)})
    ttl = req.ttl_s or int(row["ttl_s"] or default_ttl())
    vals = dict(ttl_s=ttl, renewed_at=now, expires_at=now + timedelta(seconds=ttl))
    c.execute(update(capture_lease).where(capture_lease.c.connection_id == req.connection_id).values(**vals))
    row.update(vals)
    return _grant(row, True)


def release(c: Connection, connection_id: str, agent_id: str | None, actor: str, reason: str = "") -> dict[str, Any]:
    """Clear the lease now. agent_id=None: operator forced failover (any holder). The holder's next renew gets 409
    and the next grant carries epoch + 1 (HA-01)."""
    row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)
                    .with_for_update()).mappings().first()
    if row is None or row["mode"] == STANDALONE or not row["holder"] or (
            agent_id is not None and row["holder"] != agent_id):
        return {"released": False, "holder": row["holder"] if row else None, "epoch": int(row["epoch"]) if row else 0}
    now = db_now(c)
    c.execute(update(capture_lease).where(capture_lease.c.connection_id == connection_id).values(
        holder=None, lease_id=None, expires_at=now, last_holder=row["holder"], released_at=now))
    store.audit(c, actor, "agent.lease.release", connection_id,
                {"holder": row["holder"], "epoch": row["epoch"], **({"reason": reason} if reason else {})})
    return {"released": True, "holder": row["holder"], "epoch": int(row["epoch"])}


def check_fence(c: Connection, connection_id: str, agent_id: str, epoch_header: str | None) -> None:
    """Raise 423 unless the batch may be accepted under the connection's lease (call inside the batch txn)."""
    row = c.execute(select(capture_lease.c.holder, capture_lease.c.epoch, capture_lease.c.mode)
                    .where(capture_lease.c.connection_id == connection_id).with_for_update()).first()
    if row is None or row[2] == STANDALONE:
        return                                   # standalone connection
    try:
        got = int(epoch_header) if epoch_header not in (None, "") else None
    except ValueError:
        got = None
    if row[0] == agent_id and got == int(row[1]):
        return
    REGISTRY.inc("alelt_batches_received_total", connection_id=connection_id, status="lease_fenced")
    raise HTTPException(status.HTTP_423_LOCKED, {"error": "lease_fenced", "holder": row[0], "epoch": int(row[1]),
                                                 "got_epoch": got})


def fence_precheck(engine: Engine, connection_id: str, agent_id: str, epoch_header: str | None) -> None:
    """Cheap check before writing the body to the store (the authoritative one runs in the batch txn)."""
    with engine.connect() as c:
        row = c.execute(select(capture_lease.c.holder, capture_lease.c.epoch, capture_lease.c.mode)
                        .where(capture_lease.c.connection_id == connection_id)).first()
    if row is not None and row[2] != STANDALONE and not (
            row[0] == agent_id and str(row[1]) == (epoch_header or "").strip()):
        REGISTRY.inc("alelt_batches_received_total", connection_id=connection_id, status="lease_fenced")
        raise HTTPException(status.HTTP_423_LOCKED, {"error": "lease_fenced", "holder": row[0], "epoch": int(row[1])})


def check_holder(c: Connection, connection_id: str, agent_id: str | None, epoch_header: str | None = None,
                 what: str = "request") -> None:
    """HA-04: /catalog and /snapshot from a known agent must come from the live holder (423 otherwise). Unknown
    caller (dev static token without an agent id) or standalone connection: allowed. Epoch checked when sent."""
    if not agent_id:
        return
    row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)).mappings().first()
    if row is None or row["mode"] == STANDALONE:
        return
    ok = row["holder"] == agent_id and _alive(row, db_now(c))
    if ok and epoch_header not in (None, ""):
        ok = str(row["epoch"]) == str(epoch_header).strip()
    if not ok:
        REGISTRY.inc("alelt_lease_fenced_total", connection_id=connection_id, what=what)
        raise HTTPException(status.HTTP_423_LOCKED, {"error": "lease_fenced", "holder": row["holder"],
                                                     "epoch": int(row["epoch"] or 0), "what": what})


def is_standby(c: Connection, connection_id: str | None, agent_id: str) -> bool:
    """True when the connection runs active/passive and `agent_id` is not the live holder (heartbeat, HA-04)."""
    if not connection_id:
        return False
    row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)).mappings().first()
    if row is None or row["mode"] == STANDALONE:
        return False
    return not (row["holder"] == agent_id and _alive(row, db_now(c)))


def holder(c: Connection, connection_id: str) -> str | None:
    """Live lease holder of an active/passive connection, else None (HA-05)."""
    row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)).mappings().first()
    if row is None or row["mode"] == STANDALONE or not _alive(row, db_now(c)):
        return None
    return str(row["holder"])


def clear_lease(c: Connection, connection_id: str, actor: str) -> bool:
    """Back to standalone capture for a connection (fencing off). Refused while a lease is live. The row is kept
    (holder cleared, mode=standalone) so the epoch stays monotonic (HA-14)."""
    row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)
                    .with_for_update()).mappings().first()
    if row is None or row["mode"] == STANDALONE:
        return False
    if _alive(row, db_now(c)):
        raise RuntimeError(f"lease of {connection_id} is held by {row['holder']} until {row['expires_at']}; "
                           "set the agents to ha.mode=standalone (or release) first")
    c.execute(update(capture_lease).where(capture_lease.c.connection_id == connection_id).values(
        mode=STANDALONE, holder=None, lease_id=None, last_holder=None, released_at=None))
    store.audit(c, actor, "agent.lease.clear", connection_id, {"epoch": row["epoch"]})
    return True


# ------------------------------------------------------------------ topology write freeze (HA-02)
FREEZE_KEY = "topology_freeze"


def frozen(c: Connection) -> dict[str, Any] | None:
    """Active write freeze of this metadata database (topology switch between drain and resume), else None."""
    from .db import hub_setting

    v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == FREEZE_KEY)).scalar()
    return dict(v) if v else None


def set_freeze(c: Connection, op_id: str, reason: str) -> None:
    from .db import hub_setting

    val = {"op": op_id, "reason": reason, "since": utcnow().isoformat()}
    c.execute(insert(hub_setting).values(key=FREEZE_KEY, value=val).on_conflict_do_update(
        index_elements=["key"], set_={"value": val, "updated_at": utcnow()}))


def lift_freeze(c: Connection) -> bool:
    from .db import hub_setting

    return bool(c.execute(delete(hub_setting).where(hub_setting.c.key == FREEZE_KEY)).rowcount)


def check_frozen(c: Connection, what: str) -> None:
    """503 + Retry-After while frozen: agents keep the data in their spool and ship it to the new stack."""
    fz = frozen(c)
    if fz is not None:
        REGISTRY.inc("alelt_topology_frozen_rejects_total", what=what)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            {"error": "topology_frozen", "operation": fz.get("op"), "what": what},
                            headers={"Retry-After": "30"})


def lease_rows(c: Connection) -> dict[str, dict[str, Any]]:
    """Active/passive lease rows (cleared / standalone rows are left out)."""
    return {r["connection_id"]: dict(r) for r in c.execute(select(capture_lease)).mappings()
            if r["mode"] != STANDALONE}


# ------------------------------------------------------------------ routes
def _tx(fn: Any, req: LeaseRequest) -> LeaseGrant:
    with get_engine().begin() as c:
        _check_agent(c, req)
        return fn(c, req)  # type: ignore[no-any-return]


@router.post("/acquire", response_model=LeaseGrant)
def http_acquire(req: LeaseRequest, request: Request, _: str = Depends(agent_identity)) -> LeaseGrant:
    require_binding(request, req.agent_id, req.connection_id)
    return _tx(acquire, req)


@router.post("/renew", response_model=LeaseGrant)
def http_renew(req: LeaseRequest, request: Request, _: str = Depends(agent_identity)) -> LeaseGrant:
    require_binding(request, req.agent_id, req.connection_id)
    return _tx(renew, req)


@router.post("/release")
def http_release(req: LeaseRequest, request: Request, _: str = Depends(agent_identity)) -> dict[str, Any]:
    require_binding(request, req.agent_id, req.connection_id)
    with get_engine().begin() as c:
        _check_agent(c, req)
        return release(c, req.connection_id, req.agent_id, f"agent:{req.agent_id}")


@router.get("", response_model=LeaseGrant)
def http_status(request: Request, connection_id: str = Query(...), _: str = Depends(agent_identity)) -> LeaseGrant:
    require_binding(request, None, connection_id)
    with get_engine().connect() as c:
        if c.execute(select(connection.c.id).where(connection.c.id == connection_id)).scalar() is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connection")
        row = c.execute(select(capture_lease).where(capture_lease.c.connection_id == connection_id)).mappings().first()
        if row is None or row["mode"] == STANDALONE:
            return LeaseGrant(granted=False, connection_id=connection_id)
        r = dict(row)
        return _grant(r, _alive(r, db_now(c)))


# ------------------------------------------------------------------ hub nodes
def node_id(role: str) -> str:
    return os.environ.get("ALELT_NODE_ID") or f"{socket.gethostname()}:{os.getpid()}:{role}"


def node_beat(engine: Engine, role: str, nid: str | None = None, first: bool = False) -> None:
    """Upsert this node; `first` (process start) also resets started_at (fixed ALELT_NODE_ID, HA-20). Rows not seen
    for NODE_GC_S are deleted."""
    from .blobstore import spool_url

    now = utcnow()
    vals: dict[str, Any] = dict(role=role, host=socket.gethostname(), version=__version__, spool_url=spool_url(),
                                db_backend=engine.dialect.name, last_seen=now)
    upd = {**vals, "started_at": now} if first else vals
    with engine.begin() as c:
        c.execute(insert(hub_node).values(node_id=nid or node_id(role), started_at=now, **vals)
                  .on_conflict_do_update(index_elements=["node_id"], set_=upd))
        c.execute(delete(hub_node).where(hub_node.c.last_seen < now - timedelta(seconds=NODE_GC_S)))


def start_node_heartbeat(engine: Engine, role: str, nid: str | None = None) -> threading.Event:
    """Daemon thread: hub_node heartbeat every NODE_BEAT_S; set the returned event to stop."""
    stop = threading.Event()
    me = nid or node_id(role)

    def loop() -> None:
        first = True
        while not stop.is_set():
            try:
                node_beat(engine, role, me, first=first)
                first = False
            except Exception as e:  # noqa: BLE001 - metadata outage: keep trying
                log.debug("node heartbeat failed: %s", e)
            stop.wait(NODE_BEAT_S)

    threading.Thread(target=loop, name=f"alelt-node-{role}", daemon=True).start()
    return stop


def nodes(c: Connection, alive_s: float = NODE_ALIVE_S) -> list[dict[str, Any]]:
    now = utcnow()
    out = []
    for r in c.execute(select(hub_node).order_by(hub_node.c.role, hub_node.c.node_id)).mappings():
        age = (now - r["last_seen"]).total_seconds()
        if age > 24 * 3600:
            continue
        out.append({**dict(r), "alive": age <= alive_s})
    return out

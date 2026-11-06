"""Agent gateway, protocol v1 (DESIGN.md §9). Agents connect outbound only (R4); commands are whitelisted (R5)."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, select, update

from alelt_common.models import (
    AgentEvent,
    BatchAck,
    BatchHeader,
    CatalogSnapshot,
    Command,
    Heartbeat,
    Limits,
    Position,
    RegisterRequest,
    RegisterResponse,
    SnapshotChunkHeader,
)
from alelt_common.util import decompress, sha256_hex, utcnow

from . import blobstore, ha, jobs, store
from .auth import AgentIdentity, agent_identity, authenticate_agent, check_protocol, require_binding
from .db import agent, agent_command, batch, connection, get_engine, snapshot_chunk
from .dialect import insert
from .metrics import REGISTRY
from .settings import get_settings

log = logging.getLogger("alelt_hub.gateway")
router = APIRouter(prefix="/agent/v1", tags=["agent"])


class CommandAck(BaseModel):
    ok: bool = True
    result: dict[str, Any] | None = None


def _reject(reason: str, code: int, detail: Any, cid: str = "") -> HTTPException:
    REGISTRY.inc("alelt_batches_received_total", connection_id=cid, status=reason)
    return HTTPException(code, detail)


async def _body(request: Request) -> bytes:
    limit = get_settings().max_body_mb << 20
    if int(request.headers.get("content-length") or 0) > limit:
        raise _reject("too_large", status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "body too large")
    data = await request.body()
    if len(data) > limit:
        raise _reject("too_large", status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "body too large")
    return data


def _agent_conn(agent_id: str) -> str:
    with get_engine().connect() as c:
        cid = c.execute(select(agent.c.connection_id).where(agent.c.id == agent_id)).scalar()
    if cid is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not registered")
    return str(cid)


# ------------------------------------------------------------------ register / heartbeat
CREDENTIAL_CAPABILITY = "credential.v1"     # agent stores a per-agent credential returned at registration (F-14)


@router.post("/register", response_model=RegisterResponse)
def register(req: RegisterRequest, request: Request, ident: str = Depends(agent_identity)) -> RegisterResponse:
    who: AgentIdentity = request.state.alelt_agent
    require_binding(request, req.agent_id, req.connection_id, registering=True)
    cred: tuple[str, Any] | None = None
    with get_engine().begin() as c:
        cur = c.execute(select(agent.c.connection_id).where(agent.c.id == req.agent_id)).first()
        if cur is not None and cur[0] != req.connection_id:      # AHS-01: no silent re-homing of an agent
            raise HTTPException(status.HTTP_409_CONFLICT, f"agent {req.agent_id!r} is registered to connection "
                                f"{cur[0]!r}; remove the agent in the console before registering it elsewhere")
        c.execute(insert(connection).values(id=req.connection_id, engine=req.engine, db_database=req.database)
                  .on_conflict_do_nothing(index_elements=["id"]))
        vals = dict(connection_id=req.connection_id, version=req.version, hostname=req.hostname,
                    capabilities=req.capabilities, status="REGISTERED",
                    cert_thumbprint=ident if who.kind == "mtls" else None)
        c.execute(insert(agent).values(id=req.agent_id, **vals)
                  .on_conflict_do_update(index_elements=["id"], set_=vals))
        lim = c.execute(select(agent.c.limits).where(agent.c.id == req.agent_id)).scalar()
        last = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == req.connection_id)
                         .order_by(batch.c.seq.desc()).limit(1)).first()
        store.audit(c, f"agent:{req.agent_id}", "agent.register", req.connection_id,
                    {"version": req.version, "hostname": req.hostname, "auth": who.kind})
        if who.kind == "enrollment" and CREDENTIAL_CAPABILITY in req.capabilities:
            from .setup import issue_credential   # enrollment -> per-agent credential (shown once)

            cred = issue_credential(c, req.agent_id, req.connection_id, enrollment_hash=who.value)
    resume = Position(engine=req.engine, text=last[0] or last[1]) if last else None
    # XC-15: no limits stored for the agent = None, the agent keeps its local (DBA) limits
    return RegisterResponse(accepted=True, resume_from=resume, limits=Limits(**lim) if lim else None,
                            credential=cred[0] if cred else None, credential_expires_at=cred[1] if cred else None)


@router.post("/credential/rotate")
def rotate(request: Request, _: str = Depends(agent_identity)) -> dict[str, Any]:
    """Agent-initiated credential rotation: returns a new `alelta_` credential; the old one keeps working for
    ALELT_AGENT_CREDENTIAL_GRACE_S so an in-flight request is not lost."""
    who: AgentIdentity = request.state.alelt_agent
    if who.kind != "credential":
        raise HTTPException(status.HTTP_409_CONFLICT, "only a per-agent credential can be rotated")
    from .setup import rotate_credential

    token, exp = rotate_credential(who.value)
    return {"credential": token, "credential_expires_at": exp.isoformat()}


_METRIC_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,79}$")


def _metric_names(m: dict[str, float]) -> dict[str, float]:
    """REQ-11: agent-reported metrics (X-16 names); bounded, Prometheus-safe names only, finite values."""
    out: dict[str, float] = {}
    for k, v in list(m.items())[:64]:
        if _METRIC_NAME.match(k) and isinstance(v, (int, float)) and math.isfinite(v):
            out[k] = float(v)
    return out


@router.post("/heartbeat")
def heartbeat_http(hb: Heartbeat, request: Request, _: str = Depends(agent_identity)) -> dict[str, bool]:
    require_binding(request, hb.agent_id)
    return heartbeat(hb)


def heartbeat(hb: Heartbeat, _: str = "") -> dict[str, bool]:
    with get_engine().begin() as c:
        state = hb.state
        if state == "CAPTURING":                     # HA-04: a non-holder is never shown as a second active
            cid = c.execute(select(agent.c.connection_id).where(agent.c.id == hb.agent_id)).scalar()
            if ha.is_standby(c, cid, hb.agent_id):
                state = "STANDBY"
        r = c.execute(update(agent).where(agent.c.id == hb.agent_id).values(
            status=state, last_lri=hb.last_read_pos.text if hb.last_read_pos else agent.c.last_lri,
            durable_pos=hb.durable_pos.text if hb.durable_pos else agent.c.durable_pos, spool_bytes=hb.spool_bytes,
            metrics={**_metric_names(hb.metrics), "cpu_pct": hb.cpu_pct, "mem_mb": hb.mem_mb,
                     "batches_sent": hb.batches_sent, "errors": hb.errors}, last_heartbeat=utcnow()))
    if not r.rowcount:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not registered")
    return {"ok": True}


@router.post("/events")
def post_event(ev: AgentEvent, request: Request, _: str = Depends(agent_identity)) -> dict[str, bool]:
    """Agent events that are not command results (those use /commands/{id}/ack). Stored in the audit log."""
    require_binding(request, ev.agent_id)
    with get_engine().begin() as c:
        if c.execute(select(agent.c.id).where(agent.c.id == ev.agent_id)).scalar() is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not registered")
        store.audit(c, f"agent:{ev.agent_id}", f"agent.event.{ev.type}", ev.agent_id,
                    json.loads(ev.model_dump_json()))
    REGISTRY.inc("alelt_agent_events_total", type=ev.type)
    return {"ok": True}


# ------------------------------------------------------------------ catalog
def _caller_agent(request: Request, connection_id: str, header_agent: str | None) -> str | None:
    """Agent behind a request without an agent_id in the body (HA-04): X-ALELT-Agent-Id (bound), the credential's agent,
    or the single agent of the connection registered with this mTLS certificate."""
    if header_agent:
        require_binding(request, header_agent, connection_id)
        return header_agent
    ident: AgentIdentity | None = getattr(request.state, "alelt_agent", None)
    if ident is None:
        return None
    if ident.agent_id:
        return ident.agent_id
    if ident.kind == "mtls":
        with get_engine().connect() as c:
            ids = [r[0] for r in c.execute(select(agent.c.id).where(
                agent.c.connection_id == connection_id, agent.c.cert_thumbprint == ident.value).limit(2))]
        return str(ids[0]) if len(ids) == 1 else None
    return None


@router.post("/catalog")
def post_catalog(snap: CatalogSnapshot, request: Request, _: str = Depends(agent_identity),
                 x_alelt_agent_id: str | None = Header(default=None),
                 x_alelt_lease_epoch: str | None = Header(default=None)) -> dict[str, Any]:
    require_binding(request, None, snap.connection_id)
    caller = _caller_agent(request, snap.connection_id, x_alelt_agent_id)
    with get_engine().begin() as c:
        if c.execute(select(connection.c.id).where(connection.c.id == snap.connection_id)).scalar() is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown connection (register first)")
        ha.check_frozen(c, "catalog")                                       # HA-02
        ha.check_holder(c, snap.connection_id, caller, x_alelt_lease_epoch, "catalog")   # HA-04
        vid, created = store.ingest_catalog(c, snap)
    return {"version": vid, "created": created, "tables": len(snap.tables)}


# ------------------------------------------------------------------ raw blocks
@router.post("/blocks", response_model=BatchAck)
async def post_blocks(request: Request, x_alelt_batch: str = Header(...),
                      x_alelt_reposition: str | None = Header(default=None),
                      x_alelt_lease_epoch: str | None = Header(default=None),
                      _: str = Depends(agent_identity)) -> BatchAck:
    try:
        hdr = BatchHeader.model_validate_json(x_alelt_batch)
    except ValidationError as e:
        raise _reject("bad_header", status.HTTP_400_BAD_REQUEST, json.loads(e.json())) from None
    try:
        check_protocol(str(hdr.protocol))
    except HTTPException as e:
        raise _reject("protocol", status.HTTP_426_UPGRADE_REQUIRED, e.detail) from None
    require_binding(request, hdr.agent_id, hdr.connection_id)
    body = await _body(request)
    return await asyncio.to_thread(_receive_batch, hdr, body, x_alelt_reposition, x_alelt_lease_epoch)


FAULTS: dict[str, Any] = {}     # crash-point injection for tests only: {"after_put"|"after_commit": callable}


def _fault(point: str) -> None:
    fn = FAULTS.get(point)
    if fn is not None:
        fn()


def _receive_batch(hdr: BatchHeader, body: bytes, reposition: str | None, lease_epoch: str | None = None) -> BatchAck:
    s = get_settings()
    eng = get_engine()
    with eng.connect() as c:
        dup = c.execute(select(batch.c.to_lri, batch.c.sha256).where(batch.c.id == hdr.batch_id)).first()
        if dup is not None:
            if dup[1] != hdr.sha256:
                raise _reject("dup_mismatch", status.HTTP_409_CONFLICT, "batch_id reused with different content")
            REGISTRY.inc("alelt_batches_received_total", connection_id=hdr.connection_id, status="duplicate")
            return BatchAck(batch_id=hdr.batch_id, durable_pos=Position(engine=hdr.engine, text=dup[0]), duplicate=True)
        known = c.execute(select(agent.c.connection_id).where(agent.c.id == hdr.agent_id)).scalar()
        if known != hdr.connection_id:
            raise _reject("unknown_agent", status.HTTP_403_FORBIDDEN, "agent not registered for this connection")
        ha.check_frozen(c, "blocks")                                        # HA-02: switch in progress
        backlog = c.execute(select(func.count()).select_from(batch).where(
            batch.c.connection_id == hdr.connection_id, batch.c.status.in_(["RECEIVED", "DECODED"]))).scalar_one()
    if backlog >= s.max_unloaded_batches:
        REGISTRY.inc("alelt_batches_received_total", connection_id=hdr.connection_id, status="backpressure")
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "hub backlog full", headers={"Retry-After": "30"})
    cap = s.max_raw_mb << 20                         # AHS-03: never inflate past the declared / allowed size
    if hdr.raw_bytes < 0 or hdr.raw_bytes > cap:
        raise _reject("too_large", status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                      f"raw_bytes {hdr.raw_bytes} exceeds ALELT_MAX_RAW_MB", hdr.connection_id)
    try:
        raw = decompress(body, hdr.codec, max_out=hdr.raw_bytes)
    except Exception:  # noqa: BLE001
        raise _reject("codec", status.HTTP_422_UNPROCESSABLE_ENTITY,
                      f"cannot decompress ({hdr.codec}) within raw_bytes") from None
    if len(raw) != hdr.raw_bytes or sha256_hex(raw) != hdr.sha256:
        raise _reject("sha_mismatch", status.HTTP_422_UNPROCESSABLE_ENTITY, "sha256/raw_bytes mismatch",
                      hdr.connection_id)

    ha.fence_precheck(eng, hdr.connection_id, hdr.agent_id, lease_epoch)       # X-47: only the lease holder
    spool = blobstore.get_store()
    ref = spool.put(blobstore.batch_key(hdr.connection_id, str(hdr.batch_id), hdr.codec), body)   # durable BEFORE row/ACK

    with eng.begin() as c:
        c.execute(select(connection.c.id).where(connection.c.id == hdr.connection_id).with_for_update())
        # F-21: a concurrent re-send of the same batch may have committed after our pre-check; its row references the
        # very object we just (re)wrote, so answer duplicate and never delete it on the reject paths below.
        dup = c.execute(select(batch.c.to_lri, batch.c.sha256).where(batch.c.id == hdr.batch_id)).first()
        if dup is not None:
            if dup[1] != hdr.sha256:
                raise _reject("dup_mismatch", status.HTTP_409_CONFLICT, "batch_id reused with different content")
            REGISTRY.inc("alelt_batches_received_total", connection_id=hdr.connection_id, status="duplicate")
            return BatchAck(batch_id=hdr.batch_id, durable_pos=Position(engine=hdr.engine, text=dup[0]), duplicate=True)
        _fault("after_put")
        try:
            ha.check_fence(c, hdr.connection_id, hdr.agent_id, lease_epoch)
        except HTTPException:
            blobstore.delete_ref(ref)
            raise
        prev = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == hdr.connection_id)
                         .order_by(batch.c.seq.desc()).limit(1)).first()
        if prev is not None:
            expected = prev[0] or prev[1]
            if hdr.from_pos.text != expected and not _reposition_ok(c, hdr, reposition):
                blobstore.delete_ref(ref)
                raise _reject("gap", status.HTTP_409_CONFLICT,
                              {"error": "position gap/overlap", "expected_from": expected, "got": hdr.from_pos.text},
                              hdr.connection_id)
        cat = store.latest_catalog_id(c, hdr.connection_id)
        r = c.execute(insert(batch).values(
            id=hdr.batch_id, connection_id=hdr.connection_id, agent_id=hdr.agent_id, engine=hdr.engine,
            from_lri=hdr.from_pos.text, to_lri=hdr.to_pos.text, next_lri=hdr.next_pos.text if hdr.next_pos else None,
            rec_count=hdr.rec_count, raw_bytes=hdr.raw_bytes, sha256=hdr.sha256, codec=hdr.codec,
            spool_path=ref, catalog_version_id=cat).on_conflict_do_nothing(index_elements=["id"]))
        if r.rowcount:
            jobs.mark_dirty(c, hdr.connection_id)           # one "stream dirty" marker per connection (RC4)
        store.touch_agent(c, hdr.agent_id)                  # liveness: a batch is a sign of life (RC6)
    _fault("after_commit")                                # crash-point: committed, ACK not yet sent
    REGISTRY.inc("alelt_batches_received_total", connection_id=hdr.connection_id,
                 status="ok" if r.rowcount else "duplicate")
    REGISTRY.inc("alelt_batch_bytes_total", float(hdr.raw_bytes), connection_id=hdr.connection_id)
    log.info("batch received", extra={"component": "alelt_hub", "batch_id": str(hdr.batch_id),
                                      "position": hdr.to_pos.text})
    return BatchAck(batch_id=hdr.batch_id, durable_pos=hdr.to_pos, duplicate=not r.rowcount)


def _reposition_ok(c: Any, hdr: BatchHeader, command_id: str | None) -> bool:
    """A jump is legal only right after a START_CAPTURE{from_pos} command acked by this agent."""
    if not command_id:
        return False
    try:
        cid = uuid.UUID(command_id)
    except ValueError:
        return False
    row = c.execute(select(agent_command.c.name, agent_command.c.args, agent_command.c.status).where(
        agent_command.c.id == cid, agent_command.c.agent_id == hdr.agent_id)).first()
    if not (row and row[0] == "START_CAPTURE" and row[2] in ("SENT", "ACKED")):
        return False
    want = (row[1] or {}).get("from_pos")
    return isinstance(want, str) and _same_pos(hdr.engine, want, hdr.from_pos.text)


def _same_pos(engine: str, a: str, b: str) -> bool:
    """Equal positions in the engine's own order (XC-17: case / zero-padding of the typed text does not matter)."""
    if a == b:
        return True
    src = store.source_connector(engine)
    if src is None:
        return False
    try:
        return src.compare(src.parse_position(a.strip()), src.parse_position(b.strip())) == 0
    except Exception:  # noqa: BLE001 - unparsable text never matches
        return False


# ------------------------------------------------------------------ snapshot chunks
def _snapshot_fence(conn_id: str, agent_id: str, epoch: str | None) -> None:
    with get_engine().connect() as c:
        ha.check_frozen(c, "snapshot")                                      # HA-02
        ha.check_holder(c, conn_id, agent_id, epoch, "snapshot")            # HA-04: stale holder fenced


@router.post("/snapshot")
async def post_snapshot(request: Request, x_alelt_snapshot: str = Header(...), x_alelt_agent_id: str = Header(...),
                        x_alelt_lease_epoch: str | None = Header(default=None),
                        _: str = Depends(agent_identity)) -> dict[str, Any]:
    require_binding(request, x_alelt_agent_id)
    try:
        hdr = SnapshotChunkHeader.model_validate_json(x_alelt_snapshot)
    except ValidationError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, json.loads(e.json())) from None
    body = await _body(request)
    conn_id = await asyncio.to_thread(_agent_conn, x_alelt_agent_id)
    await asyncio.to_thread(_snapshot_fence, conn_id, x_alelt_agent_id, x_alelt_lease_epoch)
    try:                                             # AHS-03: bounded inflate
        await asyncio.to_thread(decompress, body, hdr.codec, get_settings().max_raw_mb << 20)
    except Exception:  # noqa: BLE001
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "cannot decompress within ALELT_MAX_RAW_MB") from None
    ref = await asyncio.to_thread(blobstore.get_store().put,
                                  blobstore.snapshot_key(str(hdr.snapshot_id), hdr.chunk_no, hdr.codec), body)
    with get_engine().begin() as c:
        r = c.execute(insert(snapshot_chunk).values(
            snapshot_id=hdr.snapshot_id, chunk_no=hdr.chunk_no, connection_id=conn_id, table_fqn=hdr.table.fqn,
            rows=hdr.rows, last=hdr.last, header=json.loads(hdr.model_dump_json(by_alias=True)),
            spool_path=ref).on_conflict_do_nothing())
        if r.rowcount:
            jobs.enqueue(c, "snapshot_load", {"snapshot_id": str(hdr.snapshot_id), "chunk_no": hdr.chunk_no},
                         dedup_key=f"snap:{hdr.snapshot_id}:{hdr.chunk_no}")
        store.touch_agent(c, x_alelt_agent_id)                 # liveness: a chunk is a sign of life (RC6)
    return {"ok": True, "duplicate": not r.rowcount}


# ------------------------------------------------------------------ control: long-poll fallback
@router.get("/commands", response_model=list[Command])
async def get_commands(request: Request, agent_id: str = Query(...), wait: float = Query(0, ge=0, le=60),
                       _: str = Depends(agent_identity)) -> list[Command]:
    require_binding(request, agent_id)
    deadline = asyncio.get_running_loop().time() + wait
    while True:
        cmds = await asyncio.to_thread(_take, agent_id)
        if cmds or asyncio.get_running_loop().time() >= deadline:
            return cmds
        await asyncio.sleep(0.5)


@router.post("/commands/{command_id}/ack")
def ack(command_id: uuid.UUID, body: CommandAck, request: Request, agent_id: str = Query(...),
        _: str = Depends(agent_identity)) -> dict[str, bool]:
    require_binding(request, agent_id)
    with get_engine().begin() as c:
        if not store.ack_command(c, agent_id, command_id, body.ok, body.result):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown command")
    return {"ok": True}


def _take(agent_id: str) -> list[Command]:
    with get_engine().begin() as c:
        store.touch_agent(c, agent_id)                      # liveness: a command poll is a sign of life (RC6)
        return store.pending_commands(c, agent_id)


# ------------------------------------------------------------------ control: WebSocket
@router.websocket("/control")
async def control(ws: WebSocket, agent_id: str = Query(...)) -> None:
    """Hub pushes whitelisted commands; agent sends {type: ack|heartbeat|event}."""
    try:                       # same identity rules as HTTP (trusted proxy, constant-time, binding; F-14)
        check_protocol(ws.headers.get("x-alelt-protocol"))
        who = await asyncio.to_thread(authenticate_agent, ws.headers, ws.scope)
        if who.agent_id is not None and who.agent_id != agent_id:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "agent_id does not match the agent credential")
        if who.kind == "mtls":
            from .auth import _mtls_binding

            await asyncio.to_thread(_mtls_binding, who.value, agent_id, None, False)
    except HTTPException:
        await ws.close(code=4401)
        return
    await ws.accept()
    REGISTRY.inc("alelt_control_sessions_total")
    try:
        while True:
            for cmd in await asyncio.to_thread(_take, agent_id):
                await ws.send_text(json.dumps({"type": "command", "command": json.loads(cmd.model_dump_json())}))
            try:
                msg = json.loads(await asyncio.wait_for(ws.receive_text(), timeout=1.0))
            except asyncio.TimeoutError:
                continue
            await asyncio.to_thread(_on_ws_message, agent_id, msg)
    except WebSocketDisconnect:
        return


def _on_ws_message(agent_id: str, msg: Any) -> None:
    """WS messages are validated; malformed ones are dropped (logged), never crash the session (AHS-20)."""
    try:
        _ws_message(agent_id, msg)
    except (KeyError, ValueError, TypeError, ValidationError) as e:
        log.warning("control: invalid message from %s dropped (%s)", agent_id, type(e).__name__,
                    extra={"component": "alelt_hub"})


_EVENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


def _ws_message(agent_id: str, msg: Any) -> None:
    if not isinstance(msg, dict):
        raise TypeError("message must be an object")
    kind = msg.get("type")
    if kind == "ack":
        cmd_id = uuid.UUID(str(msg["command_id"]))
        res = msg.get("result")
        with get_engine().begin() as c:
            store.ack_command(c, agent_id, cmd_id, bool(msg.get("ok", True)), res if isinstance(res, dict) else None)
    elif kind == "heartbeat":
        hb = Heartbeat.model_validate(msg["heartbeat"])
        if hb.agent_id == agent_id:                  # a session may only report for its own agent
            heartbeat(hb, "ws")
    elif kind == "event":
        name = str(msg.get("event", ""))
        if not _EVENT_NAME.match(name):
            raise ValueError("bad event name")
        detail = msg.get("detail") if isinstance(msg.get("detail"), dict) else {}
        with get_engine().begin() as c:
            store.audit(c, f"agent:{agent_id}", f"agent.event.{name}", agent_id,
                        {"event": name, "detail": detail if len(json.dumps(detail, default=str)) <= 8192
                         else {"truncated": True}})

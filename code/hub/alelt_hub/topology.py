"""Topology & HA: status, dry-run plan and the resumable HA <-> standalone switch (X-50).

  alelt topology status [--json]
  alelt topology plan  --to ha|standalone [--db-url URL] [--spool-url URL] [--out DIR]
  alelt topology apply --to ha|standalone [--db-url URL] [--spool-url URL] [--out DIR]
                            [--drain-timeout S] [--restart-wait S] [--reason TEXT] [--yes]
  alelt topology abort                  resume the agents paused by an unfinished switch (source metadata)
  alelt topology lease-clear --connection ID      back to standalone capture for one connection
  alelt migrate-metadata --from URL --to URL [--verify-only] [--reset-target]
  alelt spool-copy --from URL --to URL [--verify-only]

The CLI runs with the *current* configuration (ALELT_DB_URL, ALELT_SPOOL_URL = source); the target is given by flags.
Apply steps are idempotent and resumable (state in hub_setting `topology`, audited `topology.<step>`):
  preflight -> drain -> copy_spool -> migrate_metadata -> rewrite_refs -> write_config -> resume
`resume` queues RESUME for the agents paused by `drain` in the target metadata once the hub runs on the target spool;
until the stack is restarted with the generated config the step waits (re-run apply, or pass --restart-wait).
No data is lost: capture is paused, agent spools and the hub queue are empty before anything moves, every object and
table is verified, and the destination checkpoint (same transaction as the data) stays the exactly-once truth.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import Connection, Engine, create_engine, func, select, text

from alelt_common.models import CommandName
from alelt_common.util import utcnow

from . import blobstore, ha, metamigrate, store
from .auth import Principal, require_perm
from .db import agent, agent_command, batch, connection, get_engine, hub_setting, job
from .dialect import insert, is_sqlite, make_engine
from .ha_models import capture_lease
from .settings import get_settings

STATE_KEY = "topology"
STEPS = ["preflight", "drain", "copy_spool", "migrate_metadata", "rewrite_refs", "write_config", "resume"]
TITLES = {
    "preflight": "Check the target metadata database and spool store",
    "drain": "Pause capture and wait until agent spools and the hub queue are empty",
    "copy_spool": "Copy the hub spool / raw archive to the target store and verify every object (sha256)",
    "migrate_metadata": "Copy the metadata database and verify row counts per table",
    "rewrite_refs": "Point stored batch references at the target store",
    "write_config": "Write the configuration for the new topology (no secrets)",
    "resume": "After the restart on the new configuration, resume the paused agents",
}
AGENT_RECENT_S = 900
# HA-10: every job kind that writes to the metadata / destination; none may be RUNNING (or due) when data moves
DATA_JOB_KINDS = ("decode", "snapshot_load", "snapshot", "resync", "reprocess", "verify", "replay", "integrity")
# HA-19: only these non-secret settings are carried into the generated env file (allow-list, never a deny-regex)
CARRY_SETTINGS = frozenset({
    "ALELT_ENV", "ALELT_LOG_LEVEL", "ALELT_LOG_FORMAT", "ALELT_PUBLIC_URL", "ALELT_AGENT_AUTH", "ALELT_TRUSTED_PROXIES",
    "ALELT_FORWARDED_ALLOW_IPS", "ALELT_THUMBPRINT_HEADER", "ALELT_MAX_BODY_MB", "ALELT_MAX_RAW_MB", "ALELT_MAX_UNLOADED_BATCHES",
    "ALELT_LEASE_TTL_S", "ALELT_TABLES_CONFIG", "ALELT_AUTO_MIGRATE", "ALELT_WORKERS", "ALELT_DEST_KIND", "ALELT_DECODED_CACHE",
    "ALELT_DECODED_CACHE_MEM_MB", "ALELT_RAW_ARCHIVE_DAYS", "ALELT_CORS_ORIGINS", "ALELT_RATE_LIMIT_PER_MIN",
    "ALELT_AUTH_FAIL_LIMIT_PER_MIN", "ALELT_OIDC_ISSUER", "ALELT_OIDC_AUDIENCE", "ALELT_OIDC_JWKS_URL", "ALELT_ARCHIVE_RETENTION_HOURS",
    "ALELT_CONFIG_EXPORT_DIR", "ALELT_AGENT_CREDENTIAL_TTL_DAYS", "ALELT_AGENT_CREDENTIAL_GRACE_S", "ALELT_TIMEZONE",
})
log_fn = Callable[[str], None]


# ------------------------------------------------------------------ state
def _state(c: Connection) -> dict[str, Any]:
    return dict(c.execute(select(hub_setting.c.value).where(hub_setting.c.key == STATE_KEY)).scalar() or {})


def _save(c: Connection, st: dict[str, Any]) -> None:
    c.execute(insert(hub_setting).values(key=STATE_KEY, value=st).on_conflict_do_update(
        index_elements=["key"], set_={"value": st, "updated_at": utcnow()}))


def _save_eng(eng: Engine, st: dict[str, Any]) -> None:
    with eng.begin() as c:
        _save(c, st)


def _redact(url: str) -> str:
    return metamigrate.redact(url) if "://" in url and not url.startswith(("s3://", "file://")) else url


def _db_kind(url: str) -> str:
    return "sqlite" if is_sqlite(url) else "postgresql" if url.startswith("postgres") else url.split(":", 1)[0]


# ------------------------------------------------------------------ status
def install_options() -> list[dict[str, str]]:
    """HA vs non-HA install choices (also used by the setup wizard, X-50)."""
    return [
        {"mode": "standalone", "title": "Single host (non-HA)",
         "when": "Evaluation, small installs, one Docker host; restart-on-failure only",
         "command": "cd deploy/local && docker compose up -d", "doc": "deploy/HA.md#install-standalone"},
        {"mode": "ha", "title": "High availability on Docker (2+ hosts, Swarm or compose)",
         "when": "Production without Kubernetes; external load balancer, PostgreSQL HA and S3-compatible store",
         "command": "docker stack deploy -c deploy/ha/docker-compose.ha.yml alelt",
         "doc": "deploy/HA.md#install-ha-docker"},
        {"mode": "ha", "title": "High availability on Kubernetes (Helm)",
         "when": "Any Kubernetes (on-prem or cloud); replicas, PDBs, anti-affinity, probes",
         "command": "helm install alelt deploy/helm/alelt -f deploy/helm/alelt/values-ha.yaml",
         "doc": "deploy/HA.md#install-ha-helm"},
    ]


def status(eng: Engine | None = None) -> dict[str, Any]:
    eng = eng or get_engine()
    s = get_settings()
    sp = blobstore.describe()
    now = utcnow()
    with eng.connect() as c:
        st = _state(c)
        nodes = ha.nodes(c)
        leases = ha.lease_rows(c)
        agents = c.execute(select(agent.c.id, agent.c.connection_id, agent.c.hostname, agent.c.status,
                                  agent.c.last_heartbeat)).mappings().all()
        conns = [r[0] for r in c.execute(select(connection.c.id).order_by(connection.c.id))]
    alive = [n for n in nodes if n["alive"]]
    apis = [n for n in alive if n["role"] == "api"]
    workers = [n for n in alive if n["role"] == "worker"]
    whosts = {n["host"] for n in workers}
    pg = not is_sqlite(s.db_url)
    conn_out = []
    for cid in conns:
        lease = leases.get(cid)
        live = lease is not None and lease["holder"] and lease["expires_at"] and lease["expires_at"] > now
        ags = []
        for a in [a for a in agents if a["connection_id"] == cid]:
            role = "standalone" if lease is None else ("active" if live and lease["holder"] == a["id"] else "standby")
            ags.append({"agent_id": a["id"], "hostname": a["hostname"], "state": a["status"],
                        "last_heartbeat": a["last_heartbeat"].isoformat() if a["last_heartbeat"] else None,
                        "role": role})
        conn_out.append({"connection_id": cid, "agent_mode": "active_passive" if lease is not None else "standalone",
                         "holder": lease["holder"] if lease else None, "epoch": int(lease["epoch"]) if lease else 0,
                         "lease_id": str(lease["lease_id"]) if lease and lease["lease_id"] else None,
                         "expires_at": lease["expires_at"].isoformat() if lease and lease["expires_at"] else None,
                         "agents": ags})
    agents_ha = bool(conn_out) and all(c["agent_mode"] == "active_passive" and len(c["agents"]) >= 2 for c in conn_out)
    layers = [
        {"layer": "api", "ha": len(apis) >= 2, "kind": f"{len(apis)} replica(s)",
         "detail": ", ".join(sorted({n["host"] or "?" for n in apis})) or "no api heartbeat yet",
         "advice": None if len(apis) >= 2 else "Run 2+ API replicas behind any load balancer (stateless)."},
        {"layer": "workers", "ha": len(workers) >= 2 and len(whosts) >= 2, "kind": f"{len(workers)} worker(s)",
         "detail": f"on {len(whosts)} host(s); jobs claimed once, one worker per stream at a time",
         "advice": None if len(workers) >= 2 and len(whosts) >= 2 else "Run 2+ workers on 2+ hosts."},
        {"layer": "metadata", "ha": pg, "kind": _db_kind(s.db_url), "detail": metamigrate.redact(s.db_url),
         "advice": None if pg else "SQLite is single-host: migrate to PostgreSQL (managed HA or Patroni)."},
        {"layer": "spool", "ha": bool(sp["ha"]), "kind": str(sp["kind"]), "detail": str(sp["url"]),
         "advice": None if sp["ha"] else "Local spool is single-host: use an S3-compatible store (ALELT_SPOOL_URL=s3://...)."},
        {"layer": "agents", "ha": agents_ha, "kind": "active/passive" if agents_ha else "standalone",
         "detail": f"{sum(1 for c in conn_out if c['agent_mode'] == 'active_passive')}/{len(conn_out)} connection(s) "
                   "with a capture lease",
         "advice": None if agents_ha else "Pair each agent with a standby (ha.mode=active_passive), e.g. on the "
                                          "Db2 HADR standby host."},
        {"layer": "destination", "ha": True, "kind": "checkpoint in the load transaction",
         "detail": "failover-safe: a restarted or second worker resumes from the destination checkpoint",
         "advice": None},
    ]
    core = [x["ha"] for x in layers if x["layer"] in ("api", "workers", "metadata", "spool")]
    inferred = "ha" if all(core) else "standalone" if not any(core) else "mixed"
    op = st.get("operation")
    return {"mode": inferred if not st.get("mode") or inferred == "mixed" else
            (st["mode"] if st["mode"] == inferred else "mixed"),
            "recorded_mode": st.get("mode"), "layers": layers,
            "nodes": [{"node_id": n["node_id"], "role": n["role"], "host": n["host"],
                       "started_at": n["started_at"].isoformat(), "last_seen": n["last_seen"].isoformat(),
                       "alive": n["alive"], "spool_url": n["spool_url"]} for n in nodes],
            "connections": conn_out, "operation": _op_public(op) if op else None,
            "install_options": install_options(), "db_url": metamigrate.redact(s.db_url)}


def _op_public(op: dict[str, Any]) -> dict[str, Any]:
    return {"id": op["id"], "to": op["to"], "status": op["status"], "started_at": op["started_at"],
            "finished_at": op.get("finished_at"), "source": op.get("source"), "target": op.get("target"),
            "steps": [{"id": k, "status": op["steps"].get(k, {}).get("status", "todo"),
                       "at": op["steps"].get(k, {}).get("at"), "detail": op["steps"].get(k, {}).get("detail", "")}
                      for k in STEPS]}


# ------------------------------------------------------------------ plan
def resolve(to: str, db_url: str | None, spool_url: str | None) -> tuple[str, str]:
    if to not in ("ha", "standalone"):
        raise ValueError("--to must be ha or standalone")
    s = get_settings()
    cur_spool = blobstore.spool_url()
    if to == "ha":
        return db_url or s.db_url, spool_url or cur_spool
    local = "file://" + Path(os.environ.get("ALELT_SPOOL_DIR") or s.spool_dir).resolve().as_posix()
    return db_url or s.db_url, spool_url or (local if cur_spool.startswith("s3://") else cur_spool)


def _check_db(url: str) -> str | None:
    try:
        e = make_engine(url) if is_sqlite(url) else create_engine(url, connect_args={"connect_timeout": 5})
        with e.connect() as c:
            c.execute(text("SELECT 1"))
        e.dispose()
        return None
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"


def _offline_agents(ags: list[Any], recent: Any) -> list[str]:
    """Registered edge agents without a heartbeat in AGENT_RECENT_S (hub-run `capture:` agents stop with the hub)."""
    return [a for a, hb in ags if not str(a).startswith("capture:") and (not hb or hb < recent)]


def plan(to: str, db_url: str | None = None, spool_url: str | None = None, check: bool = True,
         eng: Engine | None = None, allow_offline: bool = False, allow_failed: bool = False) -> dict[str, Any]:
    """Dry run in plain language: what apply would do, what blocks it."""
    eng = eng or get_engine()
    s = get_settings()
    dst_db, dst_spool = resolve(to, db_url, spool_url)
    src_spool = blobstore.spool_url()
    blockers: list[str] = []
    warnings: list[str] = []
    if to == "ha" and is_sqlite(dst_db):
        blockers.append("HA needs PostgreSQL metadata: pass --db-url postgresql+psycopg://user@host:5432/alelt "
                        "(the current SQLite metadata is copied and verified).")
    if to == "ha" and not dst_spool.startswith("s3://"):
        blockers.append("HA needs a shared spool: pass --spool-url s3://bucket/prefix and set ALELT_S3_ENDPOINT, "
                        "ALELT_S3_ACCESS_KEY_ID, ALELT_S3_SECRET_ACCESS_KEY (any S3-compatible store).")
    if to == "standalone" and not dst_spool.startswith("file://"):
        blockers.append("Standalone uses a local spool: pass --spool-url file:///var/lib/alelt/spool.")
    if check and not blockers:
        if dst_db != s.db_url and (err := _check_db(dst_db)):
            blockers.append(f"Target metadata database not reachable ({metamigrate.redact(dst_db)}): {err}")
        if dst_spool != src_spool:
            try:
                blobstore.open_store(dst_spool).probe()
            except Exception as e:  # noqa: BLE001
                blockers.append(f"Target spool store not usable ({dst_spool}): {e}")
    with eng.connect() as c:
        st = _state(c)
        nb = c.execute(select(func.count()).select_from(batch)).scalar_one()
        recent = utcnow() - timedelta(seconds=AGENT_RECENT_S)
        ags = c.execute(select(agent.c.id, agent.c.last_heartbeat)).all()
        n_lease = c.execute(select(func.count()).select_from(capture_lease)
                            .where(func.coalesce(capture_lease.c.mode, "") != ha.STANDALONE)).scalar_one()
        n_failed = c.execute(select(func.count()).select_from(batch).where(batch.c.status == "FAILED")).scalar_one()
    live = [a for a, hb in ags if hb and hb >= recent]
    offline = _offline_agents(ags, recent)
    op = st.get("operation")
    if op and op.get("status") in ("running", "waiting", "failed") and op.get("to") != to:
        blockers.append(f"Switch {op['id']} to {op['to']} is unfinished: run `alelt topology apply --to "
                        f"{op['to']}` to finish it or `alelt topology abort`.")
    if offline:                                   # HA-02: an offline agent would later ship to the OLD stack only
        msg = (f"{len(offline)} registered agent(s) without a heartbeat in 15 min cannot be paused: "
               f"{', '.join(offline[:5])}. Bring them online, remove them, or pass --allow-offline (their spool is "
               "then shipped after they reconnect to the NEW stack; the old stack refuses writes once frozen).")
        (warnings if allow_offline else blockers).append(msg)
    if n_failed:                                  # HA-10: a FAILED batch is unfinished work on the source metadata
        msg = (f"{n_failed} batch(es) in FAILED state: reprocess or resolve them first, or pass --allow-failed "
               "(they are copied as FAILED).")
        (warnings if allow_failed else blockers).append(msg)
    if to == "ha" and n_lease == 0:
        warnings.append("Capture agents run standalone: add a standby agent per connection (ha.mode=active_passive) "
                        "for agent failover; the hub layers are HA without it.")
    if to == "standalone" and n_lease:
        warnings.append("Agents stay active/passive (works with a standalone hub). To go back to one agent set "
                        "ha.mode=standalone and run `alelt topology lease-clear --connection <id>`.")
    warnings.append("Capture is paused during the switch (typically minutes); the source log is read from the paused "
                    "position afterwards, nothing is skipped.")
    steps = []
    same_spool, same_db = dst_spool == src_spool, dst_db == s.db_url
    try:
        src_objects = None if not check else blobstore.object_count()      # HA-16: cached listing
    except Exception:  # noqa: BLE001
        src_objects = None
    detail = {
        "preflight": f"Target metadata: {metamigrate.redact(dst_db)} ({_db_kind(dst_db)}); target spool: {dst_spool}.",
        "drain": f"Send PAUSE to {len(live)} agent(s), wait for their spools to reach 0 and for the hub to load "
                 f"every received batch.",
        "copy_spool": "Nothing to copy (same store)." if same_spool else
        f"Copy {src_objects if src_objects is not None else 'all'} object(s) from {src_spool} to {dst_spool}; "
        "already copied objects are skipped.",
        "migrate_metadata": "Same metadata database (nothing to copy)." if same_db else
        f"Copy every hub table from {_db_kind(s.db_url)} to {_db_kind(dst_db)}; row counts verified per table.",
        "rewrite_refs": "Not needed (same store)." if same_spool else f"Update the spool reference of {nb} batch(es).",
        "write_config": f"alelt.{to}.env and values.{to}.yaml (API x{2 if to == 'ha' else 1}, "
                        f"workers x{2 if to == 'ha' else 1}); secrets stay in your secret store.",
        "resume": "Restart the stack with the generated configuration; RESUME is then delivered to the paused agents.",
    }
    for k in STEPS:
        skip = (k == "copy_spool" and same_spool) or (k == "rewrite_refs" and same_spool) or (
            k == "migrate_metadata" and same_db)
        done = op and op.get("to") == to and op["steps"].get(k, {}).get("status") in ("done", "skipped")
        steps.append({"id": k, "title": TITLES[k], "detail": detail[k],
                      "status": "blocked" if blockers else "done" if done else "skip" if skip else "todo"})
    cmd = f"alelt topology apply --to {to}"
    if db_url:
        cmd += f" --db-url '{metamigrate.redact(db_url)}'"
    if spool_url:
        cmd += f" --spool-url {spool_url}"
    return {"from": status(eng)["mode"] if check else "", "to": to, "steps": steps, "blockers": blockers,
            "warnings": warnings, "command": cmd}


# ------------------------------------------------------------------ apply
class StepWaiting(Exception):
    """Step cannot finish yet (e.g. waiting for the restart); apply exits, re-run continues."""


def _mark(op: dict[str, Any], step: str, st: str, detail: str = "") -> None:
    op["steps"][step] = {"status": st, "at": utcnow().isoformat(), "detail": detail}


def _drain(eng: Engine, op: dict[str, Any], actor: str, timeout_s: float, log: log_fn) -> str:
    """Pause every live agent, wait for empty spools and an idle hub, then freeze writes (HA-02) and verify again.
    Only an ACKED PAUSE counts (HA-26); offline agents block unless the operator allowed them; RECEIVED / DECODED
    batches and RUNNING (or due QUEUED) jobs of every data kind block (HA-10); FAILED batches block unless allowed."""
    recent = utcnow() - timedelta(seconds=AGENT_RECENT_S)
    with eng.begin() as c:
        ags = [a for a, hb in c.execute(select(agent.c.id, agent.c.last_heartbeat)).all() if hb and hb >= recent]
        for a in ags:
            if a not in op["paused_agents"]:
                cmd = store.issue_command(c, a, CommandName.PAUSE, {}, actor, reason=f"topology switch {op['id']}")
                op["paused_agents"][a] = {"command_id": str(cmd.command_id), "issued_at": cmd.issued_at.isoformat()}
        st = _state(c)
        st["operation"] = op
        _save(c, st)
    log(f"  PAUSE sent to {len(ags)} agent(s); waiting (timeout {timeout_s:.0f}s)")
    end = time.monotonic() + timeout_s
    why = ""
    while True:
        with eng.connect() as c:
            pend = []
            if not op.get("allow_offline"):
                pend += [f"{a}(offline)" for a in _offline_agents(
                    c.execute(select(agent.c.id, agent.c.last_heartbeat)).all(),
                    utcnow() - timedelta(seconds=AGENT_RECENT_S)) if a not in op["paused_agents"]]
            for a, info in op["paused_agents"].items():
                cst = c.execute(select(agent_command.c.status).where(
                    agent_command.c.id == uuid.UUID(info["command_id"]))).scalar()
                row = c.execute(select(agent.c.status, agent.c.spool_bytes, agent.c.last_heartbeat)
                                .where(agent.c.id == a)).first()
                if row is None:
                    continue
                stale = not row[2] or row[2] < utcnow() - timedelta(seconds=AGENT_RECENT_S)
                if stale:
                    if not op.get("allow_offline"):
                        pend.append(f"{a}(went offline, cmd={cst})")
                    continue
                if cst != "ACKED" or row[0] not in ("PAUSED", "STANDBY", "IDLE") or (row[1] or 0) > 0 \
                        or row[2].isoformat() < info["issued_at"]:
                    pend.append(f"{a}({row[0]}, spool={row[1] or 0}B, cmd={cst})")
            rec = c.execute(select(func.count()).select_from(batch).where(
                batch.c.status.in_(["RECEIVED", "DECODED"]))).scalar_one()
            failed = c.execute(select(func.count()).select_from(batch).where(batch.c.status == "FAILED")).scalar_one()
            now = utcnow()
            q = c.execute(select(func.count()).select_from(job).where(
                (job.c.status == "RUNNING") | ((job.c.status == "QUEUED") & job.c.kind.in_(DATA_JOB_KINDS)
                                               & (job.c.run_after <= now)))).scalar_one()
            is_frozen = ha.frozen(c) is not None
        if failed and not op.get("allow_failed"):
            raise RuntimeError(f"{failed} FAILED batch(es): reprocess / resolve them, or re-run with --allow-failed")
        why = f"agents not drained: {', '.join(pend) or '-'}; batches not loaded: {rec}; running or due jobs: {q}"
        if not pend and rec == 0 and q == 0:
            if is_frozen:
                return f"{len(op['paused_agents'])} agent(s) paused, spools empty, queue empty, writes frozen"
            with eng.begin() as c:                  # freeze, then verify once more (nothing can arrive any more)
                ha.set_freeze(c, op["id"], f"topology switch to {op['to']}")
                store.audit(c, actor, "topology.freeze", op["id"], {})
            log("  writes frozen on the source hub (agents get 503 and keep spooling)")
            continue
        if time.monotonic() >= end:
            raise TimeoutError(f"drain timed out: {why}")
        time.sleep(2)


def _write_config(to: str, dst_db: str, dst_spool: str, out: Path, op_id: str) -> list[str]:
    out.mkdir(parents=True, exist_ok=True)
    secretish = re.compile(r"(PASSWORD|SECRET|TOKEN|PRIVATE_KEY$|ACCESS_KEY)", re.I)
    skip = {"ALELT_DB_URL", "ALELT_SPOOL_URL", "ALELT_SPOOL_DIR", "ALELT_DEV_AUTH", "ALELT_TOPOLOGY"}
    lines = [f"# ALELT {to} configuration, generated by `alelt topology apply` ({op_id}, "
             f"{utcnow().isoformat(timespec='seconds')}).",
             "# No secrets in this file: provide the *_FILE targets as Docker / Kubernetes secrets.",
             f"ALELT_TOPOLOGY={to}",
             f"# metadata: {metamigrate.redact(dst_db)}",
             "ALELT_DB_URL_FILE=/run/secrets/alelt_db_url", f"ALELT_SPOOL_URL={dst_spool}"]
    if dst_spool.startswith("s3://"):
        for k in ("ALELT_S3_ENDPOINT", "ALELT_S3_REGION", "ALELT_S3_ADDRESSING", "ALELT_S3_TIMEOUT_S"):
            if os.environ.get(k):
                lines.append(f"{k}={os.environ[k]}")
        lines += ["ALELT_S3_ACCESS_KEY_ID_FILE=/run/secrets/alelt_s3_access_key_id",
                  "ALELT_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/alelt_s3_secret_access_key"]
    else:
        lines.append("# spool is a local volume: mount it on every container of this single host")
    lines.append("# carried over from the current environment (non-secret ALELT_* settings)")
    for k in sorted(os.environ):
        v = os.environ[k]
        if k not in CARRY_SETTINGS or k in skip or secretish.search(k):     # HA-19: allow-list
            continue
        if "://" in v and "@" in v:
            lines.append(f"{k}_FILE=/run/secrets/{k.lower()}   # was {metamigrate.redact(v)}")
        else:
            lines.append(f"{k}={v}")
    env_file = out / f"alelt.{to}.env"
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    n = 2 if to == "ha" else 1
    sp: dict[str, Any] = {"url": dst_spool}
    vals: dict[str, Any] = {
        "mode": to, "api": {"replicas": n}, "worker": {"replicas": n},
        "externalDatabase": {"existingSecret": "alelt-db", "urlKey": "ALELT_DB_URL"},
        "spool": sp,
    }
    if dst_spool.startswith("s3://"):
        vals["s3"] = {"endpoint": os.environ.get("ALELT_S3_ENDPOINT", ""), "region": os.environ.get("ALELT_S3_REGION",
                                                                                                "us-east-1"),
                      "addressing": os.environ.get("ALELT_S3_ADDRESSING", "path" if os.environ.get("ALELT_S3_ENDPOINT")
                                                   else "virtual"),
                      "existingSecret": "alelt-s3"}    # keys ALELT_S3_ACCESS_KEY_ID / ALELT_S3_SECRET_ACCESS_KEY
    values_file = out / f"values.{to}.yaml"
    values_file.write_text(f"# ALELT Helm values ({to}), generated by `alelt topology apply` ({op_id}).\n"
                           "# kubectl create secret generic alelt-db --from-literal=ALELT_DB_URL='<metadata URL>'\n"
                           + ("# kubectl create secret generic alelt-s3 --from-literal=ALELT_S3_ACCESS_KEY_ID=... "
                              "--from-literal=ALELT_S3_SECRET_ACCESS_KEY=...\n" if "s3" in vals else "")
                           + _yaml(vals), encoding="utf-8")
    return [str(env_file), str(values_file)]


def _yaml(d: dict[str, Any], ind: int = 0) -> str:
    out = ""
    for k, v in d.items():
        if isinstance(v, dict):
            out += " " * ind + f"{k}:\n" + _yaml(v, ind + 2)
        else:
            out += " " * ind + f"{k}: {json.dumps(v)}\n"
    return out


def _resume(src_eng: Engine, dst_eng: Engine, op: dict[str, Any], to: str, actor: str, spool_moved: bool,
            restart_wait_s: float, log: log_fn) -> str:
    end = time.monotonic() + restart_wait_s
    dst_spool = op["target"]["spool"]
    while spool_moved or src_eng is not dst_eng:
        with dst_eng.connect() as c:
            alive = [n for n in ha.nodes(c) if n["alive"]]
        ok = [n for n in alive if n["spool_url"] == dst_spool]
        old = [n for n in alive if n["spool_url"] != dst_spool]
        if any(n["role"] == "api" for n in ok) and not old:
            break
        if time.monotonic() >= end:
            raise StepWaiting(
                f"waiting for the hub to run on the new configuration ({len(ok)} node(s) on {dst_spool}, "
                f"{len(old)} still on the old one). Restart with the generated files, then run "
                f"`alelt topology apply --to {to}` again (or pass --restart-wait 600).")
        time.sleep(3)
    why = f"topology switch {op['id']}"
    with dst_eng.begin() as c:
        ha.lift_freeze(c)                       # the copied freeze row: the new stack accepts writes from now on
        for a, info in op["paused_agents"].items():
            if info.get("resumed"):
                continue
            if c.execute(select(agent.c.id).where(agent.c.id == a)).scalar() is None:
                continue
            dup = c.execute(select(agent_command.c.id).where(     # HA-21: crash after issue, before persist
                agent_command.c.agent_id == a, agent_command.c.name == CommandName.RESUME.value,
                agent_command.c.issued_at >= datetime.fromisoformat(info["issued_at"])).limit(1)).first()
            if dup is None:
                store.issue_command(c, a, CommandName.RESUME, {}, actor, reason=why)
            info["resumed"] = True
    return f"RESUME queued for {len(op['paused_agents'])} agent(s)"


def apply(to: str, db_url: str | None = None, spool_url: str | None = None, out: str | None = None,
          drain_timeout_s: float = 900, restart_wait_s: float = 0, actor: str = "cli", reason: str = "",
          log: log_fn = print, allow_offline: bool = False, allow_failed: bool = False) -> int:
    """Run (or continue) the switch. Returns 0 done, 3 waiting for restart, 1 failed."""
    s = get_settings()
    src_eng = get_engine()
    dst_db, dst_spool = resolve(to, db_url, spool_url)
    src_spool = blobstore.spool_url()
    same_db = dst_db == s.db_url
    with src_eng.connect() as c:
        st = _state(c)
    op = st.get("operation")
    target = {"db": metamigrate.redact(dst_db), "spool": dst_spool}
    if op and op.get("status") in ("running", "waiting", "failed"):
        if op["to"] != to or op["target"] != target:
            log(f"switch {op['id']} to {op['to']} ({op['target']}) is unfinished: finish it or run "
                "`alelt topology abort`")
            return 1
        log(f"continuing switch {op['id']} to {to}")
    else:
        prev = op if op and op.get("status") == "aborted" and op.get("target") == target else None
        op = {"id": f"topo-{utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}", "to": to,
              "status": "running", "started_at": utcnow().isoformat(), "finished_at": None, "actor": actor,
              "reason": reason, "source": {"db": metamigrate.redact(s.db_url), "spool": src_spool}, "target": target,
              "paused_agents": {}, "steps": {}}
        if prev and prev.get("metadata_started"):       # HA-22: an aborted switch left the target populated
            op["metadata_started"] = True
        log(f"switch {op['id']}: {op['source']} -> {target}")
    op["status"] = "running"
    op["allow_offline"] = bool(allow_offline or op.get("allow_offline"))
    op["allow_failed"] = bool(allow_failed or op.get("allow_failed"))
    st["operation"] = op
    _save_eng(src_eng, st)
    outdir = Path(out or os.environ.get("ALELT_TOPOLOGY_OUT") or (Path(s.spool_dir).parent / "topology"))
    dst_eng: Engine = src_eng if same_db else make_engine(dst_db)
    spool_moved = dst_spool != src_spool

    def persist() -> None:
        st["operation"] = op
        _save_eng(src_eng, st)
        if op["steps"].get("migrate_metadata", {}).get("status") in ("done",) and dst_eng is not src_eng:
            with dst_eng.begin() as c:
                tst = _state(c)
                tst["operation"] = op
                _save(c, tst)

    rc = 0
    try:
        for step in STEPS:
            if op["steps"].get(step, {}).get("status") in ("done", "skipped"):
                continue
            log(f"[{step}] {TITLES[step]}")
            _mark(op, step, "running")
            persist()
            detail = ""
            if step == "preflight":
                p = plan(to, db_url, spool_url, check=True, allow_offline=op["allow_offline"],
                         allow_failed=op["allow_failed"])
                if p["blockers"]:
                    raise RuntimeError("; ".join(p["blockers"]))
                detail = "target reachable"
            elif step == "drain":
                detail = _drain(src_eng, op, actor, drain_timeout_s, log)
            elif step == "copy_spool":
                if not spool_moved:
                    _mark(op, step, "skipped", "same store")
                    persist()
                    continue
                rep = blobstore.copy_store(blobstore.open_store(src_spool), blobstore.open_store(dst_spool),
                                           progress=lambda r: log(f"  {r.objects} objects checked"))
                if not rep.ok():
                    raise RuntimeError(f"{len(rep.mismatched or [])} object(s) failed verification: "
                                       f"{(rep.mismatched or [])[:5]}")
                detail = f"{rep.objects} object(s): {rep.copied} copied ({rep.bytes} bytes), {rep.skipped} already there"
            elif step == "migrate_metadata":
                if same_db:
                    _mark(op, step, "skipped", "same database")
                    persist()
                    continue
                reset = bool(op.get("metadata_started"))
                op["metadata_started"] = True
                persist()
                src_eng.dispose()
                rep2 = metamigrate.migrate(s.db_url, dst_db, reset_target=reset, log=log)
                for line in rep2.lines():
                    log("  " + line)
                if not rep2.ok:
                    raise RuntimeError("metadata verification failed: " +
                                       ", ".join(t.name for t in rep2.tables if not t.ok))
                detail = f"{len(rep2.tables)} table(s), {sum(t.source_rows for t in rep2.tables)} row(s) verified"
            elif step == "rewrite_refs":
                if not spool_moved:
                    _mark(op, step, "skipped", "same store")
                    persist()
                    continue
                n = metamigrate.rewrite_refs(dst_eng, blobstore.open_store(src_spool), blobstore.open_store(dst_spool))
                detail = f"{n} reference(s) updated"
            elif step == "write_config":
                files = _write_config(to, dst_db, dst_spool, outdir, op["id"])
                detail = "written: " + ", ".join(files)
                log("  " + detail)
            elif step == "resume":
                detail = _resume(src_eng, dst_eng, op, to, actor, spool_moved, restart_wait_s, log)
            _mark(op, step, "done", detail)
            with (dst_eng if step in ("rewrite_refs", "resume") else src_eng).begin() as c:
                store.audit(c, actor, f"topology.{step}", op["id"], {"to": to, "detail": detail, "reason": reason})
            persist()
        op["status"], op["finished_at"] = "done", utcnow().isoformat()
        with dst_eng.begin() as c:
            tst = _state(c)
            tst.update(mode=to, since=op["finished_at"], operation=op)
            _save(c, tst)
            store.audit(c, actor, "topology.switched", op["id"], {"to": to, "reason": reason})
        if dst_eng is not src_eng:
            st.update(mode=to, operation=op, superseded_by=metamigrate.redact(dst_db))
            _save_eng(src_eng, st)
        log(f"switch {op['id']} done: topology is now {to}")
    except StepWaiting as w:
        _mark(op, "resume", "running", str(w))
        op["status"] = "waiting"
        persist()
        log(str(w))
        rc = 3
    except Exception as e:  # noqa: BLE001 - recorded; re-run continues from the failed step
        cur = next((k for k in STEPS if op["steps"].get(k, {}).get("status") == "running"), "preflight")
        _mark(op, cur, "failed", f"{type(e).__name__}: {e}")
        op["status"] = "failed"
        persist()
        log(f"step {cur} failed: {e}\nfix the cause and re-run the same command (completed steps are skipped), "
            "or `alelt topology abort` to resume capture on the current hub")
        rc = 1
    finally:
        if dst_eng is not src_eng:
            dst_eng.dispose()
    return rc


def abort(actor: str = "cli", log: log_fn = print) -> int:
    eng = get_engine()
    with eng.begin() as c:
        st = _state(c)
        op = st.get("operation")
        if not op or op.get("status") not in ("running", "waiting", "failed"):
            log("no unfinished switch")
            return 0
        n = 0
        for a in op.get("paused_agents", {}):
            if c.execute(select(agent.c.id).where(agent.c.id == a)).scalar() is not None:
                store.issue_command(c, a, CommandName.RESUME, {}, actor, reason=f"topology switch {op['id']} aborted")
                n += 1
        op["status"], op["finished_at"] = "aborted", utcnow().isoformat()
        st["operation"] = op
        _save(c, st)
        ha.lift_freeze(c)                       # HA-02: the current hub accepts writes again
        store.audit(c, actor, "topology.abort", op["id"], {"resumed_agents": n})
    log(f"switch {op['id']} aborted; RESUME queued for {n} agent(s) on the current hub. Target data left untouched.")
    return 0


# ------------------------------------------------------------------ console API
router = APIRouter(prefix="/api/v1/topology", tags=["topology"])


class PlanBody(BaseModel):
    to: str
    db_url: str | None = None
    spool_url: str | None = None


class ReleaseBody(BaseModel):
    reason: str | None = None


@router.get("")
def api_status(_: Principal = Depends(require_perm("hub.view"))) -> dict[str, Any]:
    return status()


@router.post("/plan")
def api_plan(body: PlanBody, _: Principal = Depends(require_perm("hub.view"))) -> dict[str, Any]:
    from .console import err

    if body.to not in ("ha", "standalone"):
        raise err(422, "INVALID", "to must be ha or standalone")
    return plan(body.to, body.db_url or None, body.spool_url or None, check=True)


@router.post("/connections/{connection_id}/release-lease")
def api_release(connection_id: str, body: ReleaseBody,
                p: Principal = Depends(require_perm("agent.command"))) -> dict[str, Any]:
    from .console import need_reason

    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        return ha.release(c, connection_id, None, p.subject, reason)


# ------------------------------------------------------------------ CLI
def _print_status(st: dict[str, Any]) -> None:
    print(f"topology: {st['mode']}" + (f" (recorded: {st['recorded_mode']})" if st.get("recorded_mode") else ""))
    for x in st["layers"]:
        print(f"  {x['layer']:<12} {'HA ' if x['ha'] else 'non-HA'}  {x['kind']:<24} {x['detail']}")
        if x.get("advice"):
            print(f"  {'':<12} -> {x['advice']}")
    for cn in st["connections"]:
        print(f"  connection {cn['connection_id']}: {cn['agent_mode']}, holder={cn['holder'] or '-'} "
              f"epoch={cn['epoch']} agents={[a['agent_id'] + ':' + a['role'] for a in cn['agents']]}")
    if st.get("operation"):
        op = st["operation"]
        print(f"  last switch {op['id']} -> {op['to']}: {op['status']}")
        for s in op["steps"]:
            print(f"    {s['id']:<17} {s['status']:<8} {s['detail']}")


def main(argv: list[str] | None = None) -> int:
    from alelt_common.util import setup_logging

    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "migrate-metadata":
        return _cli_migrate(argv[1:])
    if argv and argv[0] == "spool-copy":
        return _cli_spool_copy(argv[1:])
    if argv and argv[0] == "topology":
        argv = argv[1:]
    ap = argparse.ArgumentParser(prog="alelt topology", description="Topology & HA: status, plan, switch")
    ap.add_argument("action", choices=["status", "plan", "apply", "abort", "lease-clear"])
    ap.add_argument("--to", choices=["ha", "standalone"])
    ap.add_argument("--db-url", help="target metadata URL (default: current)")
    ap.add_argument("--spool-url", help="target spool URL (s3://bucket/prefix or file:///path)")
    ap.add_argument("--out", help="directory for the generated configuration")
    ap.add_argument("--drain-timeout", type=float, default=900)
    ap.add_argument("--restart-wait", type=float, default=0, help="seconds to wait for the restart in step resume")
    ap.add_argument("--reason", default="")
    ap.add_argument("--connection")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    ap.add_argument("--allow-offline", action="store_true",
                    help="switch although registered agents are offline (they ship to the new stack later)")
    ap.add_argument("--allow-failed", action="store_true", help="switch although FAILED batches exist")
    a = ap.parse_args(argv)
    setup_logging(os.environ.get("ALELT_LOG_LEVEL", "WARNING"))
    import alelt_hub.ha_models  # noqa: F401
    from .db import init_db

    init_db()
    if a.action == "status":
        st = status()
        print(json.dumps(st, indent=2, default=str)) if a.json else _print_status(st)
        return 0
    if a.action == "abort":
        return abort(actor=os.environ.get("USER") or os.environ.get("USERNAME") or "cli")
    if a.action == "lease-clear":
        if not a.connection:
            ap.error("--connection is required")
        with get_engine().begin() as c:
            done = ha.clear_lease(c, a.connection, "cli")
        print("lease cleared: connection is standalone" if done else "no lease for this connection")
        return 0
    if not a.to:
        ap.error("--to is required")
    p = plan(a.to, a.db_url, a.spool_url, check=True, allow_offline=a.allow_offline, allow_failed=a.allow_failed)
    if a.action == "plan" or a.json:
        if a.json:
            print(json.dumps(p, indent=2))
            return 0 if not p["blockers"] else 2
        print(f"plan: {p['from']} -> {p['to']}")
        for s in p["steps"]:
            print(f"  [{s['status']:<7}] {s['title']}\n            {s['detail']}")
        for b in p["blockers"]:
            print(f"  BLOCKER: {b}")
        for w in p["warnings"]:
            print(f"  note: {w}")
        print(f"run: {p['command']}")
        return 0 if not p["blockers"] else 2
    if p["blockers"]:
        for b in p["blockers"]:
            print(f"BLOCKER: {b}")
        return 2
    if not a.yes and sys.stdin.isatty():
        if input(f"switch to {a.to}: capture pauses during the switch. Type '{a.to}' to continue: ").strip() != a.to:
            print("cancelled")
            return 1
    return apply(a.to, a.db_url, a.spool_url, a.out, a.drain_timeout, a.restart_wait,
                 actor=os.environ.get("USER") or os.environ.get("USERNAME") or "cli", reason=a.reason or
                 f"topology switch to {a.to}", allow_offline=a.allow_offline, allow_failed=a.allow_failed)


def _cli_migrate(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="alelt migrate-metadata",
                                 description="copy the hub metadata between SQLite and PostgreSQL, verified per table")
    ap.add_argument("--from", dest="src", required=True)
    ap.add_argument("--to", dest="dst", required=True)
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--reset-target", action="store_true", help="empty the hub tables in the target first")
    a = ap.parse_args(argv)
    rep = metamigrate.migrate(a.src, a.dst, reset_target=a.reset_target, verify_only=a.verify_only)
    print(f"{rep.source} -> {rep.target}")
    for line in rep.lines():
        print("  " + line)
    print("VERIFIED: row counts and keys match" if rep.ok else "MISMATCH: see tables above")
    return 0 if rep.ok else 1


def _cli_spool_copy(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="alelt spool-copy", description="copy the hub spool between stores")
    ap.add_argument("--from", dest="src", required=True)
    ap.add_argument("--to", dest="dst", required=True)
    ap.add_argument("--verify-only", action="store_true")
    a = ap.parse_args(argv)
    rep = blobstore.copy_store(blobstore.open_store(a.src), blobstore.open_store(a.dst), verify_only=a.verify_only)
    print(f"{rep.objects} object(s): {rep.copied} copied ({rep.bytes} bytes), {rep.skipped} identical, "
          f"{len(rep.mismatched or [])} mismatched")
    return 0 if rep.ok() else 1


if __name__ == "__main__":
    sys.exit(main())

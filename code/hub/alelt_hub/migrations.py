"""Versioned, forward-only hub metadata migrations (upgrade contract, F-24; deploy/UPGRADE.md).

Model:
- Structural changes are declared in `db.py` and applied additively on every upgrade (CREATE missing tables,
  ADD missing nullable / constant-default columns). Anything else (data backfills, renames, type changes) is a
  numbered step in `MIGRATIONS` below - never edited once released, never reverted.
- `schema_migration` records every applied step with the release that applied it.
- A database whose highest recorded step is newer than this code is refused (downgrade guard): rollback of an
  image = restore the metadata backup taken before `alelt migrate` + the previous image.
- Every step is tested from every previous release schema (`hub/alelt_hub/tests/schemas/<release>.<dialect>.sql`,
  `test_upgrade.py`).
- ALELT_AUTO_MIGRATE=0 (HA / production): processes never migrate on start; pending steps raise `SchemaPending`
  until the `migrate` job ran (`alelt migrate --check` is the gate).
"""
from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Connection, Engine, inspect, select, text
from sqlalchemy.schema import CreateColumn

log = logging.getLogger("alelt_hub.migrate")


class SchemaTooNew(RuntimeError):
    """The metadata database was migrated by a newer release."""


class SchemaPending(RuntimeError):
    """Migrations are pending and ALELT_AUTO_MIGRATE=0."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[Connection], None]


def _register_tables() -> Any:
    import alelt_diagnose.tables  # noqa: F401 - registers the diag_* tables on md
    import alelt_hub.ha_models  # noqa: F401 - registers capture_lease / hub_node (X-47, X-49)

    from .db import md

    return md


# ------------------------------------------------------------------ steps
def _m1_baseline(c: Connection) -> None:
    """Release 0.1.0 schema (created additively; nothing to transform)."""


def _m2_config_doc(c: Connection) -> None:
    """F-18: one config source of truth. Every existing pipeline gets a `config_doc` revision built from what was in
    force in 0.1 (pipeline_definition.tables_doc, else the ALELT_TABLES_CONFIG file document) with the console
    overrides (table_config / column_config rows carrying a revision) folded in. X-127: tables in HISTORY mode are
    disabled (mode kept as legacy_sync_mode)."""
    from . import config_store as cs
    from .db import config_doc, connection, pipeline_definition
    from .settings import get_settings
    from .store import config_overrides

    if c.execute(select(config_doc.c.connection_id).limit(1)).first() is not None:
        return
    files = cs.file_docs(get_settings().tables_config)
    defs = {r[0]: dict(r[1]) for r in c.execute(select(pipeline_definition.c.connection_id,
                                                       pipeline_definition.c.tables_doc)) if r[1]}
    conns = sorted({r[0] for r in c.execute(select(connection.c.id))} | set(defs) | set(files))
    todo: list[tuple[str, dict[str, Any], list[str]]] = []
    for cid in conns:
        base = defs.get(cid) or files.get(cid)
        ov = _legacy_overrides(c, cid, config_overrides)
        if base is None and not ov:
            continue
        doc = cs.fold_overrides(cs.migrate_doc(base or {"connection_id": cid}), ov)
        doc, flagged = cs.neutralize_unsupported({**doc, "connection_id": cid})
        todo.append((cid, doc, flagged))
    if not todo:
        return
    rid = cs.new_revision(c, "system:migration", "upgrade 0.1 -> config_doc (one config source of truth, F-18)",
                          "migration", {"migration": 2, "connections": [t[0] for t in todo],
                                        "disabled_unsupported": {t[0]: t[2] for t in todo if t[2]}})
    for cid, doc, _ in todo:
        cs.put_doc(c, rid, cid, doc, validate=False)


def _legacy_overrides(c: Connection, cid: str, fn: Callable[[Connection, str], dict[str, Any]]) -> dict[str, Any]:
    from .db import source_table, table_config

    ov = fn(c, cid)
    for s, t, sm in c.execute(select(source_table.c.src_schema, source_table.c.src_table, table_config.c.sync_mode)
                              .join(table_config, table_config.c.table_id == source_table.c.id)
                              .where(source_table.c.connection_id == cid, table_config.c.revision.is_not(None))):
        if sm and sm != "SOFT_DELETE":
            ov.setdefault(f"{s}.{t}", {})["sync_mode"] = sm
    return ov


def _m3_lifecycle_completion(c: Connection) -> None:
    """Table lifecycle + out-of-row completion + job lanes (Db2 to warehouse initial-load fix design, section 8.2).

    1. tables / columns are created additively; the claim index on `job` is added here (existing table);
    2. every enabled table of each connection's newest catalog gets a lifecycle row derived from its snapshot hold:
       IN_SYNC -> STREAMING (verified), SNAPSHOTTING/RESYNC -> INITIAL_LOAD, CATCHING_UP -> CATCH_UP; no snapshot row
       but the connection already flushed -> STREAMING mode=legacy (no retroactive snapshot); else NEW;
    3. keyed re-read keys parked on integrity rows become completion entries; the integrity row is released only when
       its reasons are nothing but out-of-row signals;
    4. OPEN quarantine rows of out-of-row signals -> RESOLVED (resolution: completion);
    5. QUEUED decode jobs -> one marker per connection, the others DONE (superseded)."""
    import hashlib
    import json

    from sqlalchemy import func, update

    from .db import (catalog_version, checkpoint_mirror, connection, job, quarantine_event, table_completion,
                     table_hold, table_lifecycle)
    from .dialect import insert

    next(i for i in job.indexes if i.name == "ix_job_claim").create(c, checkfirst=True)
    from alelt_common.util import utcnow

    now = utcnow()
    phase_of = {"IN_SYNC": "STREAMING", "SNAPSHOTTING": "INITIAL_LOAD", "RESYNC": "INITIAL_LOAD",
                "CATCHING_UP": "CATCH_UP"}
    for (cid,) in c.execute(select(connection.c.id)).all():
        vid = c.execute(select(func.max(catalog_version.c.id)).where(catalog_version.c.connection_id == cid)).scalar()
        if vid is None:
            continue
        body = c.execute(select(catalog_version.c.body).where(catalog_version.c.id == vid)).scalar() or {}
        try:
            from .settings import get_settings
            from .store import connection_policy

            pol: Any = connection_policy(c, str(cid), get_settings().tables_config)
        except Exception:  # noqa: BLE001 - no usable policy: every catalog table counts as enabled
            pol = None
        tabs = body.get("tables") or []
        flagged = any(t.get("capture_enabled") for t in tabs)
        flushed = (c.execute(select(checkpoint_mirror.c.last_seq).where(checkpoint_mirror.c.stream == cid))
                   .scalar() or 0) > 0
        snaps = {r[0]: (r[1], r[2] or {}) for r in c.execute(select(
            table_hold.c.table_fqn, table_hold.c.state, table_hold.c.body).where(
            table_hold.c.connection_id == cid, table_hold.c.kind == "snapshot"))}
        for t in tabs:
            ref = t.get("table") or {}
            fqn = f"{ref.get('schema')}.{ref.get('name')}"
            if flagged and not t.get("capture_enabled"):
                continue
            if pol is not None:
                from alelt_common.models import TableRef

                if not pol.table(TableRef(schema=str(ref.get("schema")), name=str(ref.get("name")))).enabled:
                    continue
            mode, verified = "auto", None
            if fqn in snaps:
                st, sb = snaps[fqn]
                phase = phase_of.get(str(st), "INITIAL_LOAD")
                if st == "IN_SYNC":
                    verified = now
            elif flushed:
                phase, mode = "STREAMING", "legacy"
            else:
                phase = "NEW"
            vals: dict[str, Any] = dict(connection_id=cid, table_fqn=fqn, phase=phase, mode=mode,
                                        snapshot_id=str((snaps.get(fqn) or (None, {}))[1].get("snapshot_id") or "")
                                        or None, body={"migrated": 3})
            if verified is not None:
                vals["verified_at"] = verified
            c.execute(insert(table_lifecycle).values(**vals).on_conflict_do_nothing(
                index_elements=["connection_id", "table_fqn"]))
    # 3. parked re-read keys -> completion entries
    for cid, fqn, st, held, b, spos in c.execute(select(
            table_hold.c.connection_id, table_hold.c.table_fqn, table_hold.c.state, table_hold.c.held,
            table_hold.c.body, table_hold.c.start_pos).where(table_hold.c.kind == "integrity")).all():
        b = dict(b or {})
        keys = b.get("reread_keys") or []
        if b.get("step") not in ("reread", "resync_wait") or not keys:
            continue
        for k in keys:
            kid = hashlib.sha256(json.dumps(k, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:32]
            c.execute(insert(table_completion).values(
                connection_id=cid, table_fqn=fqn, key_id=kid, key=k, columns=[], first_pos=spos, last_pos=spos,
                first_seq=b.get("restart_seq"), status="PENDING").on_conflict_do_nothing(
                index_elements=["connection_id", "table_fqn", "key_id"]))
        reasons = set((b.get("reasons") or {}).keys())
        if reasons and all(r.startswith("signal needs_keyed_reread") for r in reasons):
            b.update(step=None, reread_keys=[], recovered_by="completion (migration 3)")
            c.execute(update(table_hold).where(table_hold.c.connection_id == cid, table_hold.c.table_fqn == fqn,
                                               table_hold.c.kind == "integrity")
                      .values(state="IN_SYNC", held=False, start_pos=None, body=b))
    # 4. out-of-row signals in quarantine are explained (completion)
    c.execute(update(quarantine_event).where(quarantine_event.c.status == "OPEN",
                                             quarantine_event.c.reason.like("signal needs_keyed_reread%"))
              .values(status="RESOLVED"))
    # 5. one decode marker per connection
    keep: dict[str, int] = {}
    for jid, pl in c.execute(select(job.c.id, job.c.payload).where(job.c.kind == "decode", job.c.status == "QUEUED")
                             .order_by(job.c.id)).all():
        cid = str((pl or {}).get("connection_id") or "")
        if cid not in keep:
            keep[cid] = int(jid)
            taken = c.execute(select(job.c.id).where(job.c.dedup_key == f"decode:{cid}")).first() is not None
            c.execute(update(job).where(job.c.id == jid).values(
                payload={"connection_id": cid}, **({} if taken else {"dedup_key": f"decode:{cid}"})))
        else:
            c.execute(update(job).where(job.c.id == jid).values(status="DONE", error="superseded (migration 3: marker)"))


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "0.1.0 baseline", _m1_baseline),
    Migration(2, "config_doc: one config source of truth (F-18, X-127)", _m2_config_doc),
    Migration(3, "lifecycle_completion: table lifecycle, out-of-row completion, job lanes", _m3_lifecycle_completion),
)
LATEST = MIGRATIONS[-1].version


# ------------------------------------------------------------------ engine
def _missing(c: Connection) -> tuple[list[str], list[tuple[str, str]]]:
    md = _register_tables()
    insp = inspect(c)
    have = set(insp.get_table_names())
    tables = sorted(set(md.tables) - have)
    cols = []
    for name, t in md.tables.items():
        if name in have:
            got = {x["name"] for x in insp.get_columns(name)}
            cols += [(name, col.name) for col in t.columns if col.name not in got]
    return tables, cols


def _applied(c: Connection) -> set[int]:
    if "schema_migration" not in set(inspect(c).get_table_names()):
        return set()
    from .db import schema_migration

    return {int(r[0]) for r in c.execute(select(schema_migration.c.version))}


def status(eng: Engine) -> dict[str, Any]:
    """Current / latest version, pending steps, missing tables and columns (no change)."""
    with eng.connect() as c:
        done = _applied(c)
        tables, cols = _missing(c)
    return {"current": max(done) if done else 0, "latest": LATEST, "applied": sorted(done),
            "pending": [m.version for m in MIGRATIONS if m.version not in done],
            "too_new": sorted(v for v in done if v > LATEST), "missing_tables": tables, "missing_columns": cols}


def _check_not_newer(done: set[int]) -> None:
    newer = sorted(v for v in done if v > LATEST)
    if newer:
        from . import __version__

        raise SchemaTooNew(f"metadata schema is at version {max(newer)} but this release ({__version__}) knows only "
                           f"<= {LATEST}: a newer release migrated it. Run that release, or restore the pre-upgrade "
                           "metadata backup (deploy/UPGRADE.md, rollback)")


def upgrade(eng: Engine, auto: bool | None = None) -> list[int]:
    """Apply missing structure + pending steps in one transaction (Postgres: advisory lock; SQLite: BEGIN
    IMMEDIATE). Returns the versions applied now."""
    from . import __version__
    from .db import md, schema_migration
    from .dialect import add_column_sql, insert

    _register_tables()
    if auto is None:
        auto = os.environ.get("ALELT_AUTO_MIGRATE", "1") != "0"
    if not auto:
        st = status(eng)
        _check_not_newer(set(st["applied"]))
        if st["pending"] or st["missing_tables"] or st["missing_columns"]:
            raise SchemaPending(f"metadata schema needs migration (pending {st['pending']}, missing tables "
                                f"{st['missing_tables']}); run `alelt migrate` first (ALELT_AUTO_MIGRATE=0)")
        return []
    applied: list[int] = []
    with eng.begin() as c:
        if eng.dialect.name == "postgresql":
            # bounded: a migration held by another process for longer is a clear startup error, never a hang
            wait = int(float(os.environ.get("ALELT_MIGRATE_LOCK_WAIT_S", "120")) * 1000)
            c.execute(text(f"SET LOCAL lock_timeout = {wait}"))
            try:
                c.execute(text("SELECT pg_advisory_xact_lock(724631001)"))     # same key as db.create_tables
            except Exception as e:
                raise SchemaPending(f"metadata migration lock not obtained within {wait // 1000}s (another hub "
                                    f"process is migrating this database?): {type(e).__name__}") from e
            c.execute(text("SET LOCAL lock_timeout = 0"))
        _check_not_newer(_applied(c))
        md.create_all(c)
        _, cols = _missing(c)
        for table, col in cols:
            ddl = CreateColumn(md.tables[table].c[col]).compile(dialect=eng.dialect)
            c.execute(text(add_column_sql(eng.dialect, table, str(ddl))))
        done = _applied(c)
        for m in MIGRATIONS:
            if m.version in done:
                continue
            m.apply(c)
            c.execute(insert(schema_migration).values(version=m.version, name=m.name, app_version=__version__))
            applied.append(m.version)
            log.info("metadata migration %s applied: %s", m.version, m.name)
    return applied

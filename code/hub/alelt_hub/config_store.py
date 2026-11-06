"""One config source of truth (F-18, ADR-008).

The hub metadata database is authoritative. Every change is a `config_revision` (a change set: author, reason,
source); the full tables.yaml document of each connection it touches is stored in `config_doc`. The active config of
a connection is its `config_doc` with the highest revision id; rollback = a new revision equal to an older document.

YAML files (Git, `ALELT_TABLES_CONFIG`) are import/export only:
- `seed_from_files()` imports a file document once, for a connection that has no revision yet (first start / upgrade);
- after that a differing file is reported as drift (`drift()`, metric `alelt_config_file_drift`), never applied
  silently - apply it with `alelt config import FILE --reason ...` (or the console import), which creates a revision;
- `export_yaml()` / `alelt config export` writes the active (or any) revision back as diffable YAML.

Readers (worker / loader policy, console, wizard, importer, diagnosis, snapshots) go through
`store.connection_policy()` -> `active_doc()`. `pipeline_definition.tables_doc` is a materialized copy written only
by `put_doc()` in the same transaction (kept for older readers / exports).
"""
from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Engine, func, select, update

from alelt_common.util import sha256_hex, utcnow

from .db import config_doc, config_revision, connection, pipeline_definition

log = logging.getLogger("alelt_hub.config")

CONFIG_VERSION = 1                       # current tables.yaml document format
SUPPORTED_SYNC_MODES = frozenset({"SOFT_DELETE", "SNAPSHOT_ONLY"})
UNSUPPORTED_SYNC_MODES = {"HISTORY": "HISTORY sync mode is out of scope for this release (X-127)"}


class ConfigError(ValueError):
    """Invalid or unsupported tables.yaml document (HTTP 409/422 at the API)."""


# ------------------------------------------------------------------ document format
def migrate_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """Upgrade a tables.yaml document to CONFIG_VERSION (config migration, F-24). v0 (no `config_version`) -> v1:
    console alias `sync_mode: LOG` -> SOFT_DELETE. A document from a newer release is refused."""
    d = copy.deepcopy(doc or {})
    v = int(d.get("config_version") or 0)
    if v > CONFIG_VERSION:
        raise ConfigError(f"config_version {v} was written by a newer release (this hub reads <= {CONFIG_VERSION})")
    if v < 1:
        for sd in (d.get("schemas") or {}).values():
            for td in ((sd or {}).get("tables") or {}).values():
                if isinstance(td, dict) and str(td.get("sync_mode", "")).upper() == "LOG":
                    td["sync_mode"] = "SOFT_DELETE"
    d["config_version"] = CONFIG_VERSION
    return d


def unsupported_tables(doc: dict[str, Any]) -> list[str]:
    """Enabled tables whose sync mode this release cannot honour (X-127)."""
    out = []
    for s, sd in (doc.get("schemas") or {}).items():
        for t, td in ((sd or {}).get("tables") or {}).items():
            td = td or {}
            mode = str(td.get("sync_mode") or "SOFT_DELETE").upper()
            if td.get("enabled", True) and (mode in UNSUPPORTED_SYNC_MODES or mode not in SUPPORTED_SYNC_MODES):
                out.append(f"{s}.{t} ({mode})")
    return out


def validate_doc(connection_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Normalized, validated document for `connection_id` (raises ConfigError)."""
    if not isinstance(doc, dict):
        raise ConfigError("config document must be a mapping")
    d = migrate_doc(doc)
    if d.get("connection_id") not in (None, "", connection_id):
        raise ConfigError(f"document is for connection {d.get('connection_id')!r}, not {connection_id!r}")
    d["connection_id"] = connection_id
    if not isinstance(d.get("schemas") or {}, dict):
        raise ConfigError("schemas must be a mapping")
    bad = unsupported_tables(d)
    if bad:
        raise ConfigError("unsupported sync mode on enabled table(s): " + ", ".join(bad)
                          + " - HISTORY is out of scope for this release (X-127); use SOFT_DELETE or disable them")
    return d


def neutralize_unsupported(doc: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Migration of existing pipelines: tables in an unsupported mode are DISABLED (mode kept as
    `legacy_sync_mode`) instead of being replicated with different semantics. Returns (doc, flagged tables)."""
    d = copy.deepcopy(doc)
    flagged = []
    for s, sd in (d.get("schemas") or {}).items():
        for t, td in ((sd or {}).get("tables") or {}).items():
            if not isinstance(td, dict):
                continue
            mode = str(td.get("sync_mode") or "SOFT_DELETE").upper()
            if mode not in SUPPORTED_SYNC_MODES:
                flagged.append(f"{s}.{t}")
                td.update(enabled=False, sync_mode="SOFT_DELETE", legacy_sync_mode=mode)
    return d, flagged


def digest(doc: dict[str, Any]) -> str:
    return sha256_hex(json.dumps(doc, sort_keys=True, default=str).encode())


# ------------------------------------------------------------------ reads
def active(c: Connection, connection_id: str) -> tuple[int, dict[str, Any]] | None:
    """(revision id, document) in force for a connection, or None (no revision yet)."""
    r = c.execute(select(config_doc.c.revision_id, config_doc.c.doc).where(config_doc.c.connection_id == connection_id)
                  .order_by(config_doc.c.revision_id.desc()).limit(1)).first()
    return (int(r[0]), dict(r[1])) if r else None


def active_doc(c: Connection, connection_id: str) -> dict[str, Any] | None:
    a = active(c, connection_id)
    return a[1] if a else None


def doc_at(c: Connection, connection_id: str, revision_id: int) -> dict[str, Any] | None:
    """Document of a connection as it was right after revision `revision_id`."""
    r = c.execute(select(config_doc.c.doc).where(config_doc.c.connection_id == connection_id,
                                                 config_doc.c.revision_id <= revision_id)
                  .order_by(config_doc.c.revision_id.desc()).limit(1)).scalar()
    return dict(r) if r is not None else None


def revision_connections(c: Connection, revision_id: int) -> list[str]:
    return [r[0] for r in c.execute(select(config_doc.c.connection_id).where(config_doc.c.revision_id == revision_id)
                                    .order_by(config_doc.c.connection_id))]


def connections_with_docs(c: Connection) -> list[str]:
    return [r[0] for r in c.execute(select(config_doc.c.connection_id).distinct().order_by(config_doc.c.connection_id))]


def file_docs(path: str | Path | None) -> dict[str, dict[str, Any]]:
    """tables.yaml documents in ALELT_TABLES_CONFIG (file or directory), keyed by connection id."""
    if not path:
        return {}
    from alelt_loader.policy import _docs

    try:
        return {str(d["connection_id"]): d for d in _docs(Path(path)) if isinstance(d, dict) and d.get("connection_id")}
    except Exception as e:  # noqa: BLE001
        log.warning("config files unreadable (%s): %s", path, type(e).__name__)
        return {}


def base_doc(c: Connection, connection_id: str, tables_config: str | None = None) -> dict[str, Any]:
    """Document a new revision starts from: active doc, else (pre-migration) pipeline tables_doc / file doc."""
    d = active_doc(c, connection_id)
    if d is not None:
        return d
    p = c.execute(select(pipeline_definition.c.tables_doc)
                  .where(pipeline_definition.c.connection_id == connection_id)).scalar()
    if p:
        return migrate_doc(dict(p))
    return migrate_doc(file_docs(tables_config).get(connection_id) or {"connection_id": connection_id})


# ------------------------------------------------------------------ writes (the only write path)
def new_revision(c: Connection, author: str, reason: str, source: str, change: dict[str, Any] | None = None) -> int:
    from .dialect import insert

    rid = c.execute(insert(config_revision).values(author=author, reason=reason,
                                                   body={"source": source, "change": change or {}, "state": {}})
                    .returning(config_revision.c.id)).scalar_one()
    return int(rid)


def put_doc(c: Connection, revision_id: int, connection_id: str, doc: dict[str, Any], *,
            validate: bool = True) -> dict[str, Any]:
    """Attach the full document of `connection_id` to revision `revision_id` (it becomes active) and refresh the
    materialized `pipeline_definition.tables_doc`. Same transaction as the revision."""
    from .dialect import insert

    d = validate_doc(connection_id, doc) if validate else {**migrate_doc(doc), "connection_id": connection_id}
    c.execute(insert(config_doc).values(revision_id=revision_id, connection_id=connection_id, doc=d,
                                        sha256=digest(d)).on_conflict_do_update(
        index_elements=["revision_id", "connection_id"], set_={"doc": d, "sha256": digest(d)}))
    c.execute(update(pipeline_definition).where(pipeline_definition.c.connection_id == connection_id)
              .values(tables_doc=d, revision=revision_id, updated_at=utcnow()))
    try:
        from . import lifecycle

        lifecycle.sync_policy(c, connection_id)            # newly enabled -> NEW, disabled -> DISABLED (RC2)
    except Exception:  # noqa: BLE001 - the revision stands; the next catalog ingest / sweep syncs again
        log.exception("lifecycle sync after config revision failed", extra={"component": "alelt_hub"})
    return d


def commit(c: Connection, connection_id: str, doc: dict[str, Any], author: str, reason: str, source: str,
           change: dict[str, Any] | None = None) -> int | None:
    """New revision with `doc` for one connection; None when the document equals the active one (no-op)."""
    d = validate_doc(connection_id, doc)
    cur = active(c, connection_id)
    if cur is not None and digest(cur[1]) == digest(d):
        return None
    rid = new_revision(c, author, reason, source, {"connection_id": connection_id, **(change or {})})
    put_doc(c, rid, connection_id, d)
    return rid


def apply_table_change(doc: dict[str, Any], schema: str, table: str, change: dict[str, Any]) -> dict[str, Any]:
    """Console edit of one table (enabled / sync_mode / columns[{name, enabled, hashed}]) applied to a document."""
    d = copy.deepcopy(doc)
    sd = d.setdefault("schemas", {}).setdefault(schema, {}) or {}
    d["schemas"][schema] = sd
    td = sd.setdefault("tables", {}).setdefault(table, {}) or {}
    sd["tables"][table] = td
    if change.get("enabled") is not None:
        td["enabled"] = bool(change["enabled"])
    if change.get("sync_mode") is not None:
        td["sync_mode"] = "SOFT_DELETE" if change["sync_mode"] == "LOG" else change["sync_mode"]
    for cu in change.get("columns") or []:
        col = td.setdefault("columns", {}).setdefault(cu["name"], {}) or {}
        td["columns"][cu["name"]] = col
        col.update(enabled=bool(cu.get("enabled", True)), hashed=bool(cu.get("hashed", False)))
    return d


def fold_overrides(doc: dict[str, Any], overrides: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Legacy console overrides (table_config / column_config rows with a revision) folded into a document."""
    d = copy.deepcopy(doc)
    for fqn, ov in overrides.items():
        s, _, t = fqn.partition(".")
        d = apply_table_change(d, s, t, {
            "enabled": ov.get("enabled"), "sync_mode": ov.get("sync_mode"),
            "columns": [{"name": n, **cv} for n, cv in (ov.get("columns") or {}).items()]})
    return d


# ------------------------------------------------------------------ files: seed, drift, export
def seed_from_files(eng: Engine, tables_config: str | None, author: str = "system:file-seed") -> list[tuple[str, int]]:
    """Import file documents for connections that have no revision yet (one-time; never overrides)."""
    docs = file_docs(tables_config)
    out: list[tuple[str, int]] = []
    if not docs:
        return out
    with eng.begin() as c:
        if c.dialect.name == "postgresql":                 # api + worker + replicas start together
            from sqlalchemy import text

            c.execute(text("SELECT pg_advisory_xact_lock(724631002)"))
        have = set(connections_with_docs(c))
        for cid, doc in sorted(docs.items()):
            if cid in have:
                continue
            d, flagged = neutralize_unsupported(migrate_doc(doc))
            rid = new_revision(c, author, f"initial import of {tables_config}", "file-seed",
                               {"connection_id": cid, "file": str(tables_config), "disabled_unsupported": flagged})
            put_doc(c, rid, cid, d)
            out.append((cid, rid))
            log.info("config: seeded connection %s from %s as revision %s", cid, tables_config, rid)
    return out


def drift(c: Connection, tables_config: str | None) -> list[dict[str, Any]]:
    """File documents that differ from the active revision (reported, never applied)."""
    from .console import diff as _diff

    out = []
    for cid, fdoc in sorted(file_docs(tables_config).items()):
        cur = active(c, cid)
        if cur is None:
            out.append({"connection_id": cid, "revision": None, "state": "NOT_IMPORTED", "changes": []})
            continue
        try:
            want = migrate_doc(fdoc)
        except ConfigError as e:
            out.append({"connection_id": cid, "revision": cur[0], "state": "INVALID", "error": str(e), "changes": []})
            continue
        if digest({**want, "connection_id": cid}) != digest(cur[1]):
            out.append({"connection_id": cid, "revision": cur[0], "state": "DRIFT",
                        "changes": _diff(cur[1].get("schemas") or {}, want.get("schemas") or {})[:200]})
    return out


def export_yaml(doc: dict[str, Any], revision_id: int | None = None) -> str:
    import yaml

    head = (f"# exported from the hub metadata (config revision {revision_id}); import with "
            "`alelt config import <file> --reason ...` - editing this file alone changes nothing\n")
    return head + yaml.safe_dump(doc, sort_keys=True, allow_unicode=True)


def known_connections(c: Connection) -> list[str]:
    ids = {r[0] for r in c.execute(select(connection.c.id))}
    return sorted(ids | set(connections_with_docs(c)))


def revision_count(c: Connection) -> int:
    return int(c.execute(select(func.count()).select_from(config_revision)).scalar_one())

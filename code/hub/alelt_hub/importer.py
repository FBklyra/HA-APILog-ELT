"""Migration importer + pipeline wizards + production safety guard (console API, /api/v1).

Import (`import.run`): plugins -> discover (live read-only API, or offline files) -> review/edit plan + diff ->
apply (one config_revision; pipelines, triggers, roles, golden spec; legacy source stays active: LEGACY).
Wizard (`table.configure`): source catalog, source test + prerequisite checklist (fix-it SQL shown, never run: R1),
destination test + bootstrap, smart defaults, create parallel test (+ pre-configured agent), live progress.
Guard (`users.manage` to edit): production deny list; every source/destination/legacy endpoint is checked, and the
generated agent config carries it as `deny_hosts`.

Secrets: credentials are used for the request only; values that the user chooses to keep go to the secret store
(alelt_import.secrets) and only references are stored. Nothing secret is returned, logged or audited.
"""
from __future__ import annotations

import base64
import binascii
import secrets as pysecrets
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import Connection, func, select, update

from alelt_common.models import TableRef
from alelt_common.util import utcnow
from alelt_import import golden as golden_mod
from alelt_import import wizard as wiz
from alelt_import.files import BundleError, load_files
from alelt_import import hostmap
from alelt_import.guard import DenyList, ProdTargetError, normalize_entry
from alelt_import.models import MigrationPlan, PlanEdits, InventoryModel
from alelt_import.plan import EditError, apply_edits, changes_existing, plan_diff, slug
from alelt_import.plugin import DiscoveryError, ImportPlugin, plugins
from alelt_import.secrets import SecretStore, valid_ref

from . import setup as setup_mod
from . import store
from .auth import Principal, can, current_principal, require_perm
from .console import _new_revision, _seal_revision, err, need_reason
from .db import (
    agent,
    app_user,
    batch,
    batch_table_stat,
    connection,
    get_engine,
    golden_capture,
    import_session,
    pipeline_definition,
    pipeline_trigger,
    recon_result,
    role_assignment,
    snapshot_chunk,
    source_table,
)
from .dialect import seconds_since
from .settings import get_settings

router = APIRouter(prefix="/api/v1", tags=["import", "wizard"])
_DENY_KEY = "guard.prod_denylist"
_ONLINE_S = 120
_PLUGINS: dict[str, ImportPlugin] | None = None


# ------------------------------------------------------------------ helpers
def _plugins() -> dict[str, ImportPlugin]:
    global _PLUGINS
    if _PLUGINS is None:
        _PLUGINS = plugins()
    return _PLUGINS


def _iso(v: Any) -> str | None:
    return v.isoformat().replace("+00:00", "Z") if v else None


def _sid(v: str) -> uuid.UUID:
    try:
        return uuid.UUID(v)
    except ValueError:
        raise err(404, "NOT_FOUND", f"unknown id {v}") from None


def _sources() -> dict[str, str]:
    """Registered source engines -> capture mode."""
    out: dict[str, str] = {}
    for eng in get_settings().sources:
        src = store.source_connector(eng)
        if src is not None:
            out[eng] = src.capture_mode
    return out


def denylist(c: Connection | None = None) -> DenyList:
    def load(cx: Connection) -> list[str]:
        return list((setup_mod._get(cx, _DENY_KEY) or {}).get("entries") or [])
    console_entries = load(c) if c is not None else _with(load)
    return DenyList.of([*get_settings().prod_denylist, *console_entries])


def _with(fn: Any) -> Any:
    with get_engine().connect() as c:
        return fn(c)


def _guard(settings: dict[str, Any] | None, what: str, c: Connection | None = None) -> None:
    try:
        denylist(c).check_settings(settings or {})
    except ProdTargetError as e:
        raise err(409, "PROD_TARGET", f"{what}: {e}") from None


def _secret_values(store_: SecretStore, provided: dict[str, SecretInput], keys: set[str]) -> dict[str, str]:
    """Persist provided secrets, return key -> reference. Unknown keys are refused."""
    refs: dict[str, str] = {}
    for k, v in provided.items():
        if k not in keys:
            raise err(400, "UNKNOWN_SECRET", f"unknown secret key {k}")
        if v.ref:
            if not valid_ref(v.ref):
                raise err(400, "BAD_SECRET_REF", "secret reference must be env:<NAME> or file:<NAME>")
            refs[k] = v.ref
        elif v.value:
            refs[k] = store_.put(k, v.value)
    return refs


# ------------------------------------------------------------------ models
class SecretInput(BaseModel):
    value: str | None = Field(default=None, repr=False)
    ref: str | None = None


class DiscoverReq(BaseModel):
    plugin: str
    credentials: dict[str, str] = Field(default_factory=dict, repr=False)
    reason: str | None = None


class FileIn(BaseModel):
    name: str = Field(min_length=1, max_length=300)
    data_base64: str = Field(repr=False)


class DiscoverFilesReq(BaseModel):
    plugin: str | None = None                  # None = auto-detect among plugins that accept files
    files: list[FileIn] = Field(min_length=1, max_length=200)
    reason: str | None = None


class PlanEditReq(BaseModel):
    edits: PlanEdits
    reason: str | None = None


class ApplyReq(BaseModel):
    reason: str | None = None
    environment: Literal["TEST", "PROD"] = "TEST"
    secrets: dict[str, SecretInput] = Field(default_factory=dict)
    source_overrides: dict[str, dict[str, Any]] = Field(default_factory=dict)        # legacy_id -> settings
    destination_overrides: dict[str, dict[str, Any]] = Field(default_factory=dict)
    start_shadow: bool = True
    host_map: dict[str, str] | None = None      # prod host -> test host (required step, see /host-map)


class HostMapReq(BaseModel):
    mappings: dict[str, str]
    reason: str | None = None


_HOSTMAP_KEY = "import.hostmap:"


# ------------------------------------------------------------------ import: plugins + discovery
@router.get("/import/plugins")
def import_plugins(_: Principal = Depends(require_perm("import.run"))) -> list[dict[str, Any]]:
    return [p.info().model_dump() for _, p in sorted(_plugins().items())]


def _plugin(name: str) -> ImportPlugin:
    p = _plugins().get(name)
    if p is None:
        raise err(404, "UNKNOWN_PLUGIN", f"no import plugin '{name}'")
    return p


def _taken_ids(c: Connection) -> set[str]:
    return {r[0] for r in c.execute(select(connection.c.id))}


def _new_session(p: Principal, plugin: ImportPlugin, mode: str, reason: str, inv: InventoryModel | None,
                 message: str | None, files: list[dict[str, str]] | None = None,
                 golden: dict[str, Any] | None = None) -> dict[str, Any]:
    plan = None
    with get_engine().begin() as c:
        if inv is not None:
            plan = plugin.map(inv, _sources())
        sid = uuid.uuid4()
        c.execute(import_session.insert().values(
            id=sid, plugin=plugin.name, platform=inv.platform if inv else plugin.display_name, mode=mode,
            status="DISCOVERED" if inv is not None else "FAILED", started_by=p.subject, finished_at=utcnow(),
            files=files, inventory=inv.model_dump(mode="json") if inv else None,
            plan=plan.model_dump(mode="json") if plan else None, edits=None, golden=golden, message=message))
        store.audit(c, p.subject, "import.discover", str(sid),
                    {"plugin": plugin.name, "mode": mode, "ok": inv is not None, "reason": reason,
                     "files": [f["name"] for f in files or []]}, p.primary_role)
    return session_view(sid)


@router.post("/import/discover")
def discover(body: DiscoverReq, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    """Live read-only discovery. Credentials are used for this call only (never stored, logged or returned)."""
    reason = need_reason(body.reason)
    plugin = _plugin(body.plugin)
    if not plugin.live:
        raise err(400, "NOT_LIVE", f"{plugin.display_name} supports file import only")
    dl = denylist()
    for ep in plugin.endpoints(body.credentials):
        m = dl.match(ep)
        if m:
            raise err(409, "PROD_TARGET", f"{plugin.display_name}: '{ep}' matches the production deny list entry "
                                          f"'{m}'. Use the offline file import instead.")
    inv, msg = None, None
    try:
        inv = plugin.discover(body.credentials)
    except DiscoveryError as e:
        msg = str(e)
    except Exception as e:  # noqa: BLE001  (never echo exception text: it may carry request data)
        msg = f"discovery failed ({type(e).__name__})"
    finally:
        body.credentials.clear()
    return _new_session(p, plugin, "live", reason, inv, msg)


@router.post("/import/discover/files")
def discover_files(body: DiscoverFilesReq, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    """Offline import: legacy API exports (JSON / ZIP), landing DDL, INFORMATION_SCHEMA.COLUMNS CSV. No network."""
    reason = need_reason(body.reason)
    try:
        raw = {f.name: base64.b64decode(f.data_base64, validate=True) for f in body.files}
    except (binascii.Error, ValueError):
        raise err(400, "BAD_FILE", "files must be base64 encoded") from None
    try:
        bundle = load_files(raw)
    except BundleError as e:
        raise err(413 if "large" in str(e) else 400, "BAD_FILE", str(e)) from None
    gold = golden_mod.to_json(bundle.ddl, bundle.columns) if bundle.has_golden else None
    candidates = [_plugin(body.plugin)] if body.plugin else [x for _, x in sorted(_plugins().items(), key=lambda kv: (
        kv[0] == "yaml_manifest", kv[0])) if x.accepts_files]
    inv, msg, chosen = None, None, candidates[0] if candidates else _plugin("yaml_manifest")
    errors = []
    if bundle.json_docs or bundle.yaml_docs:
        for cand in candidates:
            try:
                inv, chosen = cand.discover_files(bundle), cand
                break
            except DiscoveryError as e:
                errors.append(f"{cand.display_name}: {e}")
            except Exception as e:  # noqa: BLE001
                errors.append(f"{cand.display_name}: {type(e).__name__}")
        if inv is None:
            msg = "; ".join(errors) or "no plugin recognised the files"
    elif gold:
        msg = "only a golden spec was found (DDL / columns); no pipeline definitions"
    else:
        msg = "no recognised file (expected legacy export JSON/ZIP, manifest YAML, DDL .sql or columns .csv)"
    out = _new_session(p, chosen, "files", reason, inv, msg, bundle.files, gold)
    if inv is None and gold:      # golden-only drop is still useful to the parallel-test wizard
        with get_engine().begin() as c:
            c.execute(update(import_session).where(import_session.c.id == uuid.UUID(out["id"]))
                      .values(status="GOLDEN_ONLY"))
        out["status"] = "GOLDEN_ONLY"
    return out


# ------------------------------------------------------------------ import: session view, edits, diff
def _current(c: Connection) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[str]], set[str]]:
    defs = {r.connection_id: {"tables": r.tables_doc, "latency_target_s": r.latency_target_s}
            for r in c.execute(select(pipeline_definition))}
    trig = {f"triggers.{r.kind}.{r.name}": {"target": r.target, "events": r.events or [],
                                            "after": r.after_connections or []}
            for r in c.execute(select(pipeline_trigger))}
    roles: dict[str, list[str]] = {}
    for email, role in c.execute(select(app_user.c.email, role_assignment.c.role)
                                 .join(role_assignment, role_assignment.c.user_id == app_user.c.id)):
        roles.setdefault((email or "").lower(), []).append(role)
    return defs, trig, roles, set(defs)


def _plan_of(r: Any) -> MigrationPlan | None:
    if not r.plan:
        return None
    plan = MigrationPlan.model_validate(r.plan)
    return apply_edits(plan, PlanEdits.model_validate(r.edits)) if r.edits else plan


def session_view(sid: uuid.UUID) -> dict[str, Any]:
    with get_engine().connect() as c:
        r = c.execute(select(import_session).where(import_session.c.id == sid)).first()
        if r is None:
            raise err(404, "NOT_FOUND", "unknown import run")
        plan = _plan_of(r)
        defs, trig, roles, existing = _current(c)
    d = plan_diff(plan, defs, trig, roles) if plan else []
    gold = r.golden or {}
    return {"id": str(r.id), "plugin": r.plugin, "platform": r.platform, "mode": r.mode, "status": r.status,
            "started_at": _iso(r.started_at), "finished_at": _iso(r.finished_at), "started_by": r.started_by,
            "files": r.files or [], "message": r.message, "inventory": r.inventory,
            "plan": plan.model_dump(mode="json") if plan else None,
            "warnings": [w.model_dump() for w in plan.warnings] if plan else [],
            "diff": d, "needs_approval": changes_existing(d, existing),
            "golden": {"tables": gold.get("tables", []), "columns": len(gold.get("columns", []))} if gold else None,
            "requested_by": r.requested_by, "approved_by": r.approved_by, "applied_revision": r.applied_revision,
            "result": r.result}


@router.get("/import/runs/{run_id}")
def get_run(run_id: str, _: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    return session_view(_sid(run_id))


@router.put("/import/runs/{run_id}/plan")
def edit_plan(run_id: str, body: PlanEditReq, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    sid = _sid(run_id)
    with get_engine().begin() as c:
        r = c.execute(select(import_session).where(import_session.c.id == sid)).first()
        if r is None or not r.plan:
            raise err(404, "NOT_FOUND", "unknown import run or no plan")
        if r.status == "APPLIED":
            raise err(409, "ALREADY_APPLIED", "this import was applied; start a new discovery")
        try:
            apply_edits(MigrationPlan.model_validate(r.plan), body.edits)
        except EditError as e:
            raise err(409 if e.code in ("KEY_COLUMN", "UNSUPPORTED", "DUPLICATE_ID") else 404, e.code, str(e)) from None
        c.execute(update(import_session).where(import_session.c.id == sid).values(
            edits=body.edits.model_dump(mode="json"), requested_by=None))
        store.audit(c, p.subject, "import.plan.edit", run_id, {"reason": body.reason or "review"}, p.primary_role)
    return session_view(sid)


# ------------------------------------------------------------------ apply (shared by import + wizard)
def _upsert_def(c: Connection, cid: str, values: dict[str, Any]) -> None:
    if c.execute(select(pipeline_definition.c.connection_id).where(pipeline_definition.c.connection_id == cid)).first():
        c.execute(update(pipeline_definition).where(pipeline_definition.c.connection_id == cid)
                  .values(**values, updated_at=utcnow()))
    else:
        c.execute(pipeline_definition.insert().values(connection_id=cid, **values))


def _write_tables_doc(cid: str, doc: dict[str, Any], rev: int | None = None) -> str | None:
    """Diffable YAML export of the pipeline config for Git (F-18: export only - the loader reads the active config
    revision from the metadata DB, never this file). ALELT_CONFIG_EXPORT_DIR (or ALELT_PIPELINES_DIR), else
    <spool parent>/config-export. Never written into ALELT_TABLES_CONFIG."""
    import os

    from .config_store import export_yaml

    s = get_settings()
    root = Path(os.environ.get("ALELT_CONFIG_EXPORT_DIR") or os.environ.get("ALELT_PIPELINES_DIR")
                or s.spool_dir.parent / "config-export")
    cfg = Path(s.tables_config).resolve() if s.tables_config else None
    if cfg is not None and (root.resolve() == cfg or root.resolve() == cfg.parent and cfg.is_file()):
        root = s.spool_dir.parent / "config-export"
    import re

    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}", cid) or ".." in cid:
        return None                              # never a path from user input (path traversal)
    try:
        root.mkdir(parents=True, exist_ok=True)
        f = root / f"{cid}.yaml"
        f.write_text(export_yaml(doc, rev), encoding="utf-8")
        return str(f)
    except OSError:
        return None


def _store_golden(c: Connection, cid: str, schema: str | None, gold: dict[str, Any], origin: str) -> None:
    c.execute(golden_capture.insert().values(id=uuid.uuid4(), connection_id=cid, schema_name=schema, origin=origin,
                                             status="OK" if gold.get("tables") else "EMPTY", body=gold))


def _materialize(c: Connection, p: Principal, rev: int, origin: str, environment: str,
                 pc: dict[str, Any], status: str) -> dict[str, Any]:
    """Create/update one pipeline: connection row (legacy id kept), definition, loader config file."""
    cid = pc["connection_id"]
    src = pc["source"]
    settings = src.get("settings") or {}
    if c.execute(select(connection.c.id).where(connection.c.id == cid)).first() is None:
        c.execute(connection.insert().values(
            id=cid, engine=pc["source_engine"], db_database=str(settings.get("database") or "") or None,
            db_instance=f"{settings.get('host')}:{settings.get('port')}" if settings.get("host") else None,
            legacy_connection_id=pc.get("legacy_id")))
    _upsert_def(c, cid, {
        "origin": origin, "environment": environment, "legacy_ref": pc.get("legacy_id"),
        "source_engine": pc["source_engine"], "destination_engine": pc.get("destination_engine") or "postgres",
        "source": src, "destination": pc.get("destination") or {}, "tables_doc": pc["tables"],
        "latency_target_s": int(pc.get("latency_target_s") or 120), "schedule": pc.get("schedule"),
        "status": status, "active_source": "LEGACY", "revision": rev, "created_by": p.subject})
    from . import config_store

    try:                                         # F-18: the pipeline's config = a document in revision `rev`
        tdoc = config_store.put_doc(c, rev, cid, pc["tables"])
    except config_store.ConfigError as e:
        raise err(409, "INVALID_CONFIG", f"{cid}: {e}") from None
    path = _write_tables_doc(cid, tdoc, rev)
    return {"connection_id": cid, "status": status, "config_file": path}


@router.post("/import/runs/{run_id}/apply")
def apply_run(run_id: str, body: ApplyReq, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    sid = _sid(run_id)
    sstore = SecretStore()
    with get_engine().begin() as c:
        r = c.execute(select(import_session).where(import_session.c.id == sid)).first()
        if r is None:
            raise err(404, "NOT_FOUND", "unknown import run")
        if r.status == "APPLIED":
            raise err(409, "ALREADY_APPLIED", "this import was already applied")
        plan = _plan_of(r)
        if plan is None:
            raise err(409, "NO_PLAN", r.message or "nothing to apply")
        conns = [x for x in plan.connections if x.include]
        if not conns and not any(t.include for t in plan.triggers) and not any(x.include for x in plan.roles):
            raise err(409, "EMPTY_PLAN", "nothing is selected")
        for pc in conns:
            if pc.legacy_id in body.source_overrides:
                pc.source["settings"] = {**pc.source.get("settings", {}), **body.source_overrides[pc.legacy_id]}
            if pc.legacy_id in body.destination_overrides:
                pc.destination["settings"] = {**pc.destination.get("settings", {}),
                                              **body.destination_overrides[pc.legacy_id]}
        mapping = _host_mapping(c, sid, body.host_map, conns)       # required prod -> test host mapping
        for pc in conns:
            pc.source["settings"] = hostmap.apply(pc.source.get("settings") or {}, mapping)
            pc.destination["settings"] = hostmap.apply(pc.destination.get("settings") or {}, mapping)
            _guard(pc.source.get("settings"), f"{pc.legacy_name} source", c)
            _guard(pc.destination.get("settings"), f"{pc.legacy_name} destination", c)
        defs, trig, roles, existing = _current(c)
        d = plan_diff(plan, defs, trig, roles)
        if changes_existing(d, existing):           # two-person rule: it changes pipelines already in force
            if r.requested_by is None:
                c.execute(update(import_session).where(import_session.c.id == sid).values(
                    status="PENDING_APPROVAL", requested_by=p.subject))
                store.audit(c, p.subject, "import.apply.request", run_id, {"reason": reason}, p.primary_role)
                return {**_view_after(c, sid), "status": "PENDING_APPROVAL"}
            if r.requested_by.strip().lower() == p.subject.strip().lower():
                raise err(409, "TWO_PERSON_RULE", "a second DataOperator must approve this import")
        keys = {s.key for s in plan.secrets}
        refs = _secret_values(sstore, body.secrets, keys)
        rev = _new_revision(c, p.subject, reason, "alelt_import", {
            "import_session": run_id, "plugin": plan.plugin, "connections": [x.connection_id for x in conns]})
        results = []
        for pc in conns:
            hub_keys = [s.key for s in plan.secrets if s.where == "hub" and pc.connection_id in s.used_by]
            missing = [k for k in hub_keys if k not in refs]
            doc = pc.model_dump(mode="json")
            doc["source"]["secret_refs"] = {f: refs[k] for f, k in pc.source.get("secrets", {}).items() if k in refs}
            doc["destination"]["secret_refs"] = {f: refs[k] for f, k in pc.destination.get("secrets", {}).items()
                                                 if k in refs}
            status = "NEEDS_SECRETS" if missing else ("SHADOW" if body.start_shadow and not pc.paused else "READY")
            res = _materialize(c, p, rev, f"import:{run_id}", body.environment, doc, status)
            if r.golden:
                _store_golden(c, pc.connection_id, pc.legacy_schema, r.golden, "files")
                res["golden"] = golden_mod.contract_check(r.golden.get("tables", []), _enabled_dest(pc.tables))
            res["missing_secrets"] = missing
            results.append(res)
        for t in (t for t in plan.triggers if t.include):
            vals = {"target": t.target, "events": t.events, "after_connections": t.after_connections,
                    "settings": t.settings, "enabled": True, "legacy_ref": t.legacy_id, "revision": rev}
            have = c.execute(select(pipeline_trigger.c.id).where(pipeline_trigger.c.kind == t.kind,
                                                                 pipeline_trigger.c.name == t.name)).scalar()
            if have:
                c.execute(update(pipeline_trigger).where(pipeline_trigger.c.id == have).values(**vals))
            else:
                c.execute(pipeline_trigger.insert().values(id=uuid.uuid4(), kind=t.kind, name=t.name, **vals))
        granted = []
        for role in (x for x in plan.roles if x.include):
            uid = c.execute(select(app_user.c.id).where(func.lower(app_user.c.email) == role.email.lower())).scalar()
            if uid is None:
                uid = uuid.uuid4()
                c.execute(app_user.insert().values(id=uid, email=role.email, entra_oid=None))
            for rn in role.roles:
                if not c.execute(select(role_assignment.c.user_id).where(role_assignment.c.user_id == uid,
                                                                       role_assignment.c.role == rn)).first():
                    c.execute(role_assignment.insert().values(user_id=uid, role=rn))
                    granted.append(f"{role.email}:{rn}")
        _seal_revision(c, rev)
        result = {"revision": rev, "connections": results, "triggers": sum(1 for t in plan.triggers if t.include),
                  "roles_granted": granted, "secrets_saved": sorted(refs)}
        c.execute(update(import_session).where(import_session.c.id == sid).values(
            status="APPLIED", applied_revision=rev, approved_by=p.subject if r.requested_by else None, result=result))
        store.audit(c, p.subject, "import.apply", run_id,
                    {"revision": rev, "reason": reason, "connections": [x["connection_id"] for x in results],
                     "secret_keys": sorted(refs), "environment": body.environment, "host_map": mapping},
                    p.primary_role)
        return _view_after(c, sid)


def _host_mapping(c: Connection, sid: uuid.UUID, inline: dict[str, str] | None, conns: list[Any]) -> dict[str, str]:
    """Stored mapping (PUT /host-map) + inline `host_map`; every host of the selected connections must be mapped,
    and no mapping target may be on the production deny list (409 HOST_MAP_REQUIRED / PROD_TARGET)."""
    try:
        mapping = {**(setup_mod._get(c, _HOSTMAP_KEY + str(sid)) or {}).get("mappings", {}),
                   **hostmap.normalize(inline)}
    except ValueError as e:
        raise err(400, "BAD_HOST_MAP", str(e)) from None
    unmapped, denied = hostmap.check(conns, mapping, denylist(c))
    if unmapped:
        raise err(409, "HOST_MAP_REQUIRED", "map every production host to its test host before apply: "
                  + ", ".join(unmapped))
    if denied:
        raise err(409, "PROD_TARGET", "mapping targets on the production deny list: " + ", ".join(denied))
    return mapping


@router.get("/import/runs/{run_id}/host-map")
def get_host_map(run_id: str, _: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    """Hosts of the selected connections that need a test-host mapping, with the current mapping and deny flags."""
    sid = _sid(run_id)
    with get_engine().connect() as c:
        r = c.execute(select(import_session).where(import_session.c.id == sid)).first()
        if r is None:
            raise err(404, "NOT_FOUND", "unknown import run")
        plan = _plan_of(r)
        stored = (setup_mod._get(c, _HOSTMAP_KEY + str(sid)) or {}).get("mappings", {})
        dl = denylist(c)
    conns = [x for x in (plan.connections if plan else []) if x.include]
    need = hostmap.required(conns)
    for e in need:
        e["prod_denied"] = dl.match(e["host"]) is not None
        e["test_host"] = stored.get(e["host"])
        e["test_denied"] = bool(e["test_host"]) and dl.match(e["test_host"]) is not None
    return {"hosts": need, "complete": all(e["test_host"] and not e["test_denied"] for e in need)}


@router.put("/import/runs/{run_id}/host-map")
def put_host_map(run_id: str, body: HostMapReq, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    reason = need_reason(body.reason or "map production hosts to test hosts")
    sid = _sid(run_id)
    try:
        m = hostmap.normalize(body.mappings)
    except ValueError as e:
        raise err(400, "BAD_HOST_MAP", str(e)) from None
    with get_engine().begin() as c:
        if c.execute(select(import_session.c.id).where(import_session.c.id == sid)).first() is None:
            raise err(404, "NOT_FOUND", "unknown import run")
        dl = denylist(c)
        bad = sorted(t for t in m.values() if dl.match(t))
        if bad:
            raise err(409, "PROD_TARGET", "test hosts on the production deny list: " + ", ".join(bad))
        cur = (setup_mod._get(c, _HOSTMAP_KEY + str(sid)) or {}).get("mappings", {})
        setup_mod._put(c, _HOSTMAP_KEY + str(sid), {"mappings": {**cur, **m}, "by": p.subject,
                                                    "at": utcnow().isoformat()})
        store.audit(c, p.subject, "import.hostmap", run_id, {"mappings": m, "reason": reason}, p.primary_role)
    return get_host_map(run_id)


def _view_after(c: Connection, sid: uuid.UUID) -> dict[str, Any]:
    r = c.execute(select(import_session.c.status, import_session.c.result, import_session.c.requested_by,
                         import_session.c.applied_revision).where(import_session.c.id == sid)).one()
    return {"id": str(sid), "status": r.status, "result": r.result, "requested_by": r.requested_by,
            "applied_revision": r.applied_revision}


def _enabled_dest(doc: dict[str, Any]) -> list[str]:
    return [str(td.get("name_in_destination") or t) for sd in (doc.get("schemas") or {}).values()
            if sd.get("enabled", True) for t, td in (sd.get("tables") or {}).items() if td.get("enabled", True)]


# ------------------------------------------------------------------ guard (production deny list)
class DenyListUpdate(BaseModel):
    entries: list[str] = Field(default_factory=list, max_length=500)
    reason: str | None = None


def _deny_reader(p: Principal = Depends(current_principal)) -> Principal:
    if not (can(p.roles, "table.configure") or can(p.roles, "users.manage")):
        raise err(403, "FORBIDDEN", "requires table.configure or users.manage")
    return p


@router.get("/guard/denylist")
def get_denylist(_: Principal = Depends(_deny_reader)) -> dict[str, Any]:
    with get_engine().connect() as c:
        mine = list((setup_mod._get(c, _DENY_KEY) or {}).get("entries") or [])
    env = [normalize_entry(e) for e in get_settings().prod_denylist if e.strip()]
    return {"entries": [{"entry": e, "source": "settings"} for e in env] +
                       [{"entry": e, "source": "console"} for e in mine],
            "effective": list(denylist().entries)}


@router.put("/guard/denylist")
def put_denylist(body: DenyListUpdate, p: Principal = Depends(require_perm("users.manage"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    try:
        entries = sorted({normalize_entry(e) for e in body.entries if e.strip()})
    except ValueError as e:
        raise err(400, "BAD_ENTRY", str(e)) from None
    with get_engine().begin() as c:
        setup_mod._put(c, _DENY_KEY, {"entries": entries, "updated_by": p.subject})
        store.audit(c, p.subject, "guard.denylist.update", "prod_denylist",
                    {"reason": reason, "count": len(entries)}, p.primary_role)
    return get_denylist(p)


class GuardCheck(BaseModel):
    targets: list[str] = Field(min_length=1, max_length=50)


@router.post("/guard/check")
def guard_check(body: GuardCheck, _: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    dl = denylist()
    return [{"target": t, "denied_by": dl.match(t)} for t in body.targets]


# ------------------------------------------------------------------ wizard: catalog, tests, defaults
@router.get("/wizard/sources")
def wizard_sources(_: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    out = []
    for eng in sorted(get_settings().sources):
        src = store.source_connector(eng)
        if src is None:
            continue
        out.append({"engine": eng, "display_name": src.display_name or eng, "capture_mode": src.capture_mode,
                    "available": True, "fields": src.connection_fields or [
                        {"name": "host", "label": "Host", "required": True},
                        {"name": "port", "label": "Port", "required": True},
                        {"name": "database", "label": "Database", "required": True},
                        {"name": "user", "label": "User", "required": True},
                        {"name": "password", "label": "Password", "secret": True}]})
    have = {o["engine"] for o in out}
    for eng, name in (("postgres", "PostgreSQL"), ("sqlserver", "SQL Server"), ("mysql", "MySQL"),
                      ("mariadb", "MariaDB"), ("oracle", "Oracle")):
        if eng not in have:
            out.append({"engine": eng, "display_name": name, "capture_mode": "remote", "available": False,
                        "fields": []})
    return out


class SourceTestReq(BaseModel):
    engine: str
    connection_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,62}$")
    environment: Literal["TEST", "PROD"] = "TEST"
    settings: dict[str, Any] = Field(default_factory=dict)
    password: str | None = Field(default=None, repr=False)


def _latest_catalog(c: Connection, cid: str | None) -> Any:
    if not cid:
        return None
    vid = store.latest_catalog_id(c, cid)
    return store.load_catalog(c, vid) if vid else None


def _probes(c: Connection, cid: str | None) -> dict[str, Any]:
    """Latest acked PROBE results of the connection's agent(s)."""
    if not cid:
        return {}
    from .db import agent_command

    out: dict[str, Any] = {}
    rows = c.execute(select(agent_command.c.args, agent_command.c.result)
                     .join(agent, agent.c.id == agent_command.c.agent_id)
                     .where(agent.c.connection_id == cid, agent_command.c.name == "PROBE",
                            agent_command.c.status == "ACKED")
                     .order_by(agent_command.c.acked_at)).all()
    for args, res in rows:
        name = (args or {}).get("name")
        if name and isinstance(res, dict):
            out[name] = res
    return out


@router.post("/wizard/source/test")
def source_test(body: SourceTestReq, _: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    """Guard -> network reachability -> (remote engines) catalog -> prerequisite checklist. Never writes (R1)."""
    steps: list[dict[str, Any]] = []
    try:
        denylist().check_settings(body.settings)
        steps.append({"step": "guard", "ok": True, "detail": "not on the production deny list"})
    except ProdTargetError as e:
        return {"ok": False, "steps": [{"step": "guard", "ok": False, "detail": str(e)}], "checks": [],
                "denied": True}
    if body.environment == "PROD":
        steps.append({"step": "environment", "ok": False,
                      "detail": "parallel tests run against TEST sources only; label PROD is read-only here"})
        return {"ok": False, "steps": steps, "checks": [], "denied": True}
    src = store.source_connector(body.engine)
    if src is None:
        raise err(404, "UNKNOWN_ENGINE", f"no source connector for {body.engine}")
    host, port = body.settings.get("host"), body.settings.get("port")
    if host and port:
        steps.append(wiz.tcp_check(str(host), int(port)))
    with get_engine().connect() as c:
        cat = _latest_catalog(c, body.connection_id)
        probes = _probes(c, body.connection_id)
    if cat is None and src.capture_mode == "remote":
        try:
            schemas = [s.strip() for s in str(body.settings.get("schemas") or "").split(",") if s.strip()]
            cat = src.read_catalog_remote(body.settings, body.password, schemas, body.connection_id or "wizard")
            steps.append({"step": "catalog", "ok": True, "detail": f"{len(cat.tables)} tables"})
        except NotImplementedError:
            pass
        except Exception as e:  # noqa: BLE001
            steps.append({"step": "catalog", "ok": False, "detail": f"{type(e).__name__}"})
    if cat is None and src.capture_mode == "edge":
        steps.append({"step": "catalog", "ok": True, "pending": True,
                      "detail": "read on the source host by the agent after it connects (outbound only)"})
    elif cat is not None and not any(s["step"] == "catalog" for s in steps):
        steps.append({"step": "catalog", "ok": True, "detail": f"{len(cat.tables)} tables"})
    if src.capture_mode == "remote":          # REMOTE.md: a live instance (settings + password) may connect read-only
        live = type(src)(settings=body.settings, password=body.password,  # type: ignore[call-arg]
                         connection_id=body.connection_id or "wizard")
        try:
            checks = live.prerequisite_checks(cat, probes, None, body.settings)
        finally:
            live.close()
    else:
        checks = src.prerequisite_checks(cat, probes, None, body.settings)
    body.password = None
    return {"ok": all(s["ok"] for s in steps), "steps": steps, "checks": checks, "denied": False,
            "catalog_tables": len(cat.tables) if cat else None}


@router.get("/wizard/destinations")
def wizard_destinations(_: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    """Destination catalog from the one registry (alelt_dst_kit.registry): postgres, snowflake, the generic SQL dialects
    (alelt_dst_sql.catalog, one entry per dialect) when installed, then every other registered engine. `available` =
    plugin importable on this hub; `test` = POST /wizard/destination/test supports the engine."""
    from .pipeline import destination_registry

    reg = destination_registry()
    testable = set(DestTestReq.model_fields["engine"].annotation.__args__)  # type: ignore[union-attr]
    first = ["postgres", "snowflake"]
    out: list[dict[str, Any]] = []
    for e in [*first, *(x for x in reg.engines() if x not in first)]:
        if e == "sql":
            try:
                reg.destination_ref("sql")                 # plugin folder on sys.path
                from alelt_dst_sql import catalog

                out += catalog()
            except ImportError:
                pass
            continue
        out.append({"engine": e, "display_name": reg.LABELS.get(e, e), "plugin": reg.destination_ref(e),
                    "available": reg.available(e), "test": e in testable})
    return out


class DestTestReq(BaseModel):
    engine: Literal["postgres", "snowflake", "sql"] = "postgres"
    environment: Literal["TEST", "PROD"] = "TEST"
    settings: dict[str, Any] = Field(default_factory=dict)
    bootstrap: bool = True
    reason: str | None = "parallel test destination"


@router.post("/wizard/destination/test")
def destination_test(body: DestTestReq, p: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    """Test + bootstrap in one click (idempotent). PROD-labelled destinations are read-only: never bootstrapped."""
    try:
        denylist().check_settings(body.settings)
    except ProdTargetError as e:
        return {"ok": False, "steps": [{"step": "guard", "ok": False, "detail": str(e)}], "denied": True}
    boot = body.bootstrap and body.environment == "TEST"
    s = body.settings
    try:
        req = setup_mod.DestinationReq(engine=body.engine, url=s.get("url"), schema=s.get("schema") or "alelt_setup_check",
                                       account=s.get("account"), user=s.get("user"), role=s.get("role"),
                                       warehouse=s.get("warehouse"), database=s.get("database"), bootstrap=boot,
                                       reason=body.reason, dialect=s.get("dialect"), host=s.get("host"),
                                       port=s.get("port"), path=s.get("path"), password=s.get("password"))
    except ValidationError as e:
        raise err(422, "INVALID", "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()))
    out = setup_mod.test_destination(req, p)
    if body.environment == "PROD":
        out["steps"] = [*out.get("steps", []), {"step": "bootstrap", "ok": True,
                                                "detail": "skipped: destination labelled PROD is read-only"}]
    return {**out, "denied": False, "environment": body.environment}


def _churn(c: Connection, cid: str) -> dict[str, float]:
    since = utcnow() - timedelta(days=1)
    rows = c.execute(select(batch_table_stat.c.table_fqn,
                            func.sum(batch_table_stat.c.inserts + batch_table_stat.c.updates + batch_table_stat.c.deletes))
                     .join(source_table, source_table.c.id == batch_table_stat.c.table_id)
                     .where(source_table.c.connection_id == cid, batch_table_stat.c.created_at >= since)
                     .group_by(batch_table_stat.c.table_fqn)).all()
    return {str(f): float(n or 0) for f, n in rows}


def _session_conn(c: Connection, session_id: str | None, legacy_id: str | None) -> tuple[Any, Any, dict[str, Any]]:
    """(session row, planned connection or None, golden) for a wizard that starts from an import."""
    if not session_id:
        return None, None, {}
    r = c.execute(select(import_session).where(import_session.c.id == _sid(session_id))).first()
    if r is None:
        raise err(404, "NOT_FOUND", "unknown import run")
    plan = _plan_of(r)
    pcs = [x for x in (plan.connections if plan else []) if x.supported]
    pc = next((x for x in pcs if x.legacy_id == legacy_id), pcs[0] if pcs else None)
    return r, pc, r.golden or {}


def _imported_tables(pc: Any) -> dict[str, dict[str, Any]]:
    if pc is None:
        return {}
    return {f"{s}.{t}": td for s, sd in (pc.tables.get("schemas") or {}).items()
            for t, td in (sd.get("tables") or {}).items()}


@router.get("/wizard/defaults")
def wizard_defaults(connection_id: str | None = None, session_id: str | None = None, legacy_id: str | None = None,
                    engine: str = "db2", _: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    with get_engine().connect() as c:
        _, pc, gold = _session_conn(c, session_id, legacy_id)
        cat = _latest_catalog(c, connection_id)
        churn = _churn(c, connection_id) if connection_id else {}
        probes = _probes(c, connection_id)
    imported = _imported_tables(pc)
    rows = wiz.table_defaults(cat, imported, churn, gold.get("tables", []))
    lat = wiz.recommend_latency(rows, pc.latency_target_s if pc else None)
    src = store.source_connector(engine)
    suggested = connection_id or (pc.connection_id if pc else None) or "erp"
    name = (pc.tables.get("source_name") if pc else None) or slug(suggested, upper=True)
    schemas = sorted({r["schema"] for r in rows}) or (sorted((pc.tables.get("schemas") or {})) if pc else [])
    return {"connection_id": suggested, "source_name": name, "tables": rows, "latency": lat,
            "output_profile": wiz.recommend_profile(pc.tables.get("output_profile") if pc else None, bool(gold)),
            "schemas": schemas, "catalog_available": cat is not None, "golden_tables": gold.get("tables", []),
            "legacy_schema": pc.legacy_schema if pc else None, "legacy_id": pc.legacy_id if pc else None,
            "schedule": pc.schedule.model_dump() if pc else None,
            "checks": src.prerequisite_checks(cat, probes, [TableRef(schema=r["schema"], name=r["name"])
                                                            for r in rows if r["selected"]] or None,
                                              {"schemas": ",".join(schemas)}) if src else []}


# ------------------------------------------------------------------ wizard: create parallel test + agent package
class PipelineCreate(BaseModel):
    reason: str | None = None
    connection_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    environment: Literal["TEST", "PROD"] = "TEST"
    source_engine: str = "db2"
    source: dict[str, Any] = Field(default_factory=dict)          # host, port, database, user, schemas
    password: SecretInput | None = None
    destination_engine: Literal["postgres", "snowflake"] = "postgres"
    destination: dict[str, Any] = Field(default_factory=dict)
    tables: list[str] | None = None                               # selected "S.T"; None = smart defaults
    latency_target_s: int | None = Field(default=None, ge=10, le=86400)
    output_profile: str | None = None
    session_id: str | None = None
    legacy_id: str | None = None
    agent_kind: Literal["service", "devedge"] = "service"
    hub_url: str | None = None


def _agent_package(c: Connection, p: Principal, cid: str, src: dict[str, Any], schemas: list[str],
                   tables: list[str], hub_url: str, environment: str) -> dict[str, Any]:
    """One-time enrollment token (hash stored) + pre-filled agent configs with the deny list (`deny_hosts`)."""
    token = "alelte_" + pysecrets.token_urlsafe(32)
    agent_id = f"{cid}-agent"
    req = setup_mod.EnrollReq(connection_id=cid, database=str(src.get("database") or cid.upper()),
                              host=src.get("host"), port=int(src["port"]) if str(src.get("port") or "").isdigit() else None,
                              user=src.get("user"), schemas=schemas, agent_id=agent_id, hub_url=hub_url)
    devedge, aleltagent = setup_mod._configs(req, agent_id, hub_url)
    deny = list(denylist(c).entries)
    devedge.update({"deny_hosts": deny, "environment": environment, "start": "current"})
    aleltagent.update({"deny_hosts": deny, "environment": environment,
                    "snapshot": {"allowed_tables": tables, "max_chunk_rows": 5000},
                    "capture": {"autostart": environment == "TEST"}})
    exp = utcnow() + timedelta(hours=72)
    setup_mod._put(c, setup_mod._PREFIX + setup_mod._hash(token), {
        "connection_id": cid, "agent_id": agent_id, "created_at": utcnow().isoformat(),
        "expires_at": exp.isoformat(), "created_by": p.subject, "claimed_by": None, "revoked": False})
    store.audit(c, p.subject, "agent.enroll.create", cid, {"agent_id": agent_id, "expires_at": exp.isoformat(),
                                                           "via": "wizard"}, p.primary_role)
    return {"token": token, "token_hint": token[:8] + "...", "agent_id": agent_id, "expires_at": exp.isoformat(),
            "hub_url": hub_url, "aleltagent_json": aleltagent, "devedge_json": devedge}


@router.post("/wizard/pipelines")
def create_pipeline(body: PipelineCreate, request: Request,
                    p: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    """Create a parallel test: definition + revision + loader config + golden spec + agent package. Every field has
    a default; the legacy pipeline is never touched (active source stays LEGACY)."""
    reason = need_reason(body.reason or "create parallel test")
    if body.environment != "TEST":
        raise err(409, "PROD_READ_ONLY", "the parallel-test wizard only creates TEST pipelines")
    with get_engine().begin() as c:
        _guard(body.source, "source", c)
        _guard(body.destination, "destination", c)
        sess, pc, gold = _session_conn(c, body.session_id, body.legacy_id)
        cat = _latest_catalog(c, body.connection_id)
        imported = _imported_tables(pc)
        rows = wiz.table_defaults(cat, imported, _churn(c, body.connection_id), gold.get("tables", []))
        selected = body.tables if body.tables is not None else [r["fqn"] for r in rows if r["selected"]]
        if not selected and not rows:
            schemas_in = [s.strip().upper() for s in str(body.source.get("schemas") or "").split(",") if s.strip()]
            selected = []          # catalog not yet known: every table of the schemas is captured (policy default)
        else:
            schemas_in = sorted({f.partition(".")[0] for f in selected})
        lat = body.latency_target_s or wiz.recommend_latency(rows, pc.latency_target_s if pc else None)[
            "latency_target_s"]
        profile = body.output_profile or wiz.recommend_profile(
            pc.tables.get("output_profile") if pc else None, bool(gold))
        source_name = (pc.tables.get("source_name") if pc else None) or slug(body.connection_id, upper=True)
        doc = wiz.tables_doc(body.connection_id, body.source_engine, body.destination_engine, source_name, selected,
                             cat, profile, imported, pc.legacy_schema if pc else None,
                             body.destination.get("schema"))
        doc["initial_load"] = "auto"          # worker starts alelt_snapshot once the catalog is known (REQ-14)
        refs: dict[str, str] = {}
        if body.password and (body.password.value or body.password.ref):
            refs = _secret_values(SecretStore(), {f"src_{body.connection_id}_password": body.password},
                                  {f"src_{body.connection_id}_password"})
        src_doc = {"engine": body.source_engine, "settings": {k: v for k, v in body.source.items()
                                                              if k != "password"},
                   "secret_refs": {"password": v for v in refs.values()}}
        dst_doc = {"engine": body.destination_engine,
                   "settings": {k: v for k, v in body.destination.items() if not k.lower().endswith(("password", "key"))}}
        rev = _new_revision(c, p.subject, reason, "console", {"wizard": "parallel_test",
                                                               "connection_id": body.connection_id,
                                                               "session": body.session_id})
        res = _materialize(c, p, rev, "wizard" if not sess else f"wizard+import:{sess.id}", body.environment, {
            "connection_id": body.connection_id, "legacy_id": pc.legacy_id if pc else None,
            "source_engine": body.source_engine, "destination_engine": body.destination_engine, "source": src_doc,
            "destination": dst_doc, "tables": doc, "latency_target_s": lat,
            "schedule": pc.schedule.model_dump() if pc else None}, "SHADOW")
        if gold:
            _store_golden(c, body.connection_id, pc.legacy_schema if pc else None, gold, "files")
        _seal_revision(c, rev)
        hub_url = (body.hub_url or str(request.base_url)).rstrip("/")
        pkg = _agent_package(c, p, body.connection_id, body.source, schemas_in, selected, hub_url, body.environment)
        store.audit(c, p.subject, "wizard.pipeline.create", body.connection_id,
                    {"revision": rev, "reason": reason, "tables": len(selected), "environment": body.environment,
                     "session": body.session_id}, p.primary_role)
    return {**res, "revision": rev, "tables": selected, "latency_target_s": lat, "output_profile": profile,
            "agent": pkg, "kind": body.agent_kind,
            "contract": golden_mod.contract_check(gold.get("tables", []), _enabled_dest(doc)) if gold else None}


class AgentPkgReq(BaseModel):
    reason: str | None = "agent package"
    hub_url: str | None = None


@router.post("/wizard/pipelines/{cid}/agent")
def agent_package(cid: str, body: AgentPkgReq, request: Request,
                  p: Principal = Depends(require_perm("table.configure"))) -> dict[str, Any]:
    """Fresh token + configs for an existing pipeline (e.g. the first token expired or was lost)."""
    need_reason(body.reason)
    with get_engine().begin() as c:
        d = c.execute(select(pipeline_definition).where(pipeline_definition.c.connection_id == cid)).first()
        if d is None:
            raise err(404, "NOT_FOUND", "unknown pipeline")
        if d.environment != "TEST":
            raise err(409, "PROD_READ_ONLY", "agent packages are generated for TEST pipelines only")
        src = (d.source or {}).get("settings") or {}
        _guard(src, "source", c)
        tables = _enabled_fqns(d.tables_doc)
        return _agent_package(c, p, cid, src, sorted({t.partition(".")[0] for t in tables}), tables,
                              (body.hub_url or str(request.base_url)).rstrip("/"), d.environment)


def _enabled_fqns(doc: dict[str, Any]) -> list[str]:
    return [f"{s}.{t}" for s, sd in (doc.get("schemas") or {}).items() if sd.get("enabled", True)
            for t, td in (sd.get("tables") or {}).items() if td.get("enabled", True)]


# ------------------------------------------------------------------ pipelines: list, config, progress
def _pipeline(r: Any) -> dict[str, Any]:
    src = (r.source or {}).get("settings") or {}
    return {"connection_id": r.connection_id, "environment": r.environment, "origin": r.origin,
            "status": r.status, "active_source": r.active_source, "source_engine": r.source_engine,
            "destination_engine": r.destination_engine, "latency_target_s": r.latency_target_s,
            "revision": r.revision, "legacy_ref": r.legacy_ref, "tables": len(_enabled_fqns(r.tables_doc or {})),
            "source_host": src.get("host"), "source_database": src.get("database"),
            "created_at": _iso(r.created_at), "created_by": r.created_by}


@router.get("/pipelines")
def list_pipelines(_: Principal = Depends(require_perm("view.dashboard"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return [_pipeline(r) for r in c.execute(select(pipeline_definition)
                                                .order_by(pipeline_definition.c.created_at.desc()))]


@router.get("/pipelines/{cid}/tables.yaml", response_class=PlainTextResponse)
def pipeline_yaml(cid: str, _: Principal = Depends(require_perm("table.configure"))) -> PlainTextResponse:
    import yaml

    with get_engine().connect() as c:
        doc = c.execute(select(pipeline_definition.c.tables_doc)
                        .where(pipeline_definition.c.connection_id == cid)).scalar()
    if doc is None:
        raise err(404, "NOT_FOUND", "unknown pipeline")
    return PlainTextResponse(yaml.safe_dump(doc, sort_keys=True, allow_unicode=True), media_type="text/yaml")


def _step(sid: str, status: str, detail: str = "", **kw: Any) -> dict[str, Any]:
    return {"id": sid, "status": status, "detail": detail, **kw}


def _progress_checks(src: Any, d: Any, cat: Any, probes: dict[str, Any], tables: list[TableRef] | None,
                     c: Connection | None = None) -> list[dict[str, Any]]:
    """Prerequisite checks for the progress screen. Remote engines get a LIVE instance built with the pipeline's
    settings + its stored secret (REMOTE.md constructor rule), as /wizard/source/test does; the password is resolved
    from the secret reference only for this call and never returned."""
    settings = (d.source or {}).get("settings") or {}
    if getattr(src, "capture_mode", "edge") != "remote":
        return src.prerequisite_checks(cat, probes, tables, settings)  # type: ignore[no-any-return]
    ref = ((d.source or {}).get("secret_refs") or {}).get("password")
    password = None
    if ref:
        try:
            password = SecretStore().resolve(ref)
        except Exception as e:  # noqa: BLE001 - missing secret: say so instead of a misleading connect failure
            return [{"id": "secret", "label": "Stored source password", "status": "FAIL",
                     "detail": f"secret reference {ref} cannot be resolved ({type(e).__name__})", "fix_sql": None}]
    try:
        denylist(c).check_settings(settings)
    except ProdTargetError as e:
        return [{"id": "guard", "label": "Production deny list", "status": "FAIL", "detail": str(e), "fix_sql": None}]
    live = type(src)(settings=settings, password=password, connection_id=d.connection_id)  # type: ignore[call-arg]
    try:
        return live.prerequisite_checks(cat, probes, tables, settings)  # type: ignore[no-any-return]
    finally:
        live.close()


@router.get("/wizard/pipelines/{cid}/progress")
def progress(cid: str, _: Principal = Depends(require_perm("view.dashboard"))) -> dict[str, Any]:
    """Live progress of a parallel test: agent -> catalog/prerequisites -> initial load -> CDC -> contract -> parity."""
    with get_engine().connect() as c:
        d = c.execute(select(pipeline_definition).where(pipeline_definition.c.connection_id == cid)).first()
        if d is None:
            raise err(404, "NOT_FOUND", "unknown pipeline")
        steps: list[dict[str, Any]] = []
        agents = c.execute(select(agent.c.id, agent.c.last_heartbeat, agent.c.status)
                           .where(agent.c.connection_id == cid)).all()
        online = [a for a in agents if (seconds_since(a.last_heartbeat) or 1e9) <= _ONLINE_S]
        steps.append(_step("agent", "DONE" if online else ("FAIL" if agents else "PENDING"),
                           online[0].id if online else (f"{agents[0].id}: offline" if agents else "waiting for the agent"),
                           ))
        cat = _latest_catalog(c, cid)
        src = store.source_connector(d.source_engine)
        enabled = _enabled_fqns(d.tables_doc or {})
        checks = _progress_checks(src, d, cat, _probes(c, cid), [TableRef(schema=f.partition(".")[0],
                                                                         name=f.partition(".")[2]) for f in enabled]
                                  or None, c) if src else []
        failing = [x for x in checks if x["status"] == "FAIL"]
        steps.append(_step("prerequisites", "PENDING" if cat is None else ("FAIL" if failing else "DONE"),
                           f"{len(cat.tables)} tables in catalog" if cat else "waiting for the catalog",
                           checks=checks))
        chunks = c.execute(select(snapshot_chunk.c.table_fqn, func.sum(snapshot_chunk.c.rows),
                                  func.max(snapshot_chunk.c.chunk_no))
                           .where(snapshot_chunk.c.connection_id == cid).group_by(snapshot_chunk.c.table_fqn)).all()
        done_tabs = {str(f) for f, in c.execute(select(snapshot_chunk.c.table_fqn).where(
            snapshot_chunk.c.connection_id == cid, snapshot_chunk.c.last.is_(True)))}
        per_table = {f: {"fqn": f, "snapshot_rows": 0, "snapshot_done": False, "parity": None} for f in enabled}
        for f, rows_, _n in chunks:
            per_table.setdefault(str(f), {"fqn": str(f), "snapshot_rows": 0, "snapshot_done": False, "parity": None})
            per_table[str(f)].update(snapshot_rows=int(rows_ or 0), snapshot_done=str(f) in done_tabs)
        from .db import table_hold

        for f, st_, body in c.execute(select(table_hold.c.table_fqn, table_hold.c.state, table_hold.c.body).where(
                table_hold.c.connection_id == cid, table_hold.c.kind == "snapshot")):
            t_ = per_table.setdefault(str(f), {"fqn": str(f), "snapshot_rows": 0, "snapshot_done": False,
                                               "parity": None})
            t_["snapshot_state"] = st_                       # alelt_snapshot state (orchestrated initial load)
            t_["snapshot_rows"] = max(int(t_.get("snapshot_rows") or 0), int((body or {}).get("rows_loaded") or 0))
            if st_ in ("CATCHING_UP", "IN_SYNC"):
                t_["snapshot_done"] = True
            if (body or {}).get("last_error"):
                t_["snapshot_error"] = body["last_error"]
        n_done = sum(1 for t in per_table.values() if t["snapshot_done"])
        started = bool(chunks) or any(t.get("snapshot_state") for t in per_table.values())
        steps.append(_step("initial_load", "DONE" if per_table and n_done == len(per_table) else
                           ("FAIL" if any(t.get("snapshot_error") for t in per_table.values()) else
                            "RUNNING" if started else "PENDING"), f"{n_done}/{len(per_table)} tables"))
        loaded, last = c.execute(select(func.count(), func.max(batch.c.received_at))
                                 .where(batch.c.connection_id == cid, batch.c.status == "LOADED")).one()
        failed = c.execute(select(func.count()).where(batch.c.connection_id == cid,
                                                      batch.c.status == "FAILED")).scalar() or 0
        steps.append(_step("cdc", "FAIL" if failed else ("RUNNING" if loaded else "PENDING"),
                           f"{loaded} batches loaded" + (f", {failed} failed" if failed else ""),
                           last_batch_at=_iso(last)))
        g = c.execute(select(golden_capture.c.body).where(golden_capture.c.connection_id == cid)
                      .order_by(golden_capture.c.captured_at.desc()).limit(1)).scalar()
        dest_names = _enabled_dest(d.tables_doc or {})
        if g:
            cc = golden_mod.contract_check(g.get("tables", []), dest_names)
            steps.append(_step("contract", "FAIL" if cc["missing"] else "DONE",
                               f"{len(cc['covered'])}/{len(dest_names)} tables covered by the golden spec", **cc))
        else:
            steps.append(_step("contract", "PENDING", "no golden spec yet (drop the landing DDL in the import)"))
        rec = c.execute(select(source_table.c.src_schema, source_table.c.src_table, recon_result.c.outcome,
                               recon_result.c.created_at)
                        .join(recon_result, recon_result.c.table_id == source_table.c.id)
                        .where(source_table.c.connection_id == cid)
                        .order_by(recon_result.c.created_at)).all()
        for s, t, outcome, _at in rec:
            per_table.setdefault(f"{s}.{t}", {"fqn": f"{s}.{t}", "snapshot_rows": 0, "snapshot_done": False})
            per_table[f"{s}.{t}"]["parity"] = outcome
        par = [t["parity"] for t in per_table.values() if t.get("parity")]
        steps.append(_step("parity", "PENDING" if not par else ("DONE" if all(x == "MATCH" for x in par) else "FAIL"),
                           f"{sum(1 for x in par if x == 'MATCH')}/{len(per_table)} tables match"))
    return {"connection_id": cid, "environment": d.environment, "status": d.status, "active_source": d.active_source,
            "steps": steps, "tables": sorted(per_table.values(), key=lambda t: t["fqn"])}


@router.get("/import/runs/{run_id}/golden/columns.csv", response_class=PlainTextResponse)
def golden_csv(run_id: str, _: Principal = Depends(require_perm("import.run"))) -> PlainTextResponse:
    with get_engine().connect() as c:
        g = c.execute(select(import_session.c.golden).where(import_session.c.id == _sid(run_id))).scalar()
    if not g:
        raise err(404, "NOT_FOUND", "no golden spec in this import")
    return PlainTextResponse(g.get("columns_csv", ""), media_type="text/csv")


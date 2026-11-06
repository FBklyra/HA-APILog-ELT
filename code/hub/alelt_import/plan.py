"""Inventory -> MigrationPlan (vendor-neutral mapper), user edits, and diff against the current configuration."""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from alelt_common.util import utcnow

from .models import (
    MASK,
    InvConnection,
    InvDestination,
    InventoryModel,
    InvTable,
    MigrationPlan,
    PlanEdits,
    PlannedConnection,
    PlannedRole,
    PlannedSecret,
    PlannedTrigger,
    Warning_,
)
from .reports import diff as tree_diff

DEFAULT_SOURCES: Mapping[str, str] = {"db2": "edge"}          # engine -> capture mode (registered connectors)
PLANNED_SOURCES = frozenset({"postgres", "sqlserver", "mysql", "mariadb", "oracle"})
DESTINATIONS = frozenset({"snowflake", "postgres"})
DEFAULT_LATENCY_S = 120
# X-127: HISTORY (type-2 history tables) is out of scope for 0.1 - such tables are flagged (blocker) and left disabled
SYNC_MODES = {"SOFT_DELETE": "SOFT_DELETE"}
UNSUPPORTED_MODES = frozenset({"HISTORY"})
_ROLE_KEYWORDS: tuple[tuple[tuple[str, ...], list[str]], ...] = (
    (("account admin", "owner"), ["PlatformAdmin", "DataOperator"]),
    (("billing",), []),
    (("audit", "compliance"), ["Auditor"]),
    (("admin", "manager", "collaborator", "editor", "operator", "developer"), ["DataOperator"]),
    (("analyst", "reviewer", "read", "view", "member"), ["Viewer"]),
)


def slug(text: str, upper: bool = False, max_len: int = 63) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "x"
    if not s[0].isalpha():
        s = "c_" + s
    s = s[:max_len].rstrip("_")
    return s.upper() if upper else s


def _unique(base: str, taken: set[str]) -> str:
    out, n = base, 2
    while out in taken:
        out = f"{base[:60]}_{n}"
        n += 1
    taken.add(out)
    return out


def legacy_roles(role: str | None, role_map: Mapping[str, list[str]]) -> list[str] | None:
    """ALELT roles for a legacy role name; None if unknown."""
    if not role:
        return None
    if role in role_map:
        return list(role_map[role])
    low = role.lower()
    for words, roles in _ROLE_KEYWORDS:
        if any(w in low for w in words):
            return list(roles)
    return None


def event_name(e: str) -> str | None:
    low = e.lower()
    if "fail" in low or "error" in low:
        return "pipeline_error"
    if "start" in low:
        return "load_started"
    if any(w in low for w in ("end", "complete", "success", "succeed", "finish")):
        return "load_completed"
    return None


def _latency(freq_min: int | None) -> int:
    return DEFAULT_LATENCY_S if not freq_min else max(60, min(int(freq_min) * 60, 3600))


def _window(daily: str | None, tz: str | None) -> dict[str, str] | None:
    m = re.match(r"^(\d{1,2}):(\d{2})$", daily or "")
    if not m:
        return None
    h, mi = int(m.group(1)) % 24, int(m.group(2))
    return {"start": f"{h:02d}:{mi:02d}", "end": f"{(h + 4) % 24:02d}:{mi:02d}", "tz": tz or "UTC"}


def _table_doc(t: InvTable, warn: list[Warning_], ref: str) -> dict[str, Any]:
    mode = t.sync_mode.upper()
    if mode in UNSUPPORTED_MODES:
        warn.append(Warning_(code="HISTORY_MODE_UNSUPPORTED", severity="blocker", ref=ref, message=(
            f"{t.name}: sync mode {mode} is not supported in this release (X-127); the table is left DISABLED - "
            "enable it explicitly as SOFT_DELETE or keep it on the legacy pipeline")))
    elif mode not in SYNC_MODES:
        warn.append(Warning_(code="SYNC_MODE_UNSUPPORTED", ref=ref,
                             message=f"sync mode {mode} has no equivalent; SOFT_DELETE is used"))
    cols: dict[str, Any] = {}
    for c in sorted(t.columns, key=lambda c: c.name):
        if not c.enabled or c.hashed or c.is_primary_key:
            cols[c.name] = {"enabled": c.enabled or c.is_primary_key, "hashed": c.hashed and not c.is_primary_key,
                            "is_primary_key": c.is_primary_key}
    return {"enabled": t.enabled and mode not in UNSUPPORTED_MODES,
            "name_in_destination": (t.name_in_destination or t.name).upper(),
            "sync_mode": SYNC_MODES.get(mode, "SOFT_DELETE"), "columns": cols,
            **({"legacy_sync_mode": mode} if mode in UNSUPPORTED_MODES else {})}


def _connection(conn: InvConnection, dest: InvDestination | None, cid: str, sources: Mapping[str, str],
                profile: str | None, warn: list[Warning_],
                secrets: dict[str, PlannedSecret]) -> PlannedConnection:
    ref = f"connection:{conn.id}"
    engine = conn.engine
    supported = engine is not None and engine in sources
    if not supported:
        planned = engine in PLANNED_SOURCES
        warn.append(Warning_(code="SOURCE_PLANNED" if planned else "SOURCE_UNSUPPORTED", ref=ref,
                             severity="warning",
                             message=(f"{conn.name}: source type '{conn.service}' is not supported yet "
                                      f"({'planned' if planned else 'no ALELT engine'}); left out of the plan")))
    dest_engine = dest.engine if dest else None
    if dest is not None and dest_engine not in DESTINATIONS:
        warn.append(Warning_(code="DESTINATION_UNSUPPORTED", ref=ref,
                             message=f"{conn.name}: destination type '{dest.service}' has no ALELT engine"))
        supported = False
    mode = sources.get(engine or "")
    schemas: dict[str, Any] = {}
    hashed = keyless = history = 0
    for s in sorted(conn.schemas, key=lambda s: s.name):
        tables: dict[str, Any] = {}
        for t in sorted(s.tables, key=lambda t: t.name):
            tref = f"table:{conn.id}/{s.name}.{t.name}"
            doc = _table_doc(t, warn, tref)
            tables[t.name] = doc
            if t.enabled and s.enabled:
                hashed += sum(1 for c in t.columns if c.hashed and c.enabled)
                history += doc.get("legacy_sync_mode") == "HISTORY"
                keyless += bool(t.columns) and not any(c.is_primary_key for c in t.columns)
        schemas[s.name] = {"enabled": s.enabled, "name_in_destination": (s.name_in_destination or s.name).upper(),
                           "tables": tables}
    if hashed:
        warn.append(Warning_(code="HASHED_COLUMNS", ref=ref, message=(
            f"{conn.name}: {hashed} hashed column(s); hash values must match the legacy output bit-for-bit "
            "(verify during the parallel run)")))
    if history:
        warn.append(Warning_(code="HISTORY_MODE", severity="blocker", ref=ref, message=(
            f"{conn.name}: {history} table(s) use HISTORY sync mode, which is out of scope for this release (X-127); "
            "they were imported DISABLED and are not replicated")))
    if keyless:
        warn.append(Warning_(code="KEYLESS_TABLES", severity="info", ref=ref, message=(
            f"{conn.name}: {keyless} table(s) without primary key; the output profile surrogate id column is used")))
    if conn.schedule.paused:
        warn.append(Warning_(code="LEGACY_PAUSED", severity="info", ref=ref,
                             message=f"{conn.name}: paused in the legacy platform; created paused"))
    if (conn.schedule.schedule_type or "").lower() == "manual":
        warn.append(Warning_(code="MANUAL_SCHEDULE", severity="info", ref=ref,
                             message=f"{conn.name}: manual schedule in the legacy platform; ALELT streams continuously"))

    enabled_schemas = [x for x in schemas.values() if x["enabled"]]
    prefix = (conn.destination_schema or "").upper()
    legacy = (f"{prefix}_{enabled_schemas[0]['name_in_destination']}" if prefix and len(enabled_schemas) == 1
              else (prefix or None))
    source_name = slug(prefix or conn.name, upper=True)
    if profile:
        out_profile = profile
    else:
        out_profile = "golden_spec" if legacy else "alelt"
    tables_doc: dict[str, Any] = {
        "connection_id": cid, "source_engine": engine or conn.service, "destination_engine": dest_engine or "snowflake",
        "output_profile": out_profile, "schema_change_handling": conn.schema_change_policy.upper()
        if conn.schema_change_policy.upper() in ("ALLOW_ALL", "ALLOW_COLUMNS", "BLOCK_ALL") else "ALLOW_COLUMNS",
        "source_name": source_name, "legacy_schema": legacy, "schemas": schemas}
    win = _window(conn.schedule.daily_time, conn.schedule.timezone)
    if win:
        tables_doc["snapshot_window"] = win

    src_settings = dict(conn.settings)
    src_secret_refs: dict[str, str] = {}
    ident = "_".join(str(conn.settings.get(k, "")) for k in ("host", "port", "database", "user"))
    for f in conn.secret_fields:
        key = slug(f"src_{engine or conn.service}_{ident}_{f}", max_len=120)
        ps = secrets.setdefault(key, PlannedSecret(
            key=key, label=f"{conn.name}: {f}", owner=f"connection:{cid}", field=f,
            where="agent" if mode == "edge" else "hub"))
        ps.used_by.append(cid)
        src_secret_refs[f] = key
    dst_settings: dict[str, Any] = dict(dest.settings) if dest else {}
    dst_secret_refs: dict[str, str] = {}
    if dest:
        for f in dest.secret_fields:
            key = slug(f"dst_{dest.id}_{f}", max_len=120)
            ps = secrets.setdefault(key, PlannedSecret(key=key, label=f"{dest.name}: {f}",
                                                       owner=f"destination:{dest.id}", field=f))
            ps.used_by.append(cid)
            dst_secret_refs[f] = key
    total = sum(len(s["tables"]) for s in schemas.values())
    enabled = sum(1 for s in schemas.values() if s["enabled"] for t in s["tables"].values() if t["enabled"])
    return PlannedConnection(
        legacy_id=conn.id, legacy_name=conn.name, connection_id=cid, include=supported, supported=supported,
        source_engine=engine, destination_engine=dest_engine, capture_mode=mode,
        source={"engine": engine, "service": conn.service, "settings": src_settings, "secrets": src_secret_refs},
        destination={"engine": dest_engine, "legacy_id": dest.id if dest else None,
                     "name": dest.name if dest else None, "settings": dst_settings, "secrets": dst_secret_refs},
        tables=tables_doc, latency_target_s=_latency(conn.schedule.frequency_minutes), schedule=conn.schedule,
        paused=conn.schedule.paused, legacy_schema=legacy, enabled_tables=enabled, total_tables=total)


def build_plan(inv: InventoryModel, sources: Mapping[str, str] | None = None, output_profile: str | None = None,
               role_map: Mapping[str, list[str]] | None = None, taken_ids: set[str] | None = None) -> MigrationPlan:
    """Deterministic mapping. `sources` = registered source engines -> capture mode."""
    srcs = DEFAULT_SOURCES if sources is None else sources
    warn: list[Warning_] = list(inv.warnings)
    secrets: dict[str, PlannedSecret] = {}
    dests = {d.id: d for d in inv.destinations}
    taken = set(taken_ids or ())
    ids: dict[str, str] = {}
    conns: list[PlannedConnection] = []
    for c in sorted(inv.connections, key=lambda c: (c.name.lower(), c.id)):
        cid = _unique(slug(c.name), taken)
        ids[c.id] = cid
        dest = dests.get(c.destination_id or "") or (dests.get(c.group_id or "") if c.group_id else None)
        conns.append(_connection(c, dest, cid, srcs, output_profile, warn, secrets))

    triggers: list[PlannedTrigger] = []
    for t in sorted(inv.triggers, key=lambda t: (t.kind, t.name, t.id)):
        ref = f"trigger:{t.id}"
        after = [ids[x] for x in t.connection_ids if x in ids]
        events = []
        for e in t.events:
            n = event_name(e)
            if n is None:
                warn.append(Warning_(code="EVENT_UNMAPPED", severity="info", ref=ref,
                                     message=f"{t.name}: event '{e}' has no ALELT equivalent"))
            elif n not in events:
                events.append(n)
        if t.kind == "dbt_job":
            triggers.append(PlannedTrigger(legacy_id=t.id, kind="dbt_job", name=t.name, include=t.enabled,
                                           target=t.target, events=events or ["load_completed"],
                                           after_connections=after,
                                           settings={**t.settings, **({"schedule": t.schedule} if t.schedule else {})}))
        elif t.kind == "webhook" or (t.kind == "notification" and (t.target or "").startswith("http")):
            triggers.append(PlannedTrigger(legacy_id=t.id, kind="webhook", name=t.name, include=t.enabled,
                                           target=t.target, events=events or ["pipeline_error"],
                                           after_connections=after, settings=t.settings))
            if t.settings.get("secret") == MASK:
                key = slug(f"hook_{t.id}_secret", max_len=120)
                secrets.setdefault(key, PlannedSecret(key=key, label=f"{t.name}: signing secret",
                                                      owner=f"trigger:{t.id}", field="secret"))
        else:
            warn.append(Warning_(code="TRIGGER_UNSUPPORTED", ref=ref, message=(
                f"{t.name}: {t.kind} is not migrated automatically (use alert routing or a dbt job trigger)")))

    roles: list[PlannedRole] = []
    for u in sorted(inv.users, key=lambda u: u.email.lower()):
        if not u.active:
            continue
        mapped = legacy_roles(u.role, role_map or {})
        if mapped is None:
            warn.append(Warning_(code="ROLE_UNMAPPED", severity="info", ref=f"user:{u.email}",
                                 message=f"{u.email}: role '{u.role}' unknown; Viewer proposed"))
            mapped = ["Viewer"]
        if not mapped:
            continue
        roles.append(PlannedRole(email=u.email, name=u.name, roles=mapped, legacy_role=u.role,
                                 include="PlatformAdmin" not in mapped))
    plan = MigrationPlan(plugin=inv.plugin, platform=inv.platform, generated_at=utcnow(), connections=conns,
                         triggers=triggers, roles=roles, secrets=sorted(secrets.values(), key=lambda s: s.key),
                         warnings=warn)
    return summarize(plan)


def summarize(plan: MigrationPlan) -> MigrationPlan:
    inc = [c for c in plan.connections if c.include]
    plan.summary = {
        "connections": len(plan.connections), "connections_included": len(inc),
        "tables_enabled": sum(c.enabled_tables for c in inc), "tables_total": sum(c.total_tables for c in plan.connections),
        "triggers": sum(1 for t in plan.triggers if t.include), "roles": sum(1 for r in plan.roles if r.include),
        "secrets_hub": sum(1 for s in plan.secrets if s.where == "hub"),
        "secrets_agent": sum(1 for s in plan.secrets if s.where == "agent"),
        "warnings": sum(1 for w in plan.warnings if w.severity != "info"),
        "blockers": sum(1 for w in plan.warnings if w.severity == "blocker"),
    }
    return plan


class EditError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def apply_edits(plan: MigrationPlan, edits: PlanEdits) -> MigrationPlan:
    """Return a new plan with the review-tree toggles applied. Key columns cannot be disabled or hashed."""
    p = plan.model_copy(deep=True)
    by_legacy = {c.legacy_id: c for c in p.connections}
    renames: dict[str, str] = {}
    for lid, e in edits.connections.items():
        c = by_legacy.get(lid)
        if c is None:
            raise EditError("NOT_FOUND", f"unknown connection {lid}")
        if e.include is not None:
            if e.include and not c.supported:
                raise EditError("UNSUPPORTED", f"{c.legacy_name} cannot be included: no ALELT engine")
            c.include = e.include
        if e.latency_target_s is not None:
            c.latency_target_s = e.latency_target_s
        if e.output_profile:
            c.tables["output_profile"] = e.output_profile
        if e.connection_id and e.connection_id != c.connection_id:
            if any(o.connection_id == e.connection_id for o in p.connections):
                raise EditError("DUPLICATE_ID", f"connection id {e.connection_id} already used")
            renames[c.connection_id] = e.connection_id
            c.connection_id = c.tables["connection_id"] = e.connection_id
        for fqn, te in e.tables.items():
            s, _, t = fqn.partition(".")
            td = ((c.tables.get("schemas") or {}).get(s) or {}).get("tables", {}).get(t)
            if td is None:
                raise EditError("NOT_FOUND", f"unknown table {fqn} in {c.legacy_name}")
            if te.enabled is not None:
                td["enabled"] = te.enabled
            for col, ce in te.columns.items():
                cur = td["columns"].get(col, {"enabled": True, "hashed": False, "is_primary_key": False})
                if cur.get("is_primary_key") and (ce.enabled is False or ce.hashed):
                    raise EditError("KEY_COLUMN", f"key column {fqn}.{col} cannot be disabled or hashed")
                if ce.enabled is not None:
                    cur["enabled"] = ce.enabled
                if ce.hashed is not None:
                    cur["hashed"] = ce.hashed
                td["columns"][col] = cur
        c.enabled_tables = sum(1 for sd in c.tables["schemas"].values() if sd["enabled"]
                               for td in sd["tables"].values() if td["enabled"])
    if renames:
        for t in p.triggers:
            t.after_connections = [renames.get(x, x) for x in t.after_connections]
        for sec in p.secrets:
            sec.used_by = [renames.get(x, x) for x in sec.used_by]
            kind, _, ref = sec.owner.partition(":")
            if kind == "connection" and ref in renames:
                sec.owner = f"connection:{renames[ref]}"
    for t in p.triggers:
        if t.legacy_id in edits.triggers:
            t.include = edits.triggers[t.legacy_id]
    for r in p.roles:
        if r.email in edits.roles:
            r.include = edits.roles[r.email]
    return summarize(p)


def plan_diff(plan: MigrationPlan, current: Mapping[str, Mapping[str, Any]],
              current_triggers: Mapping[str, Mapping[str, Any]] | None = None,
              current_roles: Mapping[str, list[str]] | None = None) -> list[dict[str, Any]]:
    """DiffEntry list ({path, op, before, after}) of what applying `plan` changes in the current configuration.
    New connections are one `add` entry; existing ones get a path-level diff of their tables.yaml document."""
    out: list[dict[str, Any]] = []
    for c in plan.connections:
        if not c.include:
            continue
        cur = current.get(c.connection_id)
        target = {"tables": c.tables, "latency_target_s": c.latency_target_s}
        if cur is None:
            out.append({"path": c.connection_id, "op": "add", "before": None,
                        "after": {"tables": c.enabled_tables, "source_engine": c.source_engine,
                                  "latency_target_s": c.latency_target_s}})
            continue
        for path, old, new in tree_diff(dict(cur), target):
            op = "add" if old is None else "remove" if new is None else "change"
            out.append({"path": f"{c.connection_id}.{path}", "op": op, "before": old, "after": new})
    for t in plan.triggers:
        if t.include:
            key = f"triggers.{t.kind}.{t.name}"
            have = (current_triggers or {}).get(key)
            new_t = {"target": t.target, "events": t.events, "after": t.after_connections}
            if have is None:
                out.append({"path": key, "op": "add", "before": None, "after": new_t})
            elif dict(have) != new_t:
                out.append({"path": key, "op": "change", "before": dict(have), "after": new_t})
    for r in plan.roles:
        if r.include:
            have_r = sorted((current_roles or {}).get(r.email.lower(), []))
            want = sorted(set(have_r) | set(r.roles))
            if want != have_r:
                out.append({"path": f"roles.{r.email}", "op": "add" if not have_r else "change",
                            "before": have_r or None, "after": want})
    return out


def changes_existing(diff: list[dict[str, Any]], existing: set[str]) -> bool:
    """True if a diff modifies something already in force (=> two-person rule)."""
    for d in diff:
        head = d["path"].split(".", 1)[0]
        if d["op"] in ("change", "remove") or (head in existing and "." in d["path"]):
            return True
    return False

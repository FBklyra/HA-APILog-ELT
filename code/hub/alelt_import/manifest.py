"""`yaml_manifest`: generic file-based import plugin for any legacy tool that can export (or be described as) YAML/JSON.

Format (all keys optional except connections[].id/name and engine|service):

    platform: Legacy ETL
    destinations: [{id, name, engine|service, settings: {...}}]
    connections:
      - id: c1
        name: ERP
        engine: db2                      # or service: <legacy type>, mapped by prefix
        destination: wh
        destination_schema: LEGACY_X    # existing landing schema prefix
        schedule: {frequency_minutes: 15, daily_time: "02:00", timezone: America/Toronto, paused: false}
        schema_change_policy: ALLOW_COLUMNS
        settings: {host, port, database, user, password}     # secret values are masked on read
        schemas:
          <SCHEMA>:
            name_in_destination: <SCHEMA>
            tables:
              <TABLE>: {enabled: true, sync_mode: SOFT_DELETE,
                        columns: {<KEY>: {primary_key: true}, <COLUMN>: {enabled: false, hashed: false}}}
    triggers: [{id, kind: webhook|dbt_job|notification|transformation, name, target, events: [], after: [c1]}]
    users: [{email, name, role}]
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from alelt_common.util import utcnow

from .models import (
    CredentialField,
    InvColumn,
    InvConnection,
    InvDestination,
    InventoryModel,
    InvSchedule,
    InvSchema,
    InvTable,
    InvTrigger,
    InvUser,
)
from .files import Bundle
from .plugin import DiscoveryError, ImportPlugin
from .secrets import mask

_PREFIXES = (("db2", "db2"), ("postgres", "postgres"), ("pg", "postgres"), ("sql_server", "sqlserver"),
             ("sqlserver", "sqlserver"), ("mssql", "sqlserver"), ("mysql", "mysql"), ("mariadb", "mariadb"),
             ("oracle", "oracle"), ("snowflake", "snowflake"))


def guess_engine(service: str | None) -> str | None:
    s = (service or "").lower()
    return next((e for p, e in _PREFIXES if s.startswith(p)), None)


def _table(name: str, t: Mapping[str, Any]) -> InvTable:
    cols = [InvColumn(name=c, name_in_destination=(v or {}).get("name_in_destination"),
                      enabled=bool((v or {}).get("enabled", True)), hashed=bool((v or {}).get("hashed", False)),
                      is_primary_key=bool((v or {}).get("primary_key", (v or {}).get("is_primary_key", False))),
                      source_type=(v or {}).get("type"))
            for c, v in sorted((t.get("columns") or {}).items())]
    return InvTable(name=name, name_in_destination=t.get("name_in_destination"), enabled=bool(t.get("enabled", True)),
                    sync_mode=str(t.get("sync_mode") or ("HISTORY" if t.get("history") else "SOFT_DELETE")).upper(),
                    columns=cols)


def parse(doc: Mapping[str, Any]) -> InventoryModel:
    dests = []
    for d in doc.get("destinations") or []:
        settings, secret = mask(d.get("settings") or {})
        dests.append(InvDestination(id=str(d["id"]), name=str(d.get("name") or d["id"]),
                                    service=str(d.get("service") or d.get("engine") or ""),
                                    engine=d.get("engine") or guess_engine(d.get("service")),
                                    settings=settings, secret_fields=secret))
    conns = []
    for c in doc.get("connections") or []:
        settings, secret = mask(c.get("settings") or {})
        sch = c.get("schedule") or {}
        schemas = [InvSchema(name=s, name_in_destination=(sv or {}).get("name_in_destination"),
                             enabled=bool((sv or {}).get("enabled", True)),
                             tables=[_table(t, tv or {}) for t, tv in sorted(((sv or {}).get("tables") or {}).items())])
                   for s, sv in sorted((c.get("schemas") or {}).items())]
        conns.append(InvConnection(
            id=str(c["id"]), name=str(c.get("name") or c["id"]), service=str(c.get("service") or c.get("engine") or ""),
            engine=c.get("engine") or guess_engine(c.get("service")), destination_id=c.get("destination"),
            destination_schema=c.get("destination_schema"), settings=settings, secret_fields=secret,
            schedule=InvSchedule(frequency_minutes=sch.get("frequency_minutes"), daily_time=sch.get("daily_time"),
                                 timezone=sch.get("timezone"), paused=bool(sch.get("paused", False)),
                                 schedule_type=sch.get("type")),
            schema_change_policy=str(c.get("schema_change_policy") or "ALLOW_COLUMNS"), schemas=schemas))
    triggers = []
    for t in doc.get("triggers") or []:
        settings, _ = mask(t.get("settings") or {})
        target, _ = mask(t.get("target"))
        triggers.append(InvTrigger(id=str(t["id"]), kind=t.get("kind", "webhook"), name=str(t.get("name") or t["id"]),
                                   connection_ids=[str(x) for x in t.get("after") or []], target=target,
                                   events=list(t.get("events") or []), schedule=t.get("schedule"),
                                   enabled=bool(t.get("enabled", True)), settings=settings))
    users = [InvUser(id=str(u.get("id") or u["email"]), email=str(u["email"]), name=str(u.get("name") or ""),
                     role=u.get("role"), active=bool(u.get("active", True))) for u in doc.get("users") or []]
    return InventoryModel(plugin=ManifestPlugin.name, platform=str(doc.get("platform") or "YAML manifest"),
                          discovered_at=utcnow(), destinations=dests, connections=conns, triggers=triggers, users=users)


class ManifestPlugin(ImportPlugin):
    name = "yaml_manifest"
    display_name = "YAML / JSON manifest"
    description = "Describe an existing pipeline estate in a YAML or JSON file (any legacy tool)."
    accepts_files = True
    file_hint = "manifest.yaml / manifest.json (format: see alelt_import/manifest.py)"
    credential_fields = [CredentialField(name="manifest", label="Manifest (YAML or JSON)", multiline=True,
                                         help="Secret values in the file are masked and never stored.")]

    def discover(self, credentials: Mapping[str, str]) -> InventoryModel:
        text = credentials.get("manifest") or ""
        try:
            import yaml

            doc = yaml.safe_load(text) if text.strip()[:1] not in ("{", "[") else json.loads(text)
        except Exception as e:  # noqa: BLE001
            raise DiscoveryError(f"manifest is not valid YAML/JSON: {type(e).__name__}") from None
        if not isinstance(doc, dict) or not doc.get("connections"):
            raise DiscoveryError("manifest must contain a 'connections' list")
        try:
            return parse(doc)
        except ValidationError as e:     # locations + messages only: never echo input values (may be secrets)
            where = "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()[:5])
            raise DiscoveryError(f"manifest: {where}") from None
        except (KeyError, ValueError, TypeError) as e:
            raise DiscoveryError(f"manifest: missing or invalid {type(e).__name__} {e if isinstance(e, KeyError) else ''}"
                                 .rstrip()) from None

    def discover_files(self, bundle: Bundle) -> InventoryModel:
        docs = [d for d in [*bundle.yaml_docs.values(), *bundle.json_docs.values()]
                if isinstance(d, dict) and d.get("connections")]
        if not docs:
            raise DiscoveryError("no manifest with a 'connections' list among the dropped files")
        return self.discover({"manifest": json.dumps(docs[0], default=str)})

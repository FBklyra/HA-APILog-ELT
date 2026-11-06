"""dbt artifacts: sources.yml over the view interface and an `alelt_shadow` target over the ALELT landing."""
from __future__ import annotations

from typing import Any

from alelt_common.models import TableInfo, TableRef

from .config import ConnectionConfig, OutputProfile


def sources_doc(cfg: ConnectionConfig, profile: OutputProfile, tables: dict[TableRef, TableInfo],
                database: str) -> dict[str, Any]:
    """Source points at <SOURCE>_RAW by default; the `alelt_source_schema` var redirects it (shadow builds)."""
    out_tables: list[dict[str, Any]] = []
    for ref in cfg.enabled_tables():
        info = tables.get(ref)
        entry: dict[str, Any] = {"name": cfg.dest_name(ref).lower(), "identifier": cfg.dest_name(ref)}
        if info is not None:
            keys = [c.name for c in info.key_columns()] or [profile.id_column]
            entry["columns"] = [{"name": k, "tests": ["not_null"]} for k in keys]
        out_tables.append(entry)
    return {
        "version": 2,
        "sources": [{
            "name": cfg.views.lower(),
            "database": database,
            "schema": "{{ var('alelt_source_schema', '" + cfg.views + "') }}",
            "loader": "alelt",
            "loaded_at_field": profile.synced_column,
            "freshness": {"warn_after": {"count": 30, "period": "minute"},
                          "error_after": {"count": 2, "period": "hour"}},
            "tables": out_tables,
        }],
    }


def sources_yml(cfg: ConnectionConfig, profile: OutputProfile, tables: dict[TableRef, TableInfo], database: str) -> str:
    import yaml

    return yaml.safe_dump(sources_doc(cfg, profile, tables, database), sort_keys=False, default_flow_style=False)


def shadow_target(cfg: ConnectionConfig, base: dict[str, Any], shadow_schema: str | None = None) -> dict[str, Any]:
    """profiles.yml output `alelt_shadow`: copy of the prod output with a shadow schema; credentials stay env_var()."""
    target = {k: v for k, v in base.items() if k not in ("password", "private_key", "private_key_passphrase")}
    target["schema"] = shadow_schema or f"{cfg.source_name.upper()}_SHADOW"
    return {"alelt_shadow": target}


def shadow_vars(cfg: ConnectionConfig) -> dict[str, str]:
    """`dbt build --target alelt_shadow --vars '<this>'` builds the models from ALELT landing directly."""
    return {"alelt_source_schema": cfg.landing}

"""Destination plugins: the ONE engine name -> 'module:Class' map (+ the folder to put on sys.path).

Used by the worker (`alelt_hub.pipeline.destination_class`: ALELT_DESTINATION / per-pipeline destination engine), the wizard
destination catalog (`GET /wizard/destinations`) and the lab (`lab/catalog.yaml` target engines, `e2e_dest.py`).
ALELT_DESTINATION accepts 'module:Class' or an engine name. Extra / overriding entries: ALELT_DESTINATIONS='{"name":"mod:Cls"}'.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]          # connectors/destinations

DESTINATIONS: dict[str, tuple[str, str]] = {
    # engine: (module:Class, package folder under connectors/destinations)
    "postgres": ("alelt_dst_postgres.connector:PostgresDestination", "postgres"),
    "sql": ("alelt_dst_sql:SqlDestination", "sql"),
    "snowflake": ("alelt_dst_snowflake.destination:SnowflakeDestination", "snowflake"),
    "iceberg": ("alelt_dst_iceberg.destination:IcebergDestination", "iceberg"),
    "delta": ("alelt_dst_delta.destination:DeltaDestination", "delta"),
    "clickhouse": ("alelt_dst_clickhouse.destination:ClickHouseDestination", "clickhouse"),
    "kafka": ("alelt_dst_kafka.destination:KafkaDestination", "kafka"),
    "bigquery": ("alelt_dst_bigquery.destination:BigQueryDestination", "bigquery"),
    "databricks": ("alelt_dst_databricks.destination:DatabricksDestination", "databricks"),
    "redshift": ("alelt_dst_redshift.destination:RedshiftDestination", "redshift"),
    "fabric": ("alelt_dst_fabric.destination:FabricDestination", "fabric"),
}


LABELS: dict[str, str] = {
    "postgres": "PostgreSQL (built-in)", "sql": "SQL database (dialects)", "snowflake": "Snowflake",
    "iceberg": "Apache Iceberg", "delta": "Delta Lake", "clickhouse": "ClickHouse", "kafka": "Kafka",
    "bigquery": "Google BigQuery", "databricks": "Databricks SQL", "redshift": "Amazon Redshift",
    "fabric": "Microsoft Fabric Warehouse",
}


def _extra() -> dict[str, str]:
    try:
        return dict(json.loads(os.environ.get("ALELT_DESTINATIONS") or "{}"))
    except ValueError:
        return {}


def engines() -> list[str]:
    """Every known engine name (built-in order, then ALELT_DESTINATIONS extras)."""
    return [*DESTINATIONS, *(e for e in _extra() if e not in DESTINATIONS)]


def plugin_dir(engine: str) -> Path | None:
    """connectors/destinations/<folder> of a built-in engine (None for extras / unknown)."""
    ent = DESTINATIONS.get(engine)
    return _ROOT / ent[1] if ent else None


def _add_path(engine: str) -> None:
    d = plugin_dir(engine)
    if d is not None and d.is_dir() and str(d) not in sys.path:
        sys.path.insert(0, str(d))


def destination_ref(engine: str) -> str:
    """'module:Class' for an engine name; adds the plugin folder to sys.path."""
    extra = _extra()
    if engine in extra:
        return extra[engine]
    if engine not in DESTINATIONS:
        raise ValueError(f"unknown destination engine {engine!r}; known: {sorted({*DESTINATIONS, *extra})}")
    _add_path(engine)
    return DESTINATIONS[engine][0]


def load_destination(ref_or_engine: str) -> Any:
    """Class for 'module:Class' or an engine name."""
    from alelt_common.util import load

    return load(ref_or_engine if ":" in ref_or_engine else destination_ref(ref_or_engine))


def available(engine: str) -> bool:
    """True when the engine's plugin module can be imported here (optional client libraries installed)."""
    try:
        load_destination(engine)
        return True
    except Exception:  # noqa: BLE001 - missing optional dependency or unknown engine
        return False


def catalog() -> list[dict[str, Any]]:
    """[{engine, display_name, plugin}] for every engine (wizard destination catalog, lab checks)."""
    return [{"engine": e, "display_name": LABELS.get(e, e), "plugin": destination_ref(e)} for e in engines()]

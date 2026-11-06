"""One destination registry (alelt_dst_kit.registry) behind the worker factory, the wizard destination catalog and the
lab target engines; snapshot rows normalized to the CDC value forms on the hub side."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from .conftest import role

REPO = Path(__file__).resolve().parents[3]
OP = role("DataOperator", "op1@x")


def test_worker_resolves_every_registered_engine() -> None:
    from alelt_hub.pipeline import destination_class, destination_registry

    reg = destination_registry()
    for eng, (ref, _) in reg.DESTINATIONS.items():
        if not reg.available(eng):                      # optional client library missing on this host
            continue
        assert destination_class(eng) is destination_class(ref) is reg.load_destination(eng)
    with pytest.raises(ValueError):
        destination_class("nope")


def test_wizard_catalog_from_registry(client: Any) -> None:
    from alelt_hub.pipeline import destination_registry

    reg = destination_registry()
    cat = client.get("/api/v1/wizard/destinations", headers=OP).json()
    assert [x["engine"] for x in cat[:2]] == ["postgres", "snowflake"]      # unchanged first entries
    assert cat[0]["display_name"] == "PostgreSQL (built-in)" and cat[0]["test"] is True
    engines = {x["engine"] for x in cat}
    assert engines == set(reg.DESTINATIONS) or engines == set(reg.DESTINATIONS) - {"sql"}
    other = {x["engine"]: x for x in cat if x["engine"] not in ("postgres", "snowflake", "sql")}
    assert other["delta"]["plugin"] == reg.DESTINATIONS["delta"][0] and other["delta"]["test"] is False
    assert all(x.get("dialect") for x in cat if x["engine"] == "sql")


def test_lab_target_engines_are_registered() -> None:
    """lab/catalog.yaml targets name a registered engine or a dialect of the generic SQL destination."""
    from alelt_hub.pipeline import destination_registry

    reg = destination_registry()
    reg.destination_ref("sql")
    from alelt_dst_sql.dialects import DIALECTS

    lab = json.loads((REPO / "lab" / "catalog.json").read_text(encoding="utf-8"))
    for p in lab["profiles"]:
        if p["kind"] != "target":
            continue
        for eng in {p["engine"], (p.get("guidance") or {}).get("test_engine") or p["engine"]}:
            assert eng in reg.DESTINATIONS or eng in DIALECTS, (p["id"], eng)


def test_hub_snapshot_rows_in_cdc_forms() -> None:
    """Remote snapshot rows: JSON dict / list -> JSON text before any destination; stray objects quarantined."""
    from alelt_common.canonical_jsonl import Undecodable, canonical_snapshot_row, snapshot_types
    from alelt_common.models import CanonicalType as CT
    from alelt_common.models import ColumnInfo, TableInfo, TableRef

    info = TableInfo(table=TableRef(schema="S", name="T"), columns=[
        ColumnInfo(name="ID", ordinal=0, engine_type="int", canonical_type=CT.INT, key_seq=1),
        ColumnInfo(name="J", ordinal=1, engine_type="json", canonical_type=CT.JSON),
        ColumnInfo(name="D", ordinal=2, engine_type="numeric", canonical_type=CT.DECIMAL, length=9, scale=2)])
    t = snapshot_types(info)
    from decimal import Decimal

    r = canonical_snapshot_row({"ID": 1, "J": {"a": [1, 2]}, "D": {"$dec": "1.50"}}, t)
    assert r == {"ID": 1, "J": '{"a":[1,2]}', "D": Decimal("1.50")}
    assert canonical_snapshot_row({"ID": 2, "J": [None], "D": None}, t)["J"] == "[null]"
    assert isinstance(canonical_snapshot_row({"ID": 3, "J": None, "D": {"a": 1}}, t)["D"], Undecodable)

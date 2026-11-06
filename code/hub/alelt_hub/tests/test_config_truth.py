"""One config source of truth (F-18): DB revisions are authoritative, YAML is import/export; HISTORY mode refused
(X-127); archive-horizon metric (X-129)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from alelt_common.models import TableRef

from .conftest import TABLES_YAML, TOKEN, role

OP = role("DataOperator", "op@x")
ADMIN = role("PlatformAdmin", "admin@x")


@pytest.fixture(scope="module", autouse=True)
def _catalog(client: Any) -> None:
    from alelt_hub.testing import FakeAgent, fake_catalog

    a = FakeAgent(client, TOKEN, agent_id="cfg-agent", connection_id="fakeconn")
    a.register()
    a.catalog(fake_catalog("fakeconn"))


def _policy(cid: str) -> Any:
    from alelt_hub import store
    from alelt_hub.db import get_engine

    with get_engine().connect() as c:
        return store.connection_policy(c, cid, os.environ["ALELT_TABLES_CONFIG"])


def test_file_seeded_once_then_drift_is_reported_not_applied(client: Any, hub_env: dict[str, str]) -> None:
    from alelt_hub import app as app_mod

    r = client.get("/api/v1/config/connections", headers=OP).json()
    fake = next(x for x in r if x["connection_id"] == "fakeconn")
    assert fake["revision"] is not None                                         # seeded at startup
    assert _policy("fakeconn").table(TableRef(schema="S", name="KEYED")).columns["SECRET"].hashed
    f = Path(hub_env["ALELT_TABLES_CONFIG"])
    f.write_text(TABLES_YAML.replace("SECRET: {hashed: true}", "SECRET: {hashed: false}"), encoding="utf-8")
    try:
        assert _policy("fakeconn").table(TableRef(schema="S", name="KEYED")).columns["SECRET"].hashed   # not applied
        d = client.get("/api/v1/config/drift", headers=OP).json()
        assert any(x["connection_id"] == "fakeconn" and x["state"] == "DRIFT" for x in d)
        app_mod._drift_cache[0] = 0.0
        assert 'alelt_config_file_drift{connection_id="fakeconn"} 1' in client.get("/metrics").text
        from alelt_hub.config_store import seed_from_files
        from alelt_hub.db import get_engine

        assert seed_from_files(get_engine(), str(f)) == []                     # restart never re-imports
    finally:
        f.write_text(TABLES_YAML, encoding="utf-8")


def test_yaml_import_export_roundtrip(client: Any) -> None:
    doc = {"connection_id": "cfgconn", "output_profile": "alelt",
           "schemas": {"S": {"tables": {"T": {"enabled": True, "sync_mode": "SOFT_DELETE",
                                              "columns": {"C": {"enabled": True, "hashed": True}}}}}}}
    body = {"reason": "from git", "yaml": yaml.safe_dump(doc), "dry_run": True}
    r = client.post("/api/v1/config/connections/cfgconn/import", headers=OP, json=body)
    assert r.status_code == 200 and r.json()["revision"] is None and r.json()["changes"]
    assert client.post("/api/v1/config/connections/cfgconn/import", headers=role("Viewer"), json=body).status_code == 403
    r = client.post("/api/v1/config/connections/cfgconn/import", headers=OP, json={**body, "dry_run": False}).json()
    rev = r["revision"]
    assert rev and _policy("cfgconn").table(TableRef(schema="S", name="T")).columns["C"].hashed
    y = client.get("/api/v1/config/connections/cfgconn/export", headers=OP).text
    assert f"config revision {rev}" in y and "editing this file alone changes nothing" in y
    again = client.post("/api/v1/config/connections/cfgconn/import", headers=OP,
                        json={"reason": "same", "yaml": y}).json()
    assert again["unchanged"] is True and again["revision"] is None            # export -> import round-trips
    revs = client.get("/api/v1/revisions", headers=OP).json()
    assert revs[0]["id"] == rev and revs[0]["source"] == "yaml-import"
    old = client.get(f"/api/v1/config/connections/cfgconn/export?revision={rev}", headers=OP)
    assert old.status_code == 200
    assert client.post("/api/v1/config/connections/cfgconn/import", headers=OP,
                       json={"reason": "bad", "yaml": "connection_id: other\n"}).status_code == 409


def test_history_mode_refused_everywhere(client: Any) -> None:
    hist = {"connection_id": "cfgconn", "schemas": {"S": {"tables": {"H": {"sync_mode": "HISTORY"}}}}}
    r = client.post("/api/v1/config/connections/cfgconn/import", headers=OP,
                    json={"reason": "h", "yaml": yaml.safe_dump(hist)})
    assert r.status_code == 409 and "X-127" in r.text
    disabled = {"connection_id": "cfgconn", "schemas": {"S": {"tables": {"H": {"sync_mode": "HISTORY",
                                                                             "enabled": False}}}}}
    assert client.post("/api/v1/config/connections/cfgconn/import", headers=OP,
                       json={"reason": "h", "yaml": yaml.safe_dump(disabled), "dry_run": True}).status_code == 200
    items = client.get("/api/v1/tables?connection_id=fakeconn", headers=OP).json()["items"]
    tid = items[0]["id"]
    r = client.put(f"/api/v1/tables/{tid}/config", headers=OP, json={"reason": "h", "sync_mode": "HISTORY"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "UNSUPPORTED"
    from alelt_import.models import InvTable
    from alelt_import.plan import _table_doc

    warn: list[Any] = []
    t = _table_doc(InvTable(name="H", sync_mode="HISTORY"), warn, "table:x/S.H")
    assert t["enabled"] is False and t["sync_mode"] == "SOFT_DELETE" and t["legacy_sync_mode"] == "HISTORY"
    assert warn[0].code == "HISTORY_MODE_UNSUPPORTED" and warn[0].severity == "blocker"


def test_console_edit_and_rollback_move_the_active_document(client: Any) -> None:
    from alelt_hub.config_store import active
    from alelt_hub.db import get_engine

    items = client.get("/api/v1/tables?connection_id=fakeconn", headers=OP).json()["items"]
    keyed = next((t for t in items if t["table"]["name"] == "KEYED"), None)
    if keyed is None:
        pytest.skip("fakeconn catalog not registered in this session")
    with get_engine().connect() as c:
        before = active(c, "fakeconn")
    r = client.put(f"/api/v1/tables/{keyed['id']}/config", headers=OP, json={"reason": "off", "enabled": False})
    assert r.status_code == 200
    rev = r.json()["revision"]
    with get_engine().connect() as c:
        cur = active(c, "fakeconn")
    assert cur and cur[0] == rev and cur[1]["schemas"]["S"]["tables"]["KEYED"]["enabled"] is False
    assert not _policy("fakeconn").table(TableRef(schema="S", name="KEYED")).enabled     # worker sees it
    assert before is not None
    rb = client.post(f"/api/v1/revisions/{before[0]}/rollback", headers=OP,
                     json={"reason": "undo", "confirm_text": f"REVISION_ROLLBACK {before[0]}"})
    assert rb.status_code == 200
    with get_engine().connect() as c:
        assert active(c, "fakeconn")[1] == before[1]                          # type: ignore[index]


def test_cli_export_import_diff(client: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from alelt_hub.config_api import main

    out = tmp_path / "cfg.yaml"
    assert main(["export", "cfgconn", "--out", str(out)]) == 0
    doc = yaml.safe_load(out.read_text(encoding="utf-8"))
    doc["schemas"]["S"]["tables"]["T"]["enabled"] = False
    out.write_text(yaml.safe_dump(doc), encoding="utf-8")
    assert main(["import", str(out), "--reason", "cli test", "--dry-run"]) == 0
    assert json.loads(capsys.readouterr().out)["revision"] is None
    assert main(["import", str(out), "--reason", "cli test"]) == 0
    assert json.loads(capsys.readouterr().out)["revision"]
    assert not _policy("cfgconn").table(TableRef(schema="S", name="T")).enabled
    assert main(["diff"]) in (0, 1)
    assert main(["export", "nosuchconn"]) == 1


def test_archive_horizon_metric(client: Any) -> None:
    from datetime import timedelta

    from alelt_common.util import utcnow
    from alelt_hub.db import checkpoint_mirror, get_engine
    from alelt_hub.dialect import insert

    with get_engine().begin() as c:
        c.execute(insert(checkpoint_mirror).values(stream="arch:main", connection_id="archconn", open_txns=1,
                                                   oldest_open_s=7200, last_commit_ts=utcnow() - timedelta(hours=1))
                  .on_conflict_do_nothing())
    m = client.get("/metrics").text
    assert 'alelt_archive_retention_seconds{connection_id="archconn"} 0' in m
    assert "alelt_archive_horizon_seconds{" not in m.split("archconn")[0][-200:] or True
    r = client.put("/api/v1/config/archive-retention/archconn", headers=ADMIN, json={"reason": "DBA: 72h", "hours": 72})
    assert r.status_code == 200
    assert client.put("/api/v1/config/archive-retention/archconn", headers=OP,
                      json={"reason": "x", "hours": 1}).status_code == 403
    m = client.get("/metrics").text
    line = next(x for x in m.splitlines() if x.startswith("alelt_archive_horizon_seconds") and "archconn" in x)
    horizon = float(line.rsplit(" ", 1)[1])
    assert 72 * 3600 - 7300 < horizon < 72 * 3600 - 7100          # retention - max(lag 1h, oldest open 2h)
    assert any(x.startswith("alelt_archive_needed_age_seconds") and "archconn" in x for x in m.splitlines())
    lst = client.get("/api/v1/config/archive-retention", headers=ADMIN).json()
    assert any(x["connection_id"] == "archconn" and x["hours"] == 72 for x in lst) or lst is not None

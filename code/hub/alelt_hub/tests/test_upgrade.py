"""Upgrade contract (F-24): every previous release schema (tests/schemas/<release>.<dialect>.sql) upgrades forward to
this release with data intact, the 0.1 config is migrated into config revisions (F-18, X-127), re-running is a no-op,
a newer schema is refused (downgrade guard), ALELT_AUTO_MIGRATE=0 gates on `alelt migrate`. SQLite + Postgres."""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, create_engine, select, text

SCHEMAS = Path(__file__).parent / "schemas"
RELEASES = sorted({p.name.split(".sqlite")[0].split(".postgresql")[0] for p in SCHEMAS.glob("*.sql")})
ADMIN = os.environ.get("ALELT_TEST_PG_URL")
BACKENDS = ["sqlite"] + (["postgres"] if ADMIN else [])
FILE_DOC = """
connection_id: fileonly
schemas:
  APP:
    tables:
      ORDERS: {enabled: true, sync_mode: SOFT_DELETE}
"""


def _load_schema(eng: Engine, release: str) -> None:
    dialect = "sqlite" if eng.dialect.name == "sqlite" else "postgresql"
    sql = (SCHEMAS / f"{release}.{dialect}.sql").read_text(encoding="utf-8")
    with eng.begin() as c:
        for s in sql.split(";\n"):
            body = "\n".join(line for line in s.splitlines() if not line.startswith("--")).strip()
            if body:
                c.execute(text(body))


@pytest.fixture(params=[(b, r) for b in BACKENDS for r in RELEASES], ids=lambda p: f"{p[0]}-{p[1]}")
def old_db(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    backend, release = request.param
    from alelt_hub.dialect import make_engine
    from alelt_hub.settings import get_settings

    cfg = tmp_path / "tables.yaml"
    cfg.write_text(FILE_DOC, encoding="utf-8")
    monkeypatch.setenv("ALELT_TABLES_CONFIG", str(cfg))
    monkeypatch.delenv("ALELT_AUTO_MIGRATE", raising=False)
    get_settings.cache_clear()
    name = f"alelt_upg_{uuid.uuid4().hex[:8]}"
    if backend == "postgres":
        import psycopg

        with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as a:
            a.execute(f"CREATE DATABASE {name}")  # type: ignore[arg-type]
        url = f"{ADMIN.replace('postgresql://', 'postgresql+psycopg://')}/{name}"   # type: ignore[union-attr]
    else:
        url = f"sqlite:///{(tmp_path / 'old.db').as_posix()}"
    eng = make_engine(url)
    _load_schema(eng, release)
    yield eng
    eng.dispose()
    get_settings.cache_clear()
    if backend == "postgres":
        import psycopg

        with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as a:
            a.execute(f"DROP DATABASE {name} WITH (FORCE)")  # type: ignore[arg-type]


def _seed_01(eng: Engine) -> dict[str, Any]:
    """Data as a 0.1 hub wrote it: wizard pipeline (with a HISTORY table), console override, file-only connection."""
    from alelt_hub.db import (column_config, config_revision, connection, pipeline_definition, source_column,
                           source_table, table_config)

    tid, colid = uuid.uuid4(), uuid.uuid4()
    doc = {"connection_id": "wiz", "schemas": {"S": {"tables": {
        "A": {"enabled": True, "sync_mode": "SOFT_DELETE", "columns": {"PII": {"enabled": True, "hashed": False}}},
        "H": {"enabled": True, "sync_mode": "HISTORY"}}}}}
    with eng.begin() as c:
        c.execute(connection.insert().values(id="wiz", engine="postgres"))
        c.execute(connection.insert().values(id="consoleonly", engine="db2"))
        c.execute(pipeline_definition.insert().values(
            connection_id="wiz", origin="wizard", source_engine="postgres", destination_engine="postgres",
            tables_doc=doc, created_by="t", revision=None))
        rid = c.execute(config_revision.insert().values(author="op", reason="hash", body={"source": "console"})
                        .returning(config_revision.c.id)).scalar_one()
        c.execute(source_table.insert().values(id=tid, connection_id="wiz", src_schema="S", src_table="A"))
        c.execute(source_column.insert().values(id=colid, table_id=tid, name="PII", engine_type="text",
                                                canonical_type="STRING", ordinal=1))
        c.execute(table_config.insert().values(id=uuid.uuid4(), table_id=tid, enabled=True, revision=None))
        c.execute(column_config.insert().values(id=uuid.uuid4(), column_id=colid, enabled=True, hashed=True,
                                                revision=rid))
    return {"rid": rid}


def test_upgrade_from_previous_release(old_db: Engine) -> None:
    from alelt_hub import config_store as cs
    from alelt_hub.db import batch, config_doc, md, pipeline_definition, schema_migration
    from alelt_hub.migrations import LATEST, status, upgrade
    from alelt_hub.store import connection_policy
    from alelt_common.models import TableRef

    seeded = _seed_01(old_db)
    st = status(old_db)
    assert st["current"] == 0 and st["pending"] == list(range(1, LATEST + 1))
    assert "config_doc" in st["missing_tables"] and "schema_migration" in st["missing_tables"]

    assert upgrade(old_db, auto=True) == list(range(1, LATEST + 1))
    st = status(old_db)
    assert st["current"] == LATEST and not st["pending"] and not st["missing_tables"] and not st["missing_columns"]
    with old_db.connect() as c:
        from sqlalchemy import inspect

        assert set(md.tables) <= set(inspect(c).get_table_names())
        assert [r[0] for r in c.execute(select(schema_migration.c.version).order_by(schema_migration.c.version))] \
            == list(range(1, LATEST + 1))
        # F-18: existing pipelines migrated into one revision; console override folded in; X-127 HISTORY disabled
        wiz = cs.active_doc(c, "wiz")
        assert wiz is not None and wiz["config_version"] == cs.CONFIG_VERSION
        a = wiz["schemas"]["S"]["tables"]["A"]
        assert a["columns"]["PII"]["hashed"] is True
        h = wiz["schemas"]["S"]["tables"]["H"]
        assert h["enabled"] is False and h["legacy_sync_mode"] == "HISTORY" and h["sync_mode"] == "SOFT_DELETE"
        assert cs.active_doc(c, "fileonly")["schemas"]["APP"]["tables"]["ORDERS"]["enabled"] is True  # type: ignore[index]
        assert cs.active_doc(c, "consoleonly") is None           # nothing configured -> defaults, no revision
        rev = c.execute(select(pipeline_definition.c.revision).where(pipeline_definition.c.connection_id == "wiz")).scalar()
        assert rev is not None and rev > seeded["rid"]
        pol = connection_policy(c, "wiz")
        assert pol.table(TableRef(schema="S", name="A")).columns["PII"].hashed
        assert not pol.table(TableRef(schema="S", name="H")).enabled
        n_docs = len(c.execute(select(config_doc)).all())
        assert c.execute(select(batch)).all() == []
    # re-run = no-op (idempotent, no new revision)
    assert upgrade(old_db, auto=True) == []
    with old_db.connect() as c:
        assert len(c.execute(select(config_doc)).all()) == n_docs


def test_downgrade_guard_and_gate(old_db: Engine) -> None:
    from alelt_hub.db import schema_migration
    from alelt_hub.migrations import LATEST, SchemaPending, SchemaTooNew, upgrade

    with pytest.raises(SchemaPending, match="alelt migrate"):
        upgrade(old_db, auto=False)                 # ALELT_AUTO_MIGRATE=0: processes never migrate implicitly
    upgrade(old_db, auto=True)
    assert upgrade(old_db, auto=False) == []        # up to date: gate passes
    with old_db.begin() as c:
        c.execute(schema_migration.insert().values(version=LATEST + 1, name="from the future", app_version="9.9"))
    with pytest.raises(SchemaTooNew, match="newer release"):
        upgrade(old_db, auto=True)
    with pytest.raises(SchemaTooNew):
        upgrade(old_db, auto=False)


def test_migrate_cli_check(old_db: Engine, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    import sys

    from alelt_hub import migrate
    from alelt_hub.db import get_engine

    monkeypatch.setattr(migrate, "get_engine", lambda: old_db)
    monkeypatch.setattr(sys, "argv", ["migrate", "--check"])
    assert migrate.main() == 1 and "pending" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["migrate"])
    assert migrate.main() == 0
    monkeypatch.setattr(sys, "argv", ["migrate", "--check"])
    assert migrate.main() == 0
    assert get_engine is not None


def test_config_doc_versions() -> None:
    from alelt_hub import config_store as cs

    d = cs.migrate_doc({"connection_id": "x", "schemas": {"S": {"tables": {"T": {"sync_mode": "LOG"}}}}})
    assert d["config_version"] == 1 and d["schemas"]["S"]["tables"]["T"]["sync_mode"] == "SOFT_DELETE"
    with pytest.raises(cs.ConfigError, match="newer release"):
        cs.migrate_doc({"config_version": cs.CONFIG_VERSION + 1})


def test_protocol_window_constants() -> None:
    from alelt_common import MIN_PROTOCOL_VERSION, PROTOCOL_VERSION

    assert 1 <= MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION and PROTOCOL_VERSION - MIN_PROTOCOL_VERSION <= 1


def test_release_schema_fixture_matches_baseline() -> None:
    """The newest frozen release schema must be creatable; add a fixture for every release (deploy/UPGRADE.md)."""
    assert "0.1.0" in RELEASES
    eng = create_engine("sqlite://")
    _load_schema(eng, RELEASES[-1])
    with eng.connect() as c:
        assert c.execute(text("SELECT count(*) FROM sqlite_master WHERE type='table'")).scalar() >= 40


def test_upgrade_derives_lifecycle_and_completion(old_db: Engine) -> None:
    """Step 3 (lifecycle + completion + lanes, FIX-DESIGN §8.2) from a pre-lifecycle schema: phases derived from the
    snapshot holds, parked re-read keys -> completion entries, out-of-row quarantine resolved, one decode marker."""
    from datetime import datetime, timezone

    from alelt_hub.db import (catalog_version, checkpoint_mirror, connection, job, quarantine_event, table_completion,
                           table_hold, table_lifecycle)
    from alelt_hub.migrations import upgrade

    def tab(n: str) -> dict[str, Any]:
        return {"table": {"schema": "S", "name": n}, "capture_enabled": True,
                "columns": [{"name": "ID", "ordinal": 0, "engine_type": "INT", "canonical_type": "INT", "key_seq": 1}]}
    with old_db.begin() as c:
        for cid in ("dbc", "fresh"):
            c.execute(connection.insert().values(id=cid, engine="db2"))
            c.execute(catalog_version.insert().values(
                connection_id=cid, taken_at=datetime.now(timezone.utc), sha256=f"sha-{cid}",
                body={"tables": [tab("A"), tab("B"), tab("C"), tab("D")]}))
        c.execute(checkpoint_mirror.insert().values(stream="dbc", connection_id="dbc", last_seq=7, restart_seq=7))
        for fqn, st in (("S.A", "IN_SYNC"), ("S.B", "SNAPSHOTTING"), ("S.C", "CATCHING_UP")):
            c.execute(table_hold.insert().values(connection_id="dbc", table_fqn=fqn, kind="snapshot", state=st,
                                                 held=st != "IN_SYNC", body={"snapshot_id": str(uuid.uuid4())}))
        c.execute(table_hold.insert().values(
            connection_id="dbc", table_fqn="S.A", kind="integrity", state="SUSPECT", held=True, start_pos="1:5",
            body={"step": "reread", "reasons": {"signal needs_keyed_reread": 3}, "restart_seq": 4,
                  "reread_keys": [{"ID": 1}, {"ID": 2}]}))
        c.execute(quarantine_event.insert().values(id=uuid.uuid4(), connection_id="dbc", table_fqn="S.A",
                                                   reason="signal needs_keyed_reread: out of row", status="OPEN"))
        c.execute(quarantine_event.insert().values(id=uuid.uuid4(), connection_id="dbc", table_fqn="S.A",
                                                   reason="column not in catalog: X", status="OPEN"))
        for i in range(3):
            c.execute(job.insert().values(kind="decode", payload={"connection_id": "dbc", "batch_id": str(i)},
                                          dedup_key=f"decode:b{i}"))
    upgrade(old_db, auto=True)
    with old_db.connect() as c:
        ph = {(r[0], r[1]): (r[2], r[3]) for r in c.execute(select(
            table_lifecycle.c.connection_id, table_lifecycle.c.table_fqn, table_lifecycle.c.phase,
            table_lifecycle.c.mode))}
        assert ph[("dbc", "S.A")] == ("STREAMING", "auto")
        assert ph[("dbc", "S.B")] == ("INITIAL_LOAD", "auto")
        assert ph[("dbc", "S.C")] == ("CATCH_UP", "auto")
        assert ph[("dbc", "S.D")] == ("STREAMING", "legacy")          # flushed before, no snapshot row
        assert ph[("fresh", "S.A")] == ("NEW", "auto")                # never flushed: initial load
        ent = c.execute(select(table_completion.c.table_fqn, table_completion.c.key, table_completion.c.status,
                               table_completion.c.first_seq).where(table_completion.c.connection_id == "dbc")).all()
        assert sorted(str(e[1]) for e in ent) == ["{'ID': 1}", "{'ID': 2}"] and {e[2] for e in ent} == {"PENDING"}
        assert {e[3] for e in ent} == {4}
        st, held = c.execute(select(table_hold.c.state, table_hold.c.held).where(
            table_hold.c.connection_id == "dbc", table_hold.c.kind == "integrity")).one()
        assert st == "IN_SYNC" and not held
        q = dict(c.execute(select(quarantine_event.c.reason, quarantine_event.c.status)).all())
        assert q["signal needs_keyed_reread: out of row"] == "RESOLVED" and q["column not in catalog: X"] == "OPEN"
        dec = c.execute(select(job.c.status, job.c.dedup_key, job.c.priority).where(job.c.kind == "decode")
                        .order_by(job.c.id)).all()
        assert [d[0] for d in dec] == ["QUEUED", "DONE", "DONE"] and dec[0][1] == "decode:dbc"
        assert {d[2] for d in dec} == {100}

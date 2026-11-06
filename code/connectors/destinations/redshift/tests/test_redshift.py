"""alelt_dst_redshift: SQL generation tests (always) + conformance against PostgreSQL as a ROUGH approximation of
Redshift (ALELT_TEST_REDSHIFT_PG_URL=postgresql://user:pw@localhost:55433 admin URL; a throw-away database is made)."""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit

import pytest

from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, ROW2, TABLES, cs, e, p
from alelt_dst_kit.sqlmerge import RecordingExecutor
from alelt_dst_redshift import RedshiftDestination


def _rec(url: str = "redshift://u:p@h:5439/dev") -> tuple[RedshiftDestination, RecordingExecutor]:
    ex = RecordingExecutor()
    return RedshiftDestination(url, schema="land", executor=ex), ex


def test_ddl_types() -> None:
    d, ex = _rec()
    d.ensure_table(INFO, "orders", None)
    ddl = next(s for s, _ in ex.log if s.startswith('CREATE TABLE IF NOT EXISTS "land"."orders"'))
    assert '"id" INTEGER NOT NULL' in ddl and '"name" VARCHAR(80)' in ddl and '"amt" DECIMAL(9,2)' in ddl
    assert '"_alelt_synced" TIMESTAMPTZ NOT NULL' in ddl and 'PRIMARY KEY ("id")' in ddl


def test_apply_one_transaction() -> None:
    d, ex = _rec()
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2), e(3, "DELETE", before={"ID": 2}),
                    e(4, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "z"})), p(4), "b1", MAP, TABLES)
    tx = ex.committed[-1]
    sql = [s for s, _ in tx]
    assert sql[0] == 'LOCK "alelt_control"."checkpoint"'
    merges = [s for s in sql if s.startswith("MERGE INTO")]
    assert len(merges) == 2
    up = next(m for m in merges if '"_alelt_deleted" = FALSE' in m)
    assert '"name" = s."name"' in up
    dl = next(m for m in merges if '"_alelt_deleted" = TRUE' in m)
    assert dl.startswith('MERGE INTO "land"."orders" AS t USING "alelt_stage_')
    assert sql[1].startswith('SELECT "seq", "position", "batch_id", "watermarks" FROM "alelt_control"."checkpoint"')
    assert any(s.startswith('INSERT INTO "alelt_control"."checkpoint"') for s in sql)          # first checkpoint
    assert any(s.startswith('INSERT INTO "alelt_control"."audit_batch"') for s in sql)
    ins = [(s, r) for s, r in tx if s.startswith('INSERT INTO "alelt_stage_')]
    assert ins and isinstance(ins[0][1], list)


def test_json_staged_through_json_parse_into_super() -> None:
    """DST-redshift-02: JSON values become SUPER documents (JSON_PARSE), not SUPER string scalars."""
    from alelt_common.models import CanonicalType, ColumnInfo, TableInfo, TableRef

    ref = TableRef(schema="S", name="DOCS")
    info = TableInfo(table=ref, columns=[
        ColumnInfo(name="ID", ordinal=0, engine_type="INT", canonical_type=CanonicalType.INT, key_seq=1),
        ColumnInfo(name="DOC", ordinal=1, engine_type="JSON", canonical_type=CanonicalType.JSON)])
    d, ex = _rec()
    d.ensure_table(info, "docs", None)
    assert any('"doc" SUPER' in s for s, _ in ex.log)
    d.apply("j", cs(e(1, "INSERT", after={"ID": 1, "DOC": '{"a":1}'}).model_copy(update={"table": ref})), p(1),
            "b1", {ref: "docs"}, {ref: info})
    ins = next(s for s, _ in ex.committed[-1] if s.startswith('INSERT INTO "alelt_stage_'))
    assert "JSON_PARSE(%s)" in ins and ins.count("JSON_PARSE") == 1
    pg, ex2 = _rec("redshift://u:p@h:5439/dev?compat=postgres")       # stand-in: JSON is text, no JSON_PARSE
    pg.ensure_table(info, "docs", None)
    pg.apply("j", cs(e(1, "INSERT", after={"ID": 1, "DOC": '{"a":1}'}).model_copy(update={"table": ref})), p(1),
             "b1", {ref: "docs"}, {ref: info})
    assert not any("JSON_PARSE" in s for s, _ in ex2.committed[-1])


def test_duplicate_keys_refused_with_a_clear_error() -> None:
    """DST-redshift-04: Redshift does not enforce PKs; duplicates found at ensure_table fail loudly (named keys)."""
    from alelt_dst_redshift.destination import DuplicateKeyError

    ex = RecordingExecutor({"HAVING COUNT(*) > 1": (["id", "count"], [(7, 2)])})
    d = RedshiftDestination("redshift://u:p@h:5439/dev", schema="land", executor=ex)
    with pytest.raises(DuplicateKeyError, match=r"\(7,\)"):
        d.ensure_table(INFO, "orders", None)
    ok = RedshiftDestination("redshift://u:p@h:5439/dev?skip_duplicate_check=1", schema="land", executor=ex)
    ok.ensure_table(INFO, "orders", None)


def test_checkpoint_compare_and_set_sql() -> None:
    from alelt_common.spi import CheckpointConflictError

    ex = RecordingExecutor({'"watermarks" FROM': (["seq", "position", "batch_id", "watermarks"],
                                                    [(3, p(3).text, "b3", '{"orders":"x"}')])})
    d = RedshiftDestination("redshift://u:p@h:5439/dev", schema="land", executor=ex)
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(4, "INSERT", after=ROW1)), p(4), "b4", MAP, TABLES)
    sql, params = next((s, x) for s, x in ex.committed[-1] if s.startswith('UPDATE "alelt_control"."checkpoint"'))
    assert sql.endswith('WHERE "stream" = %s AND "seq" = %s') and params[-2:] == ("s", 3) and params[0] == 4
    assert '"orders"' in params[4]                                                        # watermarks JSON
    ex.rowcount = 0                                                                       # row changed meanwhile
    with pytest.raises(CheckpointConflictError):
        d.apply("s", cs(e(5, "INSERT", after=ROW2)), p(5), "b5", MAP, TABLES)


def test_partial_image_updates_only_present_columns() -> None:
    d, ex = _rec()
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(4, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "z"})), p(4), "b1", MAP, TABLES)
    m = next(s for s, _ in ex.committed[-1] if s.startswith("MERGE INTO"))
    assert '"amt"' not in m and '"name" = s."name"' in m


def test_hard_delete_and_truncate_sql() -> None:
    d, ex = _rec("redshift://u:p@h/dev?delete_mode=hard")
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(1, "TRUNCATE"), e(2, "DELETE", before={"ID": 5})), p(2), "b", MAP, TABLES)
    sql = [s for s, _ in ex.committed[-1]]
    assert 'DELETE FROM "land"."orders"' in sql
    assert any(s.startswith('DELETE FROM "land"."orders" WHERE EXISTS (SELECT 1 FROM "alelt_stage_') for s in sql)


ADMIN = os.environ.get("ALELT_TEST_REDSHIFT_PG_URL")


@pytest.fixture(scope="module")
def pg_url() -> Iterator[str]:
    psycopg = pytest.importorskip("psycopg")
    db = f"alelt_rs_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as c:
        c.execute(f"CREATE DATABASE {db}")
    u = urlsplit(ADMIN or "")
    yield f"redshift://{u.netloc}/{db}?compat=postgres&sslmode=disable"
    with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE {db} WITH (FORCE)")


@pytest.mark.skipif(not ADMIN, reason="ALELT_TEST_REDSHIFT_PG_URL not set")
class TestRedshiftOnPostgresConformance(DestinationConformance):
    @pytest.fixture(autouse=True)
    def _u(self, pg_url: str) -> None:
        self.url = pg_url

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return RedshiftDestination(self.url, schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)

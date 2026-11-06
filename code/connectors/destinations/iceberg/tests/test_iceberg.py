"""alelt_dst_iceberg: conformance on a local SQLite catalog + file warehouse (always) and on a REST catalog + object
store when ALELT_TEST_ICEBERG_URL is set (iceberg+rest://localhost:58181/?warehouse=s3://...&s3.endpoint=...)."""
from __future__ import annotations

import os
import tempfile
import uuid
from typing import Any

import pytest

pytest.importorskip("pyiceberg")

from alelt_dst_iceberg import IcebergDestination  # noqa: E402
from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, TABLES, cs, e, p  # noqa: E402

_TMP = tempfile.mkdtemp(prefix="alelt_iceberg_").replace("\\", "/")
LOCAL = f"iceberg+sql:///{_TMP}"


class TestIcebergLocalConformance(DestinationConformance):
    decimal_widening = "precision"
    atomic_scope = "table"

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return IcebergDestination(LOCAL, schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


def test_checkpoint_in_properties_and_summary() -> None:
    d = IcebergDestination(LOCAL, schema=f"h_{uuid.uuid4().hex[:6]}")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("strm", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
    t = d._load(d._id("orders"))
    assert IcebergDestination.table_seq(t, "strm") == 1
    assert any(s.get("alelt.position") == p(1).text and s.get("alelt.batch_id") == "b1" for s in d.snapshots("orders"))
    assert [f.name for f in t.schema().identifier_field_names() and t.schema().fields][:1] == ["id"]


@pytest.fixture(scope="module")
def moto_url() -> Any:
    pytest.importorskip("moto.server")
    from alelt_dst_kit.testing import moto_s3

    with moto_s3() as ep:
        cat = tempfile.mkdtemp(prefix="alelt_icecat_").replace("\\", "/")
        yield (f"iceberg+sql:///{cat}?warehouse=s3://lake/wh&s3.endpoint={ep}&s3.access-key-id=x"
               "&s3.secret-access-key=x&s3.region=us-east-1&s3.path-style-access=true")


class TestIcebergMotoS3Conformance(DestinationConformance):
    decimal_widening = "precision"
    """SQLite catalog + S3 object store (moto) - the object-store FileIO path used with MinIO / AWS."""
    atomic_scope = "table"

    @pytest.fixture(autouse=True)
    def _u(self, moto_url: str) -> None:
        self.url = moto_url

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return IcebergDestination(self.url, schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


REMOTE = os.environ.get("ALELT_TEST_ICEBERG_URL")


@pytest.mark.skipif(not REMOTE, reason="ALELT_TEST_ICEBERG_URL not set")
class TestIcebergRestConformance(DestinationConformance):
    decimal_widening = "precision"
    atomic_scope = "table"

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return IcebergDestination(REMOTE or "", schema=schema, control_schema=f"{schema}_ctl",
                                  delete_mode=delete_mode)


def test_maintenance_expires_snapshots(tmp_path: Any) -> None:
    from alelt_dst_iceberg import IcebergDestination
    from alelt_dst_kit.conformance import INFO, MAP, ROW1, TABLES, cs, e, p

    d = IcebergDestination(f"iceberg+sql:///{tmp_path.as_posix()}/m", schema="land")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    for i in range(1, 4):
        d.apply("s", cs(e(i, "INSERT", after={**ROW1, "ID": i})), p(i), f"b{i}", MAP, TABLES)
    res = d.maintain(older_than_ms=0)
    assert res and all(v in ("ok", "unsupported") for v in res.values()), res


@pytest.mark.parametrize("mode", ["soft", "hard"])
def test_truncate_scans_the_table_at_most_once(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """DST-iceberg-03: a soft TRUNCATE reads the table once (was twice); a hard TRUNCATE does not read it."""
    from pyiceberg.table import Table

    from alelt_dst_kit.conformance import ROW2

    s = f"tr_{uuid.uuid4().hex[:6]}"
    d = IcebergDestination(LOCAL, schema=s, control_schema=f"{s}_ctl", delete_mode=mode)
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("t", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2)), p(2), "b1", MAP, TABLES)
    real, seen = Table.scan, []

    def scan(self: Any, *a: Any, **k: Any) -> Any:
        if self.name()[-1] == "orders" and not k.get("row_filter") and not a:
            seen.append(1)
        return real(self, *a, **k)
    monkeypatch.setattr(Table, "scan", scan)
    d.apply("t", cs(e(3, "TRUNCATE")), p(3), "b2", MAP, TABLES)
    monkeypatch.setattr(Table, "scan", real)
    assert len(seen) == (1 if mode == "soft" else 0)
    live = [r for r in d.fetch_rows("orders") if not r[d.c_deleted]]
    assert live == [] and len(d.fetch_rows("orders")) == (2 if mode == "soft" else 0)

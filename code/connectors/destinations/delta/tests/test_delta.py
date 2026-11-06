"""alelt_dst_delta: conformance on a local directory (always), on S3 via an in-process moto server (if installed)
and on MinIO when ALELT_TEST_DELTA_S3_URL is set (delta+s3://bucket/prefix?endpoint=...&access_key=...&secret_key=...)."""
from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

pytest.importorskip("deltalake")

from alelt_dst_delta import DeltaDestination  # noqa: E402
from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, TABLES, cs, e, p  # noqa: E402

_TMP = tempfile.mkdtemp(prefix="alelt_delta_")


class TestDeltaLocalConformance(DestinationConformance):
    decimal_widening = "none"
    atomic_scope = "table"

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return DeltaDestination("delta+file:///" + _TMP.replace("\\", "/"), schema=schema,
                                control_schema=f"{schema}_ctl", delete_mode=delete_mode)


def test_txn_and_commit_info() -> None:
    d = DeltaDestination("delta+file:///" + _TMP.replace("\\", "/"), schema=f"h_{uuid.uuid4().hex[:6]}")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("strm", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
    dt = d._dt(d.uri("orders"))
    assert dt.transaction_version("strm") == 1
    meta = [h for h in d.history("orders") if h.get("alelt.stream") == "strm"]
    assert meta and meta[0]["alelt.position"] == p(1).text and meta[0]["alelt.batch_id"] == "b1"


@pytest.mark.parametrize("mode", ["soft", "hard"])
def test_truncate_plus_ops_is_one_commit(mode: str) -> None:
    """DST-delta-03: TRUNCATE + rows in one batch = ONE Delta commit (txn properties on it), correct final state."""
    from alelt_dst_kit.conformance import ROW2

    sch = f"t_{uuid.uuid4().hex[:6]}"
    d = DeltaDestination("delta+file:///" + _TMP.replace("\\", "/"), schema=sch, control_schema=f"{sch}_ctl",
                         delete_mode=mode)
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("tr", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2)), p(2), "b1", MAP, TABLES)
    v0 = d._dt(d.uri("orders")).version()
    trunc = e(3, "TRUNCATE")
    d.apply("tr", cs(trunc, e(4, "INSERT", after={**ROW1, "NAME": "again"})), p(4), "b2", MAP, TABLES)
    dt = d._dt(d.uri("orders"))
    assert dt.version() == v0 + 1 and dt.transaction_version("tr") == 2
    live = [r for r in d.fetch_rows("orders") if not r[d.c_deleted]]
    assert [(r["id"], r["name"]) for r in live] == [(1, "again")]
    if mode == "soft":
        assert len(d.fetch_rows("orders")) == 2              # row 2 kept, flagged deleted


@pytest.fixture(scope="module")
def moto_url() -> Iterator[str]:
    pytest.importorskip("moto.server")
    from alelt_dst_kit.testing import moto_s3

    with moto_s3() as ep:
        yield f"delta+s3://lake/alelt?endpoint={ep}&access_key=x&secret_key=x&region=us-east-1"


class TestDeltaMotoS3Conformance(DestinationConformance):
    decimal_widening = "none"
    atomic_scope = "table"

    @pytest.fixture(autouse=True)
    def _url(self, moto_url: str) -> None:
        self.url = moto_url

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return DeltaDestination(self.url, schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


S3 = os.environ.get("ALELT_TEST_DELTA_S3_URL")


@pytest.mark.skipif(not S3, reason="ALELT_TEST_DELTA_S3_URL not set")
class TestDeltaMinioConformance(DestinationConformance):
    decimal_widening = "none"
    atomic_scope = "table"

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return DeltaDestination(S3 or "", schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


def test_maintenance_compacts_and_vacuums(tmp_path: Any) -> None:
    from alelt_dst_delta import DeltaDestination
    from alelt_dst_kit.conformance import INFO, MAP, ROW1, TABLES, cs, e, p

    d = DeltaDestination(f"delta+file:///{tmp_path.as_posix()}/m", schema="land")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    for i in range(1, 4):
        d.apply("s", cs(e(i, "INSERT", after={**ROW1, "ID": i})), p(i), f"b{i}", MAP, TABLES)
    res = d.maintain(retention_hours=0)
    assert res and all(v == "ok" for v in res.values()), res
    assert d.row_count("orders") == 3


def test_s3_defaults_to_conditional_put() -> None:
    from alelt_dst_delta import DeltaDestination

    d = DeltaDestination("delta+s3://b/p?endpoint=http://h:9000&access_key=a&secret_key=s")
    assert d.storage.get("conditional_put") == "etag" and "AWS_S3_ALLOW_UNSAFE_RENAME" not in d.storage
    u = DeltaDestination("delta+s3://b/p?endpoint=http://h:9000&unsafe_rename=1")
    assert u.storage.get("AWS_S3_ALLOW_UNSAFE_RENAME") == "true"

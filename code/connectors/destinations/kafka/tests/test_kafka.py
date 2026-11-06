"""alelt_dst_kafka: conformance on the in-process transactional MemoryBus (always) and on a real broker when
ALELT_TEST_KAFKA_URL is set (kafka://localhost:59092?topic_prefix=aleltt)."""
from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest

from alelt_dst_kafka import KafkaDestination, MemoryBus
from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, TABLES, cs, e, p

_BUS = MemoryBus()


class TestKafkaMemoryConformance(DestinationConformance):
    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return KafkaDestination("kafka://memory:9092?topic_prefix=aleltt", schema=schema, control_schema=f"{schema}_ctl",
                                delete_mode=delete_mode, bus=_BUS)


def test_message_format_and_tombstone() -> None:
    bus = MemoryBus()
    d = KafkaDestination("kafka://m:9092?topic_prefix=x", schema="s1", bus=bus, delete_mode="hard")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("st", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={**ROW1, "ID": 2})), p(2), "b1", MAP, TABLES)
    d.apply("st", cs(e(3, "DELETE", before={"ID": 2})), p(3), "b2", MAP, TABLES)
    msgs = bus.topics["x.s1.orders"]
    k, v = msgs[0]
    assert json.loads(k) == {"id": 1}
    m = json.loads(v)
    assert m["op"] == "u" and m["row"]["amt"] == "1.50" and m["row"]["d"] == "2026-01-02" and m["seq"] == 1
    assert m["position"] == p(1).text and m["row"]["_alelt_deleted"] is False
    assert msgs[-1] == (b'{"id":2}', None)                        # hard delete = tombstone
    ck = bus.topics["x.alelt_control.checkpoint"]
    assert json.loads(ck[-1][1])["position"] == p(3).text and ck[-1][0] == b"st"


URL = os.environ.get("ALELT_TEST_KAFKA_URL")


@pytest.mark.skipif(not URL, reason="ALELT_TEST_KAFKA_URL not set")
class TestKafkaBrokerConformance(DestinationConformance):
    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return KafkaDestination(URL or "", schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


@pytest.mark.skipif(not URL, reason="ALELT_TEST_KAFKA_URL not set")
def test_broker_aborted_txn_invisible() -> None:
    s = f"ab_{uuid.uuid4().hex[:6]}"
    d = KafkaDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)

    def boom(pt: str) -> None:
        if pt == "before_commit":
            raise RuntimeError("crash")
    d._hook = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        d.apply("st", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
    assert d.fetch_rows("orders") == [] and d.read_checkpoint("st") is None


def test_checkpoint_read_is_incremental() -> None:
    bus = MemoryBus()
    d = KafkaDestination("kafka://m:9092?topic_prefix=inc", schema="s", bus=bus)
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    for i in range(1, 6):
        d.apply("st", cs(e(i, "INSERT", after={**ROW1, "ID": i})), p(i), f"b{i}", MAP, TABLES)
    bus.reads = 0
    d.apply("st", cs(e(9, "INSERT", after={**ROW1, "ID": 9})), p(9), "b9", MAP, TABLES)
    assert bus.reads <= 2                                   # only the records since the last read
    other = KafkaDestination("kafka://m:9092?topic_prefix=inc", schema="s", bus=bus)
    assert other.read_checkpoint("st").text == p(9).text  # type: ignore[union-attr]


def test_truncate_marker_on_every_partition_and_order_independent_fold() -> None:
    """DST-kafka-05: a truncate writes one marker per partition; folding is independent of the partition read
    order (rows of the truncating batch and later survive)."""
    bus = MemoryBus()
    d = KafkaDestination("kafka://m:9092?topic_prefix=x&partitions=3", schema="s3", bus=bus)
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.apply("st", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={**ROW1, "ID": 2})), p(2), "b1", MAP, TABLES)
    d.apply("st", cs(e(3, "TRUNCATE"), e(4, "INSERT", after={**ROW1, "ID": 3})), p(4), "b2", MAP, TABLES)
    marks = [i for i, (k, _) in enumerate(bus.topics["x.s3.orders"]) if k == b"__alelt_truncate__"]
    assert len(marks) == 3
    assert sorted(x for x in bus.partitions_seen["x.s3.orders"] if x is not None) == [0, 1, 2]
    topic = bus.topics["x.s3.orders"]
    bus.topics["x.s3.orders"] = topic[marks[-1] + 1:] + topic[:marks[-1] + 1]   # rows of b2 read BEFORE the markers
    live = {r["id"] for r in d.fetch_rows("orders") if not r["_alelt_deleted"]}
    assert live == {3}

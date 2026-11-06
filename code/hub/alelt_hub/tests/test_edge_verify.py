"""F-03: edge verification through VERIFY_TABLE (command issued by the hub, answered by the agent's ack)."""
from __future__ import annotations

import threading
import uuid
from decimal import Decimal
from typing import Any

import pytest

from alelt_common import verify_edge as ve
from alelt_hub.testing import FakeAgent, ev, fake_catalog

from .conftest import TOKEN, drain, need_landing


class AgentLoop:
    """Agent side of VERIFY_TABLE: long-poll commands, answer from `truth` (the source rows), ack."""

    def __init__(self, client: Any, agent: FakeAgent, truth: dict[str, list[dict[str, Any]]]) -> None:
        self.c, self.a, self.truth, self.stop, self.seen = client, agent, truth, False, []
        self.t = threading.Thread(target=self.run, daemon=True)

    def run(self) -> None:
        while not self.stop:
            r = self.c.get(f"/agent/v1/commands?agent_id={self.a.agent_id}&wait=0", headers=self.a.h)
            for cmd in r.json():
                if cmd["name"] != "VERIFY_TABLE":
                    continue
                self.seen.append(cmd["args"])
                try:
                    a = ve.validate_args(cmd["args"])
                    fq = f"{a['table']['schema']}.{a['table']['name']}"
                    cls = {"ID": "int", "NAME": "str", "AMT": "dec", "SECRET": "str"}
                    res = ve.compute_from_rows(a, self.truth[fq], ["ID"], cls, lambda t: tuple(int(x) for x in t),
                                               "fake", lambda: "00000000000000000001")
                    body = {"ok": True, "result": res}
                except Exception as e:  # noqa: BLE001
                    body = {"ok": False, "result": {"message": str(e)}}
                self.c.post(f"/agent/v1/commands/{cmd['command_id']}/ack?agent_id={self.a.agent_id}",
                            headers=self.a.h, json=body)
            threading.Event().wait(0.1)

    def __enter__(self) -> "AgentLoop":
        self.t.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop = True
        self.t.join(5)


def test_transport_issue_poll_ack(client: Any) -> None:
    from alelt_hub import edge_verify
    from alelt_hub.db import get_engine

    a = FakeAgent(client, TOKEN, agent_id="verify-agent", connection_id="verifyconn")
    a.register()
    truth = {"S.KEYED": [{"ID": i, "NAME": f"n{i}", "AMT": Decimal("1.00"), "SECRET": None} for i in range(1, 6)]}
    call = edge_verify.caller(get_engine(), "verifyconn", "test", timeout_s=20)
    with AgentLoop(client, a, truth) as loop:
        out = call({"table": {"schema": "S", "name": "KEYED"}, "verify_id": str(uuid.uuid4()),
                    "checks": ["row_count", "aggregates"]})
        assert out["ranges"][0]["rows"] == 5
        assert out["ranges"][0]["agg"]["AMT"]["sum"] == "5.00"
        with pytest.raises(RuntimeError, match="refused"):          # the agent's refusal surfaces as an error
            call({"table": {"schema": "S", "name": "NOPE"}, "verify_id": str(uuid.uuid4()), "checks": ["row_count"]})
    assert loop.seen and all(set(x) <= ve._ARG_KEYS for x in loop.seen)
    with pytest.raises(ValueError):                                 # hub refuses non-whitelisted args (R5)
        edge_verify.issue(get_engine(), "verifyconn", {"table": {"schema": "S", "name": "KEYED"},
                                                       "verify_id": str(uuid.uuid4()), "checks": ["drop"]}, "x")


def test_edge_table_reconciled_against_destination(client: Any) -> None:
    need_landing()
    from alelt_hub import edge_verify
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    a = FakeAgent(client, TOKEN, agent_id="verify-agent2", connection_id="verifyconn2")
    a.register()
    a.catalog(fake_catalog("verifyconn2"))
    assert a.send([ev(1, "A", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one", "AMT": "1.50", "SECRET": "s"}),
                   ev(2, "A", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "two", "AMT": "2.00", "SECRET": "t"}),
                   ev(3, "A", "COMMIT")]).status_code == 200
    w = Worker(get_engine(), get_settings(), "verify-worker")
    drain(w)
    p = w.pipeline("verifyconn2")
    truth = {"S.KEYED": [{"ID": 1, "NAME": "one", "AMT": Decimal("1.50"), "SECRET": "s"},
                         {"ID": 2, "NAME": "two", "AMT": Decimal("2.00"), "SECRET": "t"}]}
    with AgentLoop(client, a, truth) as loop:
        out = edge_verify.verify_edge_table(get_engine(), p, "S.KEYED")
        assert out.outcome == "MATCH", [r for r in out.results if r.status.value != "PASS"]
        assert loop.seen[0]["checks"] == ["row_count", "aggregates", "rows"]
        truth["S.KEYED"].append({"ID": 3, "NAME": "three", "AMT": Decimal("3.00"), "SECRET": "u"})  # source ahead
        out = edge_verify.verify_edge_table(get_engine(), p, "S.KEYED")
        assert out.outcome == "MISMATCH"
        assert any(r.check.value == "KEY_SET" and r.key == '["3"]' for r in out.results)
        truth["S.KEYED"][0]["NAME"] = "uno"                         # value differs: located by the keyed re-read
        truth["S.KEYED"].pop()
        out = edge_verify.verify_edge_table(get_engine(), p, "S.KEYED")
        assert [(r.key, r.column) for r in out.results if r.check.value == "SAMPLED_ROWS" and r.status.value == "FAIL"
                ][:1] == [('["1"]', "NAME")]


def test_fail_fast_and_bounded_wait(client: Any) -> None:
    import socket
    import time

    from alelt_hub import edge_verify
    from alelt_hub.db import get_engine

    a = FakeAgent(client, TOKEN, agent_id="old-agent", connection_id="oldconn")
    client.post("/agent/v1/register", headers=a.h, json={
        "agent_id": "old-agent", "connection_id": "oldconn", "engine": "fake", "version": "0.0",
        "hostname": socket.gethostname(), "database": "FAKE", "capabilities": ["raw_log", "catalog"]}).raise_for_status()
    args = {"table": {"schema": "S", "name": "KEYED"}, "verify_id": str(uuid.uuid4()), "checks": ["row_count"]}
    t0 = time.monotonic()
    with pytest.raises(edge_verify.AgentUnavailable, match="does not support VERIFY_TABLE"):
        edge_verify.caller(get_engine(), "oldconn", "t", timeout_s=30)(args)          # capability: no wait at all
    assert time.monotonic() - t0 < 2
    b = FakeAgent(client, TOKEN, agent_id="mute-agent", connection_id="muteconn")
    b.register()
    t0 = time.monotonic()
    with pytest.raises(edge_verify.AgentUnavailable, match="no answer"):
        edge_verify.caller(get_engine(), "muteconn", "t", timeout_s=1)(args)           # per-call bound
    assert time.monotonic() - t0 < 5
    with pytest.raises(edge_verify.AgentUnavailable, match="no registered agent"):
        edge_verify.caller(get_engine(), "nobody", "t")(args)

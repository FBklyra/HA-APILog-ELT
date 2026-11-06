"""SRC-ora-02: one LogMiner session across mining windows (bounded source impact).

A fake connection plays V$ARCHIVED_LOG / V$LOG and records every DBMS_LOGMNR call: consecutive windows inside the
same logs re-run START_LOGMNR only (no ADD_LOGFILE, no END_LOGMNR); a log switch adds one file; logs entirely behind
the window are removed; an online log archived under another name resets the session once; an error resets it."""
from __future__ import annotations

from typing import Any

import pytest

from alelt_src_oracle.logminer import Miner


class FakeCursor:
    def __init__(self, db: "FakeDb") -> None:
        self.db = db
        self.rows: list[tuple[Any, ...]] = []
        self.arraysize = self.prefetchrows = 0

    def execute(self, sql: str, binds: dict[str, Any] | None = None) -> None:
        b = binds or {}
        self.db.calls.append(sql)
        if "SYS_CONTEXT" in sql or "CDB FROM" in sql:
            self.rows = [("ORCL",)] if "SYS_CONTEXT" in sql else [("NO",)]
        elif "SYSTIMESTAMP" in sql:
            self.rows = [("+00:00",)]
        elif "V$ARCHIVED_LOG" in sql:
            self.rows = [(1, seq, f, n, name) for seq, f, n, name in self.db.archived if n > b["s"] and f <= b["e"]]
        elif "FROM V$LOG L" in sql:
            self.rows = [(1, seq, f, n, name) for seq, f, n, name in self.db.online if n > b["s"] and f <= b["e"]]
        elif "START_LOGMNR" in sql and self.db.fail_start:
            self.db.fail_start = False
            raise RuntimeError("ORA-01291: missing log file")
        elif "V$LOGMNR_CONTENTS" in sql:
            self.db.contents_sql = sql
            self.rows = [(scn, "01", 6, "START", None, None, None, 0, 0, None, None, None, None, None, None)
                         for scn in (self.db.scns or [b["s"]])]
        else:
            self.rows = []

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self.rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class FakeDb:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.archived: list[tuple[int, int, int, str]] = []
        self.online: list[tuple[int, int, int, str]] = [(10, 1000, 10**12, "/redo/g1.log")]
        self.fail_start = False
        self.scns: list[int] = []
        self.contents_sql = ""

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def count(self, what: str) -> int:
        n = sum(what in c for c in self.calls)
        self.calls.clear()
        return n


def _mine(m: Miner, s: int, e: int) -> int:
    return len(list(m.window(s, e)))


def test_session_reused_across_windows_and_logs_added_incrementally() -> None:
    db = FakeDb()
    m = Miner(db, ["APP"])
    db.calls.clear()
    assert _mine(m, 1000, 1999) == 1
    calls = list(db.calls)
    assert sum("ADD_LOGFILE" in c for c in calls) == 1 and sum("START_LOGMNR" in c for c in calls) == 1
    assert not any("END_LOGMNR" in c for c in calls)
    db.calls.clear()
    for w in range(2, 6):                                  # same online log: START_LOGMNR only
        _mine(m, w * 1000, w * 1000 + 999)
    calls = list(db.calls)
    assert sum("START_LOGMNR" in c for c in calls) == 4
    assert not any("ADD_LOGFILE" in c or "END_LOGMNR" in c or "REMOVE_LOGFILE" in c for c in calls)
    # log switch: g1 still online (seq 10 ends at 6000), g2 = seq 11 -> one ADDFILE, no reset
    db.online = [(10, 1000, 6000, "/redo/g1.log"), (11, 6000, 10**12, "/redo/g2.log")]
    db.calls.clear()
    _mine(m, 5500, 6500)
    assert db.count("ADD_LOGFILE") == 1
    # window entirely in seq 11: seq 10 removed, nothing else
    _mine(m, 6501, 7000)
    calls = list(db.calls)
    assert sum("REMOVE_LOGFILE" in c for c in calls) == 1 and not any("END_LOGMNR" in c for c in calls)
    assert m.stats["sessions"] == 1 and m.stats["windows"] == 7


def test_online_log_archived_or_reused_resets_once() -> None:
    db = FakeDb()
    m = Miner(db, ["APP"])
    _mine(m, 1000, 1999)
    db.online = [(12, 9000, 10**12, "/redo/g1.log")]       # g1 reused for seq 12, seq 10 archived elsewhere
    db.archived = [(10, 1000, 9000, "/arch/1_10.arc")]
    db.calls.clear()
    _mine(m, 2000, 2999)
    calls = list(db.calls)
    assert sum("END_LOGMNR" in c for c in calls) == 1     # registered file now holds other redo
    assert sum("ADD_LOGFILE" in c for c in calls) == 1 and m.stats["sessions"] == 2
    db.calls.clear()
    _mine(m, 3000, 3999)
    assert not any("END_LOGMNR" in c or "ADD_LOGFILE" in c for c in db.calls)


def test_error_resets_session_and_next_window_reregisters() -> None:
    db = FakeDb()
    m = Miner(db, ["APP"])
    _mine(m, 1000, 1999)
    db.fail_start = True
    with pytest.raises(RuntimeError):
        _mine(m, 2000, 2999)
    db.calls.clear()
    _mine(m, 2000, 2999)
    assert db.count("ADD_LOGFILE") == 1
    m.close()
    assert db.count("END_LOGMNR") == 1


def test_catchup_windows_grow_and_shrink_back_when_caught_up() -> None:
    from alelt_src_oracle.connector import OracleSource

    src = OracleSource({"window_scns": 1000, "max_window_scns": 8000})
    w = 1000
    for _ in range(5):                                   # far behind, quick windows: 2k, 4k, 8k, capped
        w = src.next_window(w, 0, w, 10**9, 0.5)
    assert w == 8000
    assert src.next_window(w, 0, w, 10**9, 120.0) == 4000   # slow window: halve
    assert src.next_window(w, 0, 10**9, 10**9, 0.5) == 1000  # caught up: base size


def test_scn_regression_is_never_consumed_and_switches_to_ordered_query() -> None:
    """SRC-ora-07: a lower SCN after a higher one (RAC thread merge) stops the window (no out-of-order commit
    token); the next windows use ORDER BY SCN, RS_ID, SSN."""
    from alelt_src_oracle.logminer import ScnOrderError, contents_sql

    db = FakeDb()
    m = Miner(db, ["APP"])
    assert not m.ordered and "ORDER BY" not in contents_sql(1)
    db.scns = [1000, 1005, 1003]
    got: list[int] = []
    with pytest.raises(ScnOrderError):
        for r in m.window(1000, 1999):
            got.append(r["SCN"])
    assert got == [1000, 1005] and m.ordered
    db.scns = [1000, 1003, 1005]
    assert [r["SCN"] for r in m.window(1000, 1999)] == [1000, 1003, 1005]
    assert db.contents_sql.endswith("ORDER BY SCN, RS_ID, SSN")


def test_snapshot_json_keeps_document_key_order() -> None:
    """SRC-ora-10: snapshot JSON is not re-sorted (the CDC path keeps the document text)."""
    from alelt_src_oracle.types import convert_driver

    assert convert_driver({"b": 1, "a": "é"}, "JSON", None) == '{"b":1,"a":"é"}'


def test_current_position_reads_scn_before_and_after_open_transactions() -> None:
    """SRC-ora-01: low = CURRENT_SCN read BEFORE V$TRANSACTION, snapshot SCN after it; restart = min(oldest, low)."""
    from alelt_src_oracle.connector import OracleSource
    from alelt_src_oracle.position import Scn

    seq: list[str] = []
    answers = iter([100, None, 105])                     # no open transaction at the V$TRANSACTION read
    src = OracleSource()

    def one(sql: str) -> Any:
        seq.append("txn" if "V$TRANSACTION" in sql else "scn")
        return next(answers)
    src._one = one  # type: ignore[method-assign]
    p = Scn.parse(src.current_position().text)  # type: ignore[union-attr]
    assert seq == ["scn", "txn", "scn"] and p.mine_from == 100 and src._snapshot_scn == 105
    answers = iter([100, 90, 105])                       # a txn open since SCN 90: mined from its start
    assert Scn.parse(src.current_position().text).mine_from == 90  # type: ignore[union-attr]


@pytest.mark.parametrize("con,version,status", [("ORCLPDB1", "19.3.0.0.0", "FAIL"), ("ORCLPDB1", "21.3.0.0.0", "INFO"),
                                                ("CDB$ROOT", "19.3.0.0.0", "INFO")])
def test_pdb_mining_needs_21c(con: str, version: str, status: str) -> None:
    """SRC-ora-08: a PDB session on 19c cannot run LogMiner -> FAIL with the CDB$ROOT fix."""
    from alelt_src_oracle import prereq

    answers = {"V$DATABASE": ("ARCHIVELOG", "YES", "NO", "YES"), "CON_NAME": (con,),
               "PRODUCT_COMPONENT_VERSION": (version,)}

    class Cur:
        def execute(self, sql: str, *a: Any) -> None:
            self.r = next((v for k, v in answers.items() if k in sql), None)
            self.many = [("LOGMINING",)] if "SESSION_" in sql else []

        def fetchone(self) -> Any:
            return self.r

        def fetchall(self) -> Any:
            return self.many

    class Conn:
        def cursor(self) -> Cur:
            return Cur()

    out = {o["id"]: o for o in prereq.checks(Conn(), None, None, {"user": "c##cap"})}
    assert out["multitenant"]["status"] == status
    if status == "FAIL":
        assert "CDB$ROOT" in out["multitenant"]["fix_sql"]

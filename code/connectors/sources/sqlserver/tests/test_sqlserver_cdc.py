"""SQL Server CDC connector unit tests (no server needed).

Golden corpus: tests/golden/sqlserver/cdc_window.json = real ``fn_cdc_get_all_changes_<ci>(.., 'all update old')``
rows + ``cdc.lsn_time_mapping`` rows recorded from a dev server (record_sqlserver.py); expected.jsonl = reviewed
canonical events.
"""
from __future__ import annotations

import base64
import json
import sys
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "sqlserver")]

from alelt_common.canonical_jsonl import decode_batch, encode_events  # noqa: E402
from alelt_common.models import CatalogSnapshot, ChangeKind, Position, TableRef  # noqa: E402
from alelt_src_sqlserver import types as T  # noqa: E402
from alelt_src_sqlserver.assemble import CaptureInstance, TxnMeta, assemble, bit_set  # noqa: E402
from alelt_src_sqlserver.cdc import CdcReader, RetentionGapError, change_fn, lit  # noqa: E402
from alelt_src_sqlserver.connector import SqlServerSource  # noqa: E402
from alelt_src_sqlserver.position import MAX, Lsn, compare_lsn, increment  # noqa: E402
from alelt_src_sqlserver.prereq import checks  # noqa: E402

GOLD = ROOT / "tests" / "golden" / "sqlserver"


def dec(v: Any) -> Any:
    if not isinstance(v, dict):
        return v
    (k, s), = v.items()
    return {"$f": float, "$dec": Decimal, "$bin": base64.b64decode, "$uuid": uuid.UUID,
            "$ts": datetime.fromisoformat, "$ldt": datetime.fromisoformat, "$date": date.fromisoformat,
            "$time": time.fromisoformat}[k](s)


def load() -> tuple[list[dict[str, Any]], dict[str, CaptureInstance], dict[bytes, TxnMeta], CatalogSnapshot]:
    doc = json.loads((GOLD / "cdc_window.json").read_text(encoding="utf-8"))
    cat = CatalogSnapshot.model_validate(doc["catalog"])
    by = {t.table.fqn: t for t in cat.tables}
    inst = {n: CaptureInstance(name=n, table=by[d["table"]], captured=dict(d["captured"]), lob_cols=set(d["lob_cols"]))
            for n, d in doc["instances"].items()}
    rows = [{k: dec(v) for k, v in r.items()} for r in doc["rows"]]
    meta = {bytes.fromhex(m["start_lsn"]): TxnMeta(tran_id=bytes.fromhex(m["tran_id"]) if m["tran_id"] else None,
                                                    begin=dec(m["begin"]), end=dec(m["end"])) for m in doc["meta"]}
    return rows, inst, meta, cat


def test_golden_window_matches_expected() -> None:
    rows, inst, meta, _ = load()
    got = encode_events(assemble(rows, inst, meta, timezone.utc))
    assert got == (GOLD / "expected.jsonl").read_bytes()


def test_golden_semantics() -> None:
    rows, inst, meta, cat = load()
    evs = assemble(rows, inst, meta, timezone.utc)
    kinds = [(e.kind.value, e.table.name if e.table else None) for e in evs]
    # rolled-back txn and savepoint-undone row never appear; non-captured table ignored
    blob = encode_events(evs).decode()
    assert "rolled back" not in blob and "SAVEPOINT_UNDONE" not in blob and "ALELT_NOT_CAPTURED" not in blob
    assert kinds.count(("COMMIT", None)) == 6
    # whole txns contiguous, commit positions strictly increasing, all events of a txn share txid
    commits = [e for e in evs if e.kind is ChangeKind.COMMIT]
    assert all(compare_lsn(a.position.text, b.position.text) < 0 for a, b in zip(commits, commits[1:]))
    assert all(Lsn.parse(c.position.text).is_commit_token and c.commit_ts and c.commit_ts.tzinfo for c in commits)
    first = evs[0]
    assert first.after is not None
    assert first.after["AMOUNT"] == Decimal("1234.50") and first.after["TINY"] == 255
    assert first.after["BIG"] == 9007199254740993 and first.after["ACTIVE"] is True
    assert first.after["EVENT_TS"] == datetime(2026, 11, 4, 13, 0, 0, 123000, tzinfo=timezone.utc)
    assert first.after["CREATED"] == datetime(2026, 11, 4, 8, 0, 0, 123456)
    assert first.after["GUID"] == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
    assert first.after["RAW"] == b"\x00\xff\x10" and first.after["DOC"] == b"\xde\xad\xbe\xef"
    assert first.after["CUSTOMER"] == "Acmé Ltd ✓" and first.after["XDOC"] == '<a b="1">x</a>'
    ups = [e for e in evs if e.kind is ChangeKind.UPDATE]
    # non-LOB update: LOB columns not carried -> omitted + unchanged:
    assert ups[0].note == "unchanged:DOC,NOTES,XDOC" and "NOTES" not in (ups[0].after or {})
    assert ups[0].before and ups[0].before["STATUS"] == "N" and ups[0].after and ups[0].after["STATUS"] == "P"
    # LOB update: new LOB values present (DOC set to NULL)
    assert ups[1].after and ups[1].after["NOTES"] == "short now" and ups[1].after["DOC"] is None
    # PK update = DELETE old key + INSERT new key in one txn
    pk = [e for e in evs if e.table and e.table.name == "ALELT_ORDER_LINES" and e.kind is not ChangeKind.INSERT]
    assert pk[0].kind is ChangeKind.DELETE and pk[0].before and pk[0].before["LINE_NO"] == 2
    dl = [e for e in evs if e.kind is ChangeKind.DELETE and e.table and e.table.name == "ALELT_ORDERS"]
    assert dl and dl[0].before and dl[0].before["ORDER_ID"] % 100 == 2
    assert all(c.capture_enabled for c in cat.tables if c.table.name != "ALELT_NOT_CAPTURED")


def test_round_trip_decode() -> None:
    rows, inst, meta, cat = load()
    evs = assemble(rows, inst, meta, timezone.utc)
    res = SqlServerSource().decode(encode_events(evs), cat, uuid.uuid4())
    assert not res.quarantined
    assert [e.model_dump(exclude={"raw_ref"}) for e in res.events] == [e.model_dump(exclude={"raw_ref"}) for e in evs]


def test_undecodable_value_is_quarantined_not_dropped() -> None:
    rows, inst, meta, cat = load()
    bad = dict(rows[0])
    bad["RAW"] = "not binary"                     # driver gave a str for a varbinary column
    evs = assemble([bad], inst, meta, timezone.utc)
    res = decode_batch(encode_events(evs), "sqlserver", uuid.uuid4())
    assert len(res.quarantined) == 1 and res.quarantined[0].raw_hex
    assert [e.kind for e in res.events] == [ChangeKind.COMMIT]
    unk = dict(rows[0])
    unk["ci"] = "dbo_GONE"
    res = decode_batch(encode_events(assemble([unk], inst, meta, timezone.utc)), "sqlserver", uuid.uuid4())
    assert len(res.quarantined) == 1 and res.quarantined[0].position is not None
    assert "unknown capture instance" in bytes.fromhex(res.quarantined[0].raw_hex).decode("utf-8", "replace")


def test_update_without_after_image_raises() -> None:
    rows, inst, meta, _ = load()
    olds = [r for r in rows if r["__$operation"] == 3][:1]
    with pytest.raises(ValueError):
        assemble(olds, inst, meta, timezone.utc)


def test_positions_and_helpers() -> None:
    a = Lsn.parse("0000002700000828001C:00000027000008280018")
    t = Lsn(a.lsn, MAX)
    assert not a.is_commit_token and t.is_commit_token and compare_lsn(a.text, t.text) < 0
    assert Lsn.parse("0x0000002700000828001C").text.endswith(":" + "F" * 20)
    assert increment(b"\x00" * 9 + b"\xff") == b"\x00" * 8 + b"\x01\x00"
    assert bit_set(b"\x00\x01", 1) and not bit_set(b"\x00\x01", 2) and bit_set(b"\x01\x00", 9)
    assert lit(a.lsn) == "0x0000002700000828001C"
    with pytest.raises(Exception):
        change_fn("dbo_x; DROP TABLE y")
    src = SqlServerSource()
    assert src.parse_position("0000002700000828001c").text == t.text
    assert T.canonical_name("float", 53) == "DOUBLE" and T.canonical_name("sql_variant") == "STRING" and T.canonical_name("my_clr_udt") == "UNKNOWN"


# --------------------------------------------------------------------------- fake server for reader / stream
class FakeCur:
    def __init__(self, srv: FakeServer) -> None:
        self.srv, self.res, self.description = srv, [], None

    def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
        self.srv.log.append(sql)
        self.res = self.srv.answer(sql, args, self)

    def fetchone(self) -> Any:
        return self.res[0] if self.res else None

    def fetchall(self) -> list[Any]:
        return list(self.res)

    def fetchmany(self, n: int) -> list[Any]:
        out, self.res = list(self.res[:n]), list(self.res[n:])
        return out

    def nextset(self) -> bool:
        return False


class FakeServer:
    """Answers the reader's documented queries from the golden window."""

    def __init__(self, min_lsn: bytes | None = None) -> None:
        self.rows, self.inst, self.meta, _ = load()
        self.max = max(self.meta)
        self.min = min_lsn or min(self.meta)
        self.log: list[str] = []

    def cursor(self) -> FakeCur:
        return FakeCur(self)

    def answer(self, sql: str, args: tuple[Any, ...], cur: FakeCur) -> list[Any]:
        import re
        lits = [bytes.fromhex(h) for h in re.findall(r"0x([0-9A-F]{20})", sql)]
        if "fn_cdc_get_max_lsn" in sql:
            return [(self.max,)]
        if "fn_cdc_get_min_lsn" in sql:
            return [(self.min,)]
        if "TZOFFSET" in sql:
            return [(0,)]
        if "fn_cdc_map_lsn_to_time" in sql:
            return [(datetime(2026, 11, 4),)]
        if "sp_cdc_scan" in sql:
            return []
        if "SELECT MAX(start_lsn)" in sql:
            lo, hi = lits
            m = [k for k in sorted(self.meta) if lo < k <= hi][:int(re.search(r"TOP \((\d+)\)", sql).group(1))]
            return [(m[-1] if m else None,)]
        if "lsn_time_mapping" in sql:
            lo, hi = lits
            return [(k, m.begin, m.end, m.tran_id) for k, m in self.meta.items() if lo < k <= hi]
        if " UNION ALL " in sql and "EXISTS" in sql:          # active-instance probe (SRC-ms-02)
            out = []
            for part in sql.split(" UNION ALL "):
                ci = re.search(r"fn_cdc_get_all_changes_(\w+)", part).group(1)
                lo, hi = [bytes.fromhex(h) for h in re.findall(r"0x([0-9A-F]{20})", part)]
                if any(r["ci"] == ci and lo <= bytes(r["__$start_lsn"]) <= hi for r in self.rows):
                    out.append((ci,))
            return out
        if "fn_cdc_get_all_changes_" in sql:
            ci = re.search(r"fn_cdc_get_all_changes_(\w+)", sql).group(1)
            lo, hi = lits
            rs = [r for r in self.rows if r["ci"] == ci and lo <= bytes(r["__$start_lsn"]) <= hi]
            keys = [k for k in self.rows[0] if k != "ci"]
            allk = sorted({k for r in self.rows if r["ci"] == ci for k in r if k != "ci"}, key=lambda k: (
                keys.index(k) if k in keys else 99))
            cur.description = [(k,) for k in allk]
            return [tuple(r.get(k) for k in allk) for r in rs]
        raise AssertionError(sql)


def test_reader_windows_and_dev_scan() -> None:
    srv = FakeServer()
    rd = CdcReader(srv, srv.inst, dev_manual_scan=True, window_txns=2)
    after = b"\x00" * 10
    seen: list[bytes] = []
    while True:
        end, rows, meta = rd.read(after)
        if end is None:
            break
        assert len(meta) <= 2 and all(after < bytes(r["__$start_lsn"]) <= end for r in rows)
        seen.append(end)
        after = end
    assert len(seen) == 3 and any("sp_cdc_scan" in s for s in srv.log)
    assert not any("sp_cdc_scan" in s for s in FakeServer().log)


def test_retention_gap_raises() -> None:
    srv = FakeServer()
    srv.min = sorted(srv.meta)[2]
    for ci in srv.inst.values():
        ci.create_date = datetime(2026, 1, 1)
    rd = CdcReader(srv, srv.inst)
    with pytest.raises(RetentionGapError):
        rd.check_retention(sorted(srv.meta)[0])
    rd.check_retention(sorted(srv.meta)[2])             # resume at/after min_lsn: nothing purged that we need
    for ci in srv.inst.values():                         # instance created after the resume point: not a gap
        ci.create_date = datetime(2027, 1, 1)
    rd.check_retention(sorted(srv.meta)[0])


def _stream(src: SqlServerSource, srv: FakeServer, from_pos: Position | None, n_commits: int) -> list[Any]:
    src._instances = srv.inst
    src._connect = lambda: srv  # type: ignore[method-assign]
    out: list[Any] = []
    commits = 0
    for ev in src.stream(from_pos):
        if ev is None:
            if commits >= n_commits:
                break
            continue
        out.append(ev)
        commits += ev.kind is ChangeKind.COMMIT
    return out


def test_stream_resume_no_gap_no_duplicate() -> None:
    srv = FakeServer()
    full = _stream(SqlServerSource(settings={"window_txns": 2}), srv, Position(engine="sqlserver",
                                                                                text=lit(b"\x00" * 10)[2:]), 6)
    commits = [e for e in full if e.kind is ChangeKind.COMMIT]
    assert len(commits) == 6
    resumed = _stream(SqlServerSource(), FakeServer(), commits[2].position, 3)
    tail = full[full.index(commits[2]) + 1:]
    assert [e.model_dump() for e in resumed] == [e.model_dump() for e in tail]
    # from_pos=None: start at the current end -> nothing old is delivered
    assert _stream(SqlServerSource(), FakeServer(), None, 0) == []


def test_prereq_agent_stopped_is_fail_with_fix() -> None:
    class C:
        def __init__(self) -> None:
            self.q = ""

        def cursor(self) -> C:
            return self

        def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
            self.q = sql

        def fetchone(self) -> Any:
            q = self.q
            if "ProductVersion" in q:
                return ("15.0.2000.1574", "Azure SQL Edge Developer", "alelttest_cdc")
            if "is_cdc_enabled" in q:
                return (True, "OFF")
            if "dm_server_services" in q:
                return ("Stopped",)
            if "job_type = N'capture'" in q:
                return (1,)
            if "job_type = N'cleanup'" in q:
                return (4320,)
            if "IS_ROLEMEMBER(N'db_owner')" in q:
                return (1, "sa")
            return None

        def fetchall(self) -> list[Any]:
            return [("dbo_ALELT_ORDERS", "dbo", "ALELT_ORDERS", None, 1, 1, b"\x00" * 9 + b"\x05")]

    res = {r["id"]: r for r in checks(C(), None, [TableRef(schema="dbo", name="ALELT_ORDERS"),
                                                  TableRef(schema="dbo", name="MISSING")],
                                      {"database": "alelttest_cdc", "user": "alelt", "dev_manual_scan": True},
                                      resume_lsn=b"\x00" * 10)}
    assert res["agent"]["status"] == "FAIL" and "DEV / TEST ONLY" in res["agent"]["detail"]
    assert res["cdc_db"]["status"] == "PASS" and res["cdc_table:DBO.ALELT_ORDERS"]["status"] == "PASS"
    assert res["cdc_table:DBO.MISSING"]["status"] == "FAIL"
    assert "sp_cdc_enable_table" in res["cdc_table:DBO.MISSING"]["fix_sql"]
    assert res["snapshot_isolation"]["status"] == "FAIL" and res["resume_retained"]["status"] == "FAIL"
    assert all(r.get("fix_sql") for r in res.values() if r["status"] == "FAIL")


# ------------------------------------------------------------------ completeness (X-132, XC-32)
def _ci_table() -> Any:
    from alelt_common.models import CanonicalType as CT, ColumnInfo, TableInfo
    return TableInfo(table=TableRef(schema="dbo", name="t"), capture_enabled=True, columns=[
        ColumnInfo(name="id", ordinal=0, engine_type="int", canonical_type=CT.INT, key_seq=1),
        ColumnInfo(name="a", ordinal=1, engine_type="int", canonical_type=CT.INT),
        ColumnInfo(name="b", ordinal=2, engine_type="int", canonical_type=CT.INT)])


def _row(lsn: int, ci: str, op: int, **vals: Any) -> dict[str, Any]:
    return {"__$start_lsn": lsn.to_bytes(10, "big"), "__$seqval": lsn.to_bytes(10, "big"), "__$operation": op,
            "__$update_mask": b"\x07", "__$command_id": 1, "ci": ci, **vals}


def test_capture_instance_switch_reads_old_until_new_start_no_dup_no_gap() -> None:
    from alelt_common.completeness import Census
    t = _ci_table()
    old = CaptureInstance(name="dbo_t", table=t, captured={"id": 1, "a": 2}, start_lsn=(1).to_bytes(10, "big"),
                          valid_to=(20).to_bytes(10, "big"))
    new = CaptureInstance(name="dbo_t2", table=t, captured={"id": 1, "a": 2, "b": 3}, start_lsn=(20).to_bytes(10, "big"))
    rows = [_row(10, "dbo_t", 2, id=1, a=1), _row(20, "dbo_t", 2, id=2, a=2), _row(20, "dbo_t2", 2, id=2, a=2, b=9),
            _row(30, "dbo_t2", 2, id=3, a=3, b=3)]
    c = Census("sqlserver")
    evs = assemble(rows, {"dbo_t": old, "dbo_t2": new}, {}, census=c)
    ins = [e.after["id"] for e in evs if e.kind is ChangeKind.INSERT]  # type: ignore[index]
    assert ins == [1, 2, 3]                                   # id 2 once, from the newer instance
    assert ("cdc:2", "ignored") in {(r["record"], r["class"]) for r in c.rows()}
    from alelt_common.completeness import RecordClass, signal_of
    s = [signal_of(e) for e in evs if signal_of(e)]
    assert len(s) == 1 and s[0].cls is RecordClass.NEEDS_KEYED_REREAD and s[0].keys == [{"id": 1}]  # b not in old


def test_computed_columns_are_never_written_as_null() -> None:
    from alelt_common.completeness import RecordClass, signal_of
    t = _ci_table()
    ci = CaptureInstance(name="dbo_t", table=t, captured={"id": 1, "a": 2, "b": 3}, computed={"b"})
    evs = assemble([_row(10, "dbo_t", 2, id=1, a=10, b=None)], {"dbo_t": ci}, {})
    ins = next(e for e in evs if e.kind is ChangeKind.INSERT)
    assert ins.after == {"id": 1, "a": 10} and "unchanged:b" in (ins.note or "")
    sig = next(signal_of(e) for e in evs if signal_of(e))
    assert sig is not None and sig.cls is RecordClass.NEEDS_KEYED_REREAD and sig.keys == [{"id": 1}]


def test_instance_started_after_resume_holds_the_table() -> None:
    src = SqlServerSource()
    t = _ci_table()
    ci = CaptureInstance(name="dbo_t", table=t, captured={"id": 1, "a": 2, "b": 3}, start_lsn=(50).to_bytes(10, "big"))
    sigs = src._late_instances({"dbo_t": ci}, (10).to_bytes(10, "big"), resumed=True)
    assert len(sigs) == 1 and sigs[0].table == t.table and sigs[0].cls.value == "unsupported"
    assert src._late_instances({"dbo_t": ci}, (10).to_bytes(10, "big"), resumed=True) == []   # once
    assert SqlServerSource()._late_instances({"dbo_t": ci}, (60).to_bytes(10, "big"), resumed=True) == []
    evs = assemble([_row(55, "dbo_t", 2, id=1, a=1, b=1)], {"dbo_t": ci}, {}, pre_signals=sigs)
    assert evs[0].kind is ChangeKind.OTHER and evs[-1].kind is ChangeKind.COMMIT and len({e.txid for e in evs}) == 1


def test_catalog_refresh_also_while_busy() -> None:
    """SRC-ms-01: a new capture instance is found on a continuously busy source (refresh after windows too)."""
    srv = FakeServer()
    src = SqlServerSource(settings={"window_txns": 1, "catalog_s": 0})
    calls: list[int] = []

    def refresh() -> None:
        calls.append(1)
        src._instances = srv.inst
    src.read_catalog = refresh  # type: ignore[method-assign]
    _stream(src, srv, Position(engine="sqlserver", text=lit(b"\x00" * 10)[2:]), 3)
    assert calls                                            # busy windows only, no idle poll needed


def test_bare_table_names_must_be_unambiguous() -> None:
    """SRC-ms-06: `ORDERS` captures dbo.ORDERS only when no other captured schema has an ORDERS."""
    from alelt_src_sqlserver.catalog import table_filter

    present = {("dbo", "ORDERS"), ("archive", "ORDERS"), ("dbo", "LINES")}
    assert table_filter(["dbo.ORDERS", "lines"], present) == {("dbo", "ORDERS"), ("dbo", "LINES")}
    with pytest.raises(ValueError, match="several schemas"):
        table_filter(["ORDERS"], present)


def test_lsn_increment_saturates() -> None:
    """SRC-ms-07."""
    from alelt_src_sqlserver.position import increment

    assert increment(b"\xff" * 10) == b"\xff" * 10
    assert increment(b"\x00" * 9 + b"\x01") == b"\x00" * 9 + b"\x02"


def test_min_lsn_cached_between_windows_but_fresh_for_retention() -> None:
    """SRC-ms-02: fn_cdc_get_min_lsn once per instance per TTL for windows; check_retention always reads it."""
    srv = FakeServer()
    rd = CdcReader(srv, srv.inst)
    n0 = sum("fn_cdc_get_min_lsn" in q for q in getattr(srv, "queries", []))
    calls: list[str] = []
    real = rd._one

    def one(sql: str, args: tuple = ()) -> Any:  # type: ignore[type-arg]
        if "fn_cdc_get_min_lsn" in sql:
            calls.append(sql)
        return real(sql, args)
    rd._one = one  # type: ignore[method-assign]
    ci = sorted(srv.inst)[0]
    rd.min_lsn(ci)
    rd.min_lsn(ci)
    assert len(calls) == 1
    rd.min_lsn(ci, fresh=True)
    assert len(calls) == 2 and n0 >= 0


def test_quiet_instances_cost_one_probe_query_and_rows_are_fetched_in_chunks() -> None:
    """SRC-ms-02: with many capture instances one UNION ALL probe finds those with changes; only they are read,
    with bounded fetchmany chunks; the result equals the per-instance read."""
    srv = FakeServer()
    base = CdcReader(srv, srv.inst)
    base.probe_min_instances = 99
    after, end = b"\x00" * 10, srv.max
    want = base.rows(after, end)
    srv.log.clear()
    rd = CdcReader(srv, srv.inst)
    rd.probe_min_instances = 0
    rd.fetch_rows = 1
    got = rd.rows(after, end)
    assert sorted(map(str, got)) == sorted(map(str, want))
    changed = {r["ci"] for r in want}
    calls = [q for q in srv.log if "fn_cdc_get_all_changes_" in q and " UNION ALL " not in q and "EXISTS" not in q]
    assert len(calls) == len(changed) and sum("EXISTS" in q for q in srv.log) == 1


def test_server_tz_setting_gives_dst_correct_commit_times() -> None:
    """SRC-ms-04: lsn_time_mapping times are server-local; a named server_tz converts each commit time with the
    offset valid at that instant (DST), the default fixed offset does not."""
    from zoneinfo import ZoneInfo

    from alelt_src_sqlserver.connector import SqlServerSource

    tz = SqlServerSource({"server_tz": "Europe/Paris"})._server_tz()
    assert tz is not None
    winter = datetime(2026, 1, 15, 12, 0).replace(tzinfo=tz).astimezone(ZoneInfo("UTC"))
    summer = datetime(2026, 7, 15, 12, 0).replace(tzinfo=tz).astimezone(ZoneInfo("UTC"))
    assert (winter.hour, summer.hour) == (11, 10)
    srv = FakeServer()
    assert CdcReader(srv, srv.inst, server_tz=tz).server_tz is tz


def test_clr_and_variant_columns_are_text_by_key_reread_never_binary() -> None:
    """SRC-ms-05: hierarchyid / geography / geometry / sql_variant are STRING (documented text form via CAST); the
    CDC row carries their CLR serialization, so the column is omitted and re-read by key (never bytes, never NULL)."""
    from alelt_common.completeness import RecordClass, signal_of
    from alelt_common.models import CanonicalType, ColumnInfo, TableInfo
    from alelt_src_sqlserver import types as TT
    from alelt_src_sqlserver.catalog import _orderable, _sel

    assert TT.canonical_name("geography") == TT.canonical_name("hierarchyid") == "STRING"
    t = TableInfo(table=TableRef(schema="dbo", name="g"), capture_enabled=True, columns=[
        ColumnInfo(name="id", ordinal=1, engine_type="int", canonical_type=CanonicalType.INT, key_seq=1),
        ColumnInfo(name="geo", ordinal=2, engine_type="geography", canonical_type=CanonicalType.STRING)])
    ci = CaptureInstance(name="dbo_g", table=t, captured={"id": 1, "geo": 2})
    evs = assemble([_row(10, "dbo_g", 2, id=1, geo=b"\xe6\x10\x00\x00\x01\x0c")], {"dbo_g": ci}, {})
    ins = next(e for e in evs if e.kind is ChangeKind.INSERT)
    assert ins.after == {"id": 1} and "unchanged:geo" in (ins.note or "")
    sig = next(signal_of(e) for e in evs if signal_of(e))
    assert sig is not None and sig.cls is RecordClass.NEEDS_KEYED_REREAD and sig.keys == [{"id": 1}]
    geo = t.columns[1]
    assert "CAST([geo] AS nvarchar(max))" in _sel(geo) and not _orderable(geo)

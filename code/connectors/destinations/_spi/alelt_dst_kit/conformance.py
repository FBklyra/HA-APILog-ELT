"""Destination conformance suite (pytest mix-in). Every lake/warehouse/streaming plugin runs it:

    class TestClickHouseConformance(DestinationConformance):
        def make(self, schema: str, delete_mode: str = "soft") -> KitDestination: ...

Covered: bootstrap/ensure_table idempotent, upsert + soft delete + partial update, checkpoint, replay x3 = no-op,
replay of an older position = no-op, PK update, missing key => nothing written, crash mid-apply (fault at
'after_read' / 'after_data' / 'before_commit') => nothing visible and the retry applies exactly once, two
concurrent writers => exactly one commits (the other is rejected with CheckpointConflictError or, for the same
batch, is a no-op), stale `expected` rejected, per-table watermarks committed with the data, truncate, hard delete
mode, keyless surrogate id, type round trip, SPI verify helpers (row_count, column_aggregates per key range,
sample_rows, key_set / key_set_diff, deleted_keys, checksums), quarantine, raw archive, describe_schema. `fetch_rows()` must return physical (lower-case) column names.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from alelt_common.models import (CanonicalType, ChangeEvent, ChangeKind, ChangeSet, ColumnInfo, Position,
                              QuarantineRecord, RawRef, TableInfo, TableRef)
from alelt_common.spi import CheckpointConflictError

from .base import KitDestination

T = TableRef(schema="S", name="ORDERS")
INFO = TableInfo(table=T, columns=[
    ColumnInfo(name="ID", ordinal=0, engine_type="INTEGER", canonical_type=CanonicalType.INT, key_seq=1, nullable=False),
    ColumnInfo(name="NAME", ordinal=1, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=20),
    ColumnInfo(name="AMT", ordinal=2, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=9, scale=2),
    ColumnInfo(name="D", ordinal=3, engine_type="DATE", canonical_type=CanonicalType.LOCALDATE),
    ColumnInfo(name="TS", ordinal=4, engine_type="TIMESTAMP", canonical_type=CanonicalType.LOCALDATETIME),
    ColumnInfo(name="FLAG", ordinal=5, engine_type="BOOLEAN", canonical_type=CanonicalType.BOOLEAN),
])
K = TableRef(schema="S", name="EVLOG")
KEYLESS = TableInfo(table=K, columns=[
    ColumnInfo(name="MSG", ordinal=0, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=40),
    ColumnInfo(name="N", ordinal=1, engine_type="INTEGER", canonical_type=CanonicalType.INT),
])
MAP = {T: "orders", K: "evlog"}
TABLES = {T: INFO, K: KEYLESS}


X = TableRef(schema="S", name="EDGEV")
EDGEV = TableInfo(table=X, columns=[
    ColumnInfo(name="ID", ordinal=0, engine_type="INTEGER", canonical_type=CanonicalType.INT, key_seq=1, nullable=False),
    ColumnInfo(name="F", ordinal=1, engine_type="DOUBLE", canonical_type=CanonicalType.DOUBLE),
    ColumnInfo(name="S", ordinal=2, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=40),
    ColumnInfo(name="DEC", ordinal=3, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=38, scale=10),
    ColumnInfo(name="D", ordinal=4, engine_type="DATE", canonical_type=CanonicalType.LOCALDATE),
    ColumnInfo(name="TS", ordinal=5, engine_type="TIMESTAMP", canonical_type=CanonicalType.INSTANT),
])
XMAP = {X: "edgev"}
XTABLES = {X: EDGEV}
# XC-31 / DST-kit-15: values every source can deliver. Each engine must store the value EXACTLY or refuse the batch
# with an exception (checkpoint unchanged, nothing visible) - never store something else.
EDGE_VALUES: dict[str, dict[str, Any]] = {
    "nan": {"F": float("nan")},
    "inf": {"F": float("inf")},
    "neg_inf": {"F": float("-inf")},
    "nul_char": {"S": "a" + chr(0) + "b"},
    "big_decimal": {"DEC": Decimal("1234567890123456789012345678.0123456789")},
    "tiny_decimal": {"DEC": Decimal("-0.0000000001")},
    "date_min": {"D": date(1, 1, 1)},
    "date_max": {"D": date(9999, 12, 31)},
    "date_1900": {"D": date(1899, 12, 31)},
    "ts_min": {"TS": datetime(1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)},
    "ts_max": {"TS": datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)},
    "ts_offset": {"TS": datetime(2026, 3, 29, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=2)))},
    "ts_sub_us": {"TS": "2026-01-02 03:04:05.123456789+00:00"},   # TIMESTAMP(9..12) sources deliver text
}


def same(a: Any, b: Any) -> bool:
    if isinstance(b, float) and b != b:
        return a is not None and str(a).lower() in ("nan", "nan()") or (isinstance(a, float) and a != a)
    if isinstance(b, float) and b in (float("inf"), float("-inf")):
        try:
            return float(a) == b
        except (TypeError, ValueError):
            return False
    return norm(a) == norm(b)


class Crash(RuntimeError):
    pass


def p(n: int) -> Position:
    return Position(engine="db2", text=f"1:{n:016X}")


def e(n: int, kind: str, before: dict[str, Any] | None = None, after: dict[str, Any] | None = None,
      table: TableRef = T) -> ChangeEvent:
    return ChangeEvent(position=p(n), txid="t", kind=ChangeKind(kind), table=table, before=before, after=after)


def cs(*events: ChangeEvent) -> list[ChangeSet]:
    return [ChangeSet(txid="x", commit_position=events[-1].position, commit_ts=None, events=list(events))]


def norm(v: Any) -> Any:
    """Engine-neutral comparison form."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v)).normalize()
    if isinstance(v, datetime):                  # full microsecond precision; aware values compared in UTC
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(timespec="microseconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, str):
        s = v.strip()
        if len(s) >= 19 and s[4:5] == "-" and s[10:11] in (" ", "T") and s[13:14] == ":":   # ISO text (SQLite)
            try:
                return norm(datetime.fromisoformat(s))
            except ValueError:
                pass
        try:
            return Decimal(s).normalize() if s.replace(".", "", 1).lstrip("-").isdigit() else s
        except ArithmeticError:  # pragma: no cover
            return s
    return v


def truthy(v: Any) -> bool:
    return v in (True, 1, "1", "true", "True")


ROW1 = {"ID": 1, "NAME": "a", "AMT": Decimal("1.50"), "D": date(2026, 1, 2), "TS": datetime(2026, 1, 2, 3, 4, 5),
        "FLAG": True}
ROW2 = {"ID": 2, "NAME": "b", "AMT": None, "D": None, "TS": None, "FLAG": False}


class DestinationConformance:
    supports_hard_delete = True
    # 'batch': a crash leaves every table unchanged; 'table': each table is atomic on its own (lake formats without
    # multi-table commits) - a table is either fully old or fully new, the stream checkpoint stays old.
    atomic_scope = "batch"
    decimal_widening = "full"            # full | precision (same scale only: Iceberg) | none (Delta: no in-place change)
    reopen_needs_bootstrap = False

    # ---- to implement
    def make(self, schema: str, delete_mode: str = "soft") -> KitDestination:  # pragma: no cover
        raise NotImplementedError

    # ---- helpers
    def fresh(self, delete_mode: str = "soft") -> tuple[KitDestination, str]:
        schema = f"aleltc_{uuid.uuid4().hex[:8]}"
        d = self.make(schema, delete_mode)
        d.bootstrap()
        d.ensure_table(INFO, "orders", None)
        d.ensure_table(KEYLESS, "evlog", None)
        return d, schema

    def rows(self, d: KitDestination, dest: str = "orders") -> dict[Any, dict[str, Any]]:
        out = {}
        for r in d.fetch_rows(dest):
            k = norm(r["id"]) if dest == "orders" else r[d.c_row_id]
            out[k] = r
        return out

    def live(self, d: KitDestination, dest: str = "orders") -> dict[Any, dict[str, Any]]:
        return {k: r for k, r in self.rows(d, dest).items() if not truthy(r.get(d.c_deleted))}

    # ---- tests
    def test_bootstrap_idempotent(self) -> None:
        d, _ = self.fresh()
        d.bootstrap()
        d.ensure_table(INFO, "orders", None)
        assert d.read_checkpoint("s") is None
        assert d.row_count("orders") == 0

    def test_upsert_soft_delete_partial_replay(self) -> None:
        d, _ = self.fresh()
        sets = cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2), e(3, "DELETE", before={"ID": 2}))
        st = d.apply("s1", sets, p(3), "b1", MAP, TABLES)
        assert st and st[0].inserts >= 1 and st[0].deletes == 1
        for _ in range(3):                                           # replay x3 = no-op
            assert d.apply("s1", sets, p(3), "b1", MAP, TABLES) == []
        rows = self.rows(d)
        assert norm(rows[norm(1)]["amt"]) == norm(Decimal("1.50")) and not truthy(rows[norm(1)][d.c_deleted])
        assert truthy(rows[norm(2)][d.c_deleted]) and norm(rows[norm(2)]["name"]) == "b"   # soft delete keeps row
        assert d.read_checkpoint("s1").text == p(3).text  # type: ignore[union-attr]
        upd = cs(e(4, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "z"}))          # partial image
        st = d.apply("s1", upd, p(4), "b2", MAP, TABLES)
        assert st[0].updates == 1
        r1 = self.rows(d)[norm(1)]
        assert norm(r1["name"]) == "z" and norm(r1["amt"]) == norm(Decimal("1.50"))       # other columns kept
        assert d.row_count("orders") == 1 and d.row_count("orders", include_deleted=True) == 2
        assert d.read_checkpoint("s1").text == p(4).text  # type: ignore[union-attr]
        assert d.apply("s1", sets, p(3), "b1", MAP, TABLES) == []                         # older batch: no-op
        if hasattr(d, "audit_rows"):
            assert len(d.audit_rows("s1")) == 2

    def test_pk_update(self) -> None:
        d, _ = self.fresh()
        d.apply("s", cs(e(1, "INSERT", after={**ROW1, "ID": 10})), p(1), "b1", MAP, TABLES)
        d.apply("s", cs(e(2, "UPDATE", before={**ROW1, "ID": 10}, after={**ROW1, "ID": 11, "NAME": "moved"})),
                p(2), "b2", MAP, TABLES)
        rows = self.rows(d)
        assert truthy(rows[norm(10)][d.c_deleted])
        assert not truthy(rows[norm(11)][d.c_deleted]) and norm(rows[norm(11)]["name"]) == "moved"

    def test_missing_key_writes_nothing(self) -> None:
        d, _ = self.fresh()
        bad = cs(e(8, "INSERT", after={"ID": 7, "NAME": "ok"}), e(9, "INSERT", after={"NAME": "no key"}))
        with pytest.raises(ValueError):
            d.apply("s2", bad, p(9), "bx", MAP, TABLES)
        assert d.row_count("orders", include_deleted=True) == 0 and d.read_checkpoint("s2") is None

    @pytest.mark.parametrize("point", ["after_read", "after_data", "before_commit"])
    def test_crash_mid_apply_nothing_partial(self, point: str) -> None:
        d, schema = self.fresh()
        d.apply("s", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        sets = cs(e(2, "INSERT", after=ROW2), e(3, "UPDATE", before={"ID": 1}, after={**ROW1, "NAME": "new"}),
                  e(4, "INSERT", after={"MSG": "m", "N": 1}, table=K))

        def boom(pt: str) -> None:
            if pt == point:
                raise Crash(pt)
        d._hook = boom  # type: ignore[method-assign]
        with pytest.raises(Crash):
            d.apply("s", sets, p(4), "b2", MAP, TABLES)
        r = self.make(schema)                                     # new process after the crash
        if self.reopen_needs_bootstrap:
            r.bootstrap()
        assert r.read_checkpoint("s").text == p(1).text  # type: ignore[union-attr]
        live = self.live(r)
        if self.atomic_scope == "batch":
            assert set(live) == {norm(1)} and norm(live[norm(1)]["name"]) == "a"
            assert not self.live(r, "evlog")
        else:                                                     # per-table atomic: old or new, never mixed
            old = set(live) == {norm(1)} and norm(live[norm(1)]["name"]) == "a"
            new = set(live) == {norm(1), norm(2)} and norm(live[norm(1)]["name"]) == "new"
            assert old or new, live
            assert len(self.live(r, "evlog")) in (0, 1)
        r.ensure_table(INFO, "orders", None)
        r.ensure_table(KEYLESS, "evlog", None)
        assert r.apply("s", sets, p(4), "b2", MAP, TABLES)            # retry applies
        for _ in range(3):
            assert r.apply("s", sets, p(4), "b2", MAP, TABLES) == []
        live = self.live(r)
        assert set(live) == {norm(1), norm(2)} and norm(live[norm(1)]["name"]) == "new"
        assert len(self.live(r, "evlog")) == 1
        assert r.read_checkpoint("s").text == p(4).text  # type: ignore[union-attr]

    def test_truncate_soft(self) -> None:
        d, _ = self.fresh()
        assert d.load_snapshot("orders", INFO, [{**ROW1}, {**ROW2}]) == 2
        assert d.row_count("orders") == 2
        d.apply("s3", cs(e(5, "TRUNCATE")), p(5), "bt", MAP, TABLES)
        assert d.row_count("orders") == 0 and d.row_count("orders", include_deleted=True) == 2
        d.apply("s3", cs(e(6, "INSERT", after=ROW1)), p(6), "b6", MAP, TABLES)
        assert set(self.live(d)) == {norm(1)}

    def test_hard_delete_mode(self) -> None:
        if not self.supports_hard_delete:
            pytest.skip("hard delete not supported")
        d, _ = self.fresh("hard")
        d.apply("h", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2)), p(2), "b1", MAP, TABLES)
        d.apply("h", cs(e(3, "DELETE", before={"ID": 2})), p(3), "b2", MAP, TABLES)
        assert set(self.rows(d)) == {norm(1)}
        assert d.row_count("orders", include_deleted=True) == 1

    def test_keyless_surrogate(self) -> None:
        d, _ = self.fresh()
        a, b = {"MSG": "x", "N": 1}, {"MSG": "y", "N": 2}
        d.apply("k", cs(e(1, "INSERT", after=a, table=K), e(2, "INSERT", after=b, table=K)), p(2), "b1", MAP, TABLES)
        d.apply("k", cs(e(3, "DELETE", before=a, table=K)), p(3), "b2", MAP, TABLES)
        rows = self.rows(d, "evlog")
        assert len(rows) == 2 and all(len(str(k)) == 32 for k in rows)
        assert sorted(norm(r["msg"]) for r in self.live(d, "evlog").values()) == ["y"]

    def test_types_roundtrip_and_verify(self) -> None:
        d, _ = self.fresh()
        d.apply("t", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={**ROW2, "AMT": Decimal("2.25")})),
                p(2), "b1", MAP, TABLES)
        r = self.rows(d)[norm(1)]
        assert norm(r["d"]) == "2026-01-02" and norm(r["ts"]) == "2026-01-02T03:04:05.000000"
        assert truthy(r["flag"]) and norm(r["amt"]) == norm(Decimal("1.5"))
        assert r.get(d.c_synced) is not None
        ag = d.aggregates("orders", ["AMT"])
        assert ag["count"] == 2 and norm(ag["min:AMT"]) == norm(Decimal("1.5"))
        assert ag["sum:AMT"] is None or norm(ag["sum:AMT"]) == norm(Decimal("3.75"))   # SQLite: no exact SUM
        assert len(d.sample("orders", ["ID"], 1)) == 1
        assert sum(1 for _ in d.key_bucket_hashes("orders", ["ID"], 4)) >= 1

    # ---- F-05: compare-and-set of the checkpoint inside the destination commit
    def second(self, schema: str) -> KitDestination:
        """Another process/worker on the same destination."""
        d2 = self.make(schema)
        if self.reopen_needs_bootstrap:
            d2.bootstrap()
        d2.ensure_table(INFO, "orders", None)
        d2.ensure_table(KEYLESS, "evlog", None)
        return d2

    def test_concurrent_writers_exactly_one_commits(self) -> None:
        d1, schema = self.fresh()
        d1.apply("cw", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d2 = self.second(schema)
        mine = cs(e(2, "UPDATE", before={"ID": 1}, after={**ROW1, "NAME": "w1"}), e(4, "INSERT", after=ROW2))
        other = cs(e(2, "UPDATE", before={"ID": 1}, after={**ROW1, "NAME": "w2"}))
        ran: list[Any] = []

        def race(pt: str) -> None:            # d2 commits after d1 read the checkpoint, before d1 writes anything
            if pt == "after_read" and not ran:
                ran.append(d2.apply("cw", other, p(3), "b-other", MAP, TABLES))
        d1._hook = race  # type: ignore[method-assign]
        with pytest.raises(CheckpointConflictError):
            d1.apply("cw", mine, p(4), "b-mine", MAP, TABLES)
        assert ran and ran[0]                                     # exactly one writer committed: d2
        r = self.make(schema)
        if self.reopen_needs_bootstrap:
            r.bootstrap()
        assert r.read_checkpoint("cw").text == p(3).text  # type: ignore[union-attr]
        live = self.live(r)
        assert set(live) == {norm(1)} and norm(live[norm(1)]["name"]) == "w2"
        if hasattr(r, "audit_rows"):
            assert len(r.audit_rows("cw")) == 2
        # the rejected writer rebuilds from the destination checkpoint and continues
        r.ensure_table(INFO, "orders", None)
        r.ensure_table(KEYLESS, "evlog", None)
        assert r.apply("cw", cs(e(4, "INSERT", after=ROW2)), p(4), "b-next", MAP, TABLES, expected=p(3))
        assert set(self.live(r)) == {norm(1), norm(2)}

    def test_duplicate_writer_same_batch_is_noop(self) -> None:
        d1, schema = self.fresh()
        d2 = self.second(schema)
        batch = cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2))
        ran: list[Any] = []

        def race(pt: str) -> None:            # stale job requeue: the same batch applied twice concurrently
            if pt == "after_read" and not ran:
                ran.append(d2.apply("dup", batch, p(2), "b1", MAP, TABLES))
        d1._hook = race  # type: ignore[method-assign]
        assert d1.apply("dup", batch, p(2), "b1", MAP, TABLES) == []
        assert ran and ran[0]
        assert d1.read_checkpoint("dup").text == p(2).text  # type: ignore[union-attr]
        assert set(self.live(d1)) == {norm(1), norm(2)}
        if hasattr(d1, "audit_rows"):
            assert len(d1.audit_rows("dup")) == 1

    def test_expected_checkpoint_mismatch_rejected(self) -> None:
        d, _ = self.fresh()
        assert d.apply("ex", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES, expected=None)
        with pytest.raises(CheckpointConflictError):               # batch planned from a stale checkpoint
            d.apply("ex", cs(e(3, "INSERT", after=ROW2)), p(3), "b3", MAP, TABLES, expected=p(0))
        assert d.read_checkpoint("ex").text == p(1).text  # type: ignore[union-attr]
        assert set(self.live(d)) == {norm(1)}
        assert d.apply("ex", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES, expected=None) == []  # replay
        assert d.apply("ex", cs(e(3, "INSERT", after=ROW2)), p(3), "b3", MAP, TABLES, expected=p(1))
        assert d.apply("ex", cs(e(2, "INSERT", after=ROW2)), p(2), "b2", MAP, TABLES) == []    # older: no-op
        assert d.read_checkpoint("ex").text == p(3).text  # type: ignore[union-attr]

    def test_per_table_watermarks(self) -> None:
        d, schema = self.fresh()
        d.apply("wm", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={"MSG": "m", "N": 1}, table=K)),
                p(2), "b1", MAP, TABLES)
        wm = {t: x.text for t, x in d.read_watermarks("wm").items()}
        assert wm == {"orders": p(2).text, "evlog": p(2).text}
        d.apply("wm", cs(e(3, "INSERT", after=ROW2)), p(3), "b2", {T: "orders"}, TABLES)   # only orders moves
        wm = {t: x.text for t, x in d.read_watermarks("wm").items()}
        assert wm == {"orders": p(3).text, "evlog": p(2).text}
        d.apply("wm", cs(e(4, "UPDATE", before={"ID": 2}, after={"ID": 2, "NAME": "q"})), p(4), "b3", MAP, TABLES,
                watermarks={"orders": p(4), "evlog": p(1)})                              # never moves back
        wm = {t: x.text for t, x in d.read_watermarks("wm").items()}
        assert wm == {"orders": p(4).text, "evlog": p(2).text}

        def boom(pt: str) -> None:
            if pt == "before_commit":
                raise Crash(pt)
        d._hook = boom  # type: ignore[method-assign]
        with pytest.raises(Crash):
            d.apply("wm", cs(e(5, "INSERT", after={"MSG": "z", "N": 2}, table=K)), p(5), "b4", MAP, TABLES)
        r = self.make(schema)
        if self.reopen_needs_bootstrap:
            r.bootstrap()
        wm = {t: x.text for t, x in r.read_watermarks("wm").items()}
        assert wm == {"orders": p(4).text, "evlog": p(2).text}                          # atomic with the data

    def test_typed_composite_keys_partial_update(self) -> None:
        """Decimal + date + timestamp composite key: the partial update finds the current row (no NULL-fill,
        DST-kit-02 / DST-delta-05 / ClickHouse typed lookup)."""
        tk = TableRef(schema="S", name="TYPED")
        info = TableInfo(table=tk, columns=[
            ColumnInfo(name="K1", ordinal=0, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=10,
                       scale=2, key_seq=1, nullable=False),
            ColumnInfo(name="K2", ordinal=1, engine_type="DATE", canonical_type=CanonicalType.LOCALDATE, key_seq=2,
                       nullable=False),
            ColumnInfo(name="K3", ordinal=2, engine_type="TIMESTAMP", canonical_type=CanonicalType.LOCALDATETIME,
                       key_seq=3, nullable=False),
            ColumnInfo(name="V", ordinal=3, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=20),
            ColumnInfo(name="W", ordinal=4, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=20)])
        d, _ = self.fresh()
        d.ensure_table(info, "typed", None)
        key = {"K1": Decimal("12.50"), "K2": date(2026, 3, 4), "K3": datetime(2026, 3, 4, 5, 6, 7, 123456)}
        mp, tb = {tk: "typed"}, {tk: info}
        d.apply("tk", cs(e(1, "INSERT", after={**key, "V": "a", "W": "keep"}, table=tk)), p(1), "b1", mp, tb)
        d.apply("tk", cs(e(2, "UPDATE", before=key, after={**key, "V": "b"}, table=tk)), p(2), "b2", mp, tb)
        (row,) = [r for r in d.fetch_rows("typed") if not truthy(r.get(d.c_deleted))]
        assert norm(row["v"]) == "b" and norm(row["w"]) == "keep"
        assert norm(row["k3"]) == "2026-03-04T05:06:07.123456"

    def test_schema_qualified_access_reference_landing(self) -> None:
        """X-124: rows / counts / aggregates / key sets of ANOTHER schema of the same destination."""
        ref, ref_schema = self.fresh()
        ref.apply("r", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={**ROW2, "AMT": Decimal("2.00")})),
                  p(2), "b1", MAP, TABLES)
        d, _ = self.fresh()
        rs = ref.schema
        rows = d.fetch_rows_in(rs, "orders")
        assert sorted(norm(r["id"]) for r in rows) == [norm(1), norm(2)]
        assert d.row_count("orders", schema=rs) == 2 and d.row_count("orders") == 0
        assert [norm(k[0]) for k in d.key_set("orders", ["ID"], schema=rs)] == [norm(1), norm(2)]
        ag = {k.lower(): v for k, v in d.column_aggregates("orders", ["ID"], schema=rs).items()}
        assert ag["_rows"] == 2 and norm(ag["id"]["max"]) == norm(2)
        assert set(d.sample_rows("orders", ["ID"], ["NAME"], [(1,)], schema=rs)) == {(1,)}

    def test_type_widening_on_drift(self) -> None:
        """VARCHAR(20) -> VARCHAR(200) and DECIMAL(9,2) -> DECIMAL(12,4) at the source: ensure_table widens the
        landing columns (or the engine has no length limit) and longer / more precise values land intact."""
        d, _ = self.fresh()
        d.apply("w", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        amt = {"full": {"length": 12, "scale": 4}, "precision": {"length": 14, "scale": 2},
               "none": {"length": 9, "scale": 2}}[self.decimal_widening]
        cols = [c.model_copy(update={"length": 200}) if c.name == "NAME" else
                c.model_copy(update=amt) if c.name == "AMT" else c for c in INFO.columns]
        wide = INFO.model_copy(update={"columns": cols})
        d.ensure_table(wide, "orders", None)
        long = "n" * 150
        big = {"full": Decimal("12345678.1234"), "precision": Decimal("123456789012.25"),
               "none": Decimal("1234567.25")}[self.decimal_widening]
        d.apply("w", cs(e(2, "INSERT", after={**ROW1, "ID": 2, "NAME": long, "AMT": big})),
                p(2), "b2", MAP, {T: wide, K: KEYLESS})
        r = self.rows(d)[norm(2)]
        assert norm(r["name"]) == long and norm(r["amt"]) == norm(big)
        assert norm(self.rows(d)[norm(1)]["amt"]) == norm(Decimal("1.50"))

    def test_delete_checkpoint_transient_stream(self) -> None:
        d, _ = self.fresh()
        d.apply("cid#pass:1", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES,
                watermarks={"cid#t:S.ORDERS": None})
        assert {k: v.text for k, v in d.read_watermarks("cid#pass:1").items()} == {"cid#t:S.ORDERS": p(1).text}
        d.apply("cid", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d.delete_checkpoint("cid#pass:1")
        assert d.read_checkpoint("cid#pass:1") is None
        assert d.read_checkpoint("cid").text == p(1).text  # type: ignore[union-attr]

    def test_verify_helpers_spi(self) -> None:
        d, _ = self.fresh()
        rows = [{**ROW1, "ID": i, "AMT": Decimal(i), "NAME": f"n{i}"} for i in range(1, 11)]
        d.apply("v", cs(*(e(i, "INSERT", after=r) for i, r in enumerate(rows, 1)), e(11, "DELETE", before={"ID": 10})),
                p(11), "b1", MAP, TABLES)
        assert d.row_count("orders") == 9 and d.row_count("orders", include_deleted=True) == 10
        ag = d.column_aggregates("orders", ["ID", "AMT"], key_col="ID", key_range=(3, 5))
        assert ag["_rows"] == 3
        a = {k.lower(): v for k, v in ag.items() if k != "_rows"}
        assert norm(a["id"]["min"]) == norm(3) and norm(a["id"]["max"]) == norm(5)
        assert norm(a["id"]["sum"]) == norm(12) and a["amt"]["count"] == 3
        assert a["amt"].get("sum") is None or norm(a["amt"]["sum"]) == norm(12)   # SQLite: DECIMAL kept as text
        first = d.sample_rows("orders", ["ID"], n=3, key_range=(4, None))
        assert [norm(r["id"]) for r in first] == [norm(4), norm(5), norm(6)]
        by_key = d.sample_rows("orders", ["ID"], ["NAME"], [(2,), (7,), (99,)])
        assert set(by_key) == {(2,), (7,)} and norm(by_key[(7,)]["name"]) == "n7"
        ks = d.key_set("orders", ["ID"], key_range=(1, 3))
        assert [norm(k[0]) for k in ks] == [norm(1), norm(2), norm(3)]
        diff = d.key_set_diff("orders", ["ID"], [(1,), (2,), (42,)], key_range=(1, 50))
        assert [norm(k[0]) for k in diff["missing"]] == [norm(42)]
        assert sorted(norm(k[0]) for k in diff["extra"]) == sorted(norm(i) for i in range(3, 10))
        assert [norm(k[0]) for k in d.deleted_keys("orders", ["ID"])] == [norm(10)]
        assert d.key_bucket_hashes("orders", ["ID"], 4)
        ck = d.column_checksums("orders", ["ID"], ["NAME"], 4)
        assert ck["NAME"] and ck == d.column_checksums("orders", ["ID"], ["NAME"], 4)

    def test_quarantine_and_raw_archive(self) -> None:
        d, _ = self.fresh()
        recs = [QuarantineRecord(position=p(1), table=T, reason="undecodable sub-record 0xC4",
                                 raw_ref=RawRef(batch_id=uuid.UUID(int=1), offset=10, length=20), raw_hex="c4ff"),
                QuarantineRecord(position=None, table=None, reason="not in catalog", raw_ref=None)]
        assert d.quarantine("q", recs) == 2
        d.quarantine("q", recs)                                       # idempotent per record
        got = d.quarantined("q")
        assert len(got) == 2 and {g["reason"] for g in got} == {"undecodable sub-record 0xC4", "not in catalog"}
        body = bytes(range(256)) * 3
        ref = d.archive_raw("conn1", "batch-1", body, {"from_lri": "1:0A", "codec": "zstd"}, date(2026, 11, 4))
        assert "conn1" in ref and "2026/11/04" in ref
        d.archive_raw("conn1", "batch-1", body, {"from_lri": "1:0A", "codec": "zstd"}, date(2026, 11, 4))
        got_b, hdr = d.raw_archived("conn1", "batch-1")  # type: ignore[misc]
        assert got_b == body and hdr["from_lri"] == "1:0A"

    def test_special_characters_round_trip(self) -> None:
        """DST-kit-15: quotes, backslashes, newlines, tabs and non-ASCII text survive every engine unchanged."""
        d, _ = self.fresh()
        name = "O'Br\\ien \"x\"\n\té中"
        d.apply("sc", cs(e(1, "INSERT", after={**ROW1, "NAME": name})), p(1), "b1", MAP, TABLES)
        assert norm(self.live(d)[norm(1)]["name"]) == norm(name)

    def test_empty_batch_advances_the_checkpoint(self) -> None:
        """DST-kit-15: a flush with no rows (all filtered) still commits its position (held tables replay later)."""
        d, _ = self.fresh()
        d.apply("eb", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d.apply("eb", [], p(5), "b2", MAP, TABLES)
        assert d.read_checkpoint("eb").text == p(5).text  # type: ignore[union-attr]
        assert set(self.live(d)) == {norm(1)}

    # ---- XC-31 / DST-kit-15: edge values, time zones, snapshot then CDC
    edge_outcomes: dict[str, str] = {}   # optional per-engine expectation: case -> "exact" | "refused"

    def test_edge_values_stored_exactly_or_refused_loudly(self) -> None:
        d, _ = self.fresh()
        d.ensure_table(EDGEV, "edgev", None)
        got: dict[str, str] = {}
        for i, (case, vals) in enumerate(EDGE_VALUES.items(), start=1):
            stream = f"ev{i}"
            row = {"ID": i, **vals}
            try:
                d.apply(stream, cs(e(i, "INSERT", after=row, table=X)), p(i), f"b{i}", XMAP, XTABLES)
            except Exception:  # noqa: BLE001 - refusing loudly is allowed; storing something else is not
                assert d.read_checkpoint(stream) is None, f"{case}: refused but the checkpoint moved"
                assert not [r for r in d.fetch_rows("edgev") if norm(r["id"]) == norm(i)], f"{case}: partial write"
                got[case] = "refused"
                continue
            stored = next(r for r in d.fetch_rows("edgev") if norm(r["id"]) == norm(i))
            for col, want in vals.items():
                assert same(stored[col.lower()], want), f"{case}: stored {stored[col.lower()]!r}, sent {want!r}"
            got[case] = "exact"
        out = os.environ.get("ALELT_CONFORMANCE_MATRIX")        # docs: per-engine matrix (README of the kit)
        if out:
            name = f"{type(self).__name__}:{getattr(getattr(d, 'dialect', None), 'name', '') or d.engine}"
            with open(out, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"engine": name, "outcomes": got}) + chr(10))
        for case, want in self.edge_outcomes.items():
            assert got[case] == want, f"{case}: {got[case]} (documented {want})"

    def test_instant_offsets_compare_in_utc(self) -> None:
        """DST-kit-15: an INSTANT sent with an offset is the same instant when read back (any storage zone)."""
        d, _ = self.fresh()
        d.ensure_table(EDGEV, "edgev", None)
        ts = datetime(2026, 10, 25, 2, 30, 0, 5, tzinfo=timezone(timedelta(hours=-5)))
        d.apply("tz", cs(e(1, "INSERT", after={"ID": 1, "TS": ts}, table=X)), p(1), "b1", XMAP, XTABLES)
        stored = next(r for r in d.fetch_rows("edgev") if norm(r["id"]) == norm(1))["ts"]
        assert norm(stored) == norm(ts)

    def test_snapshot_then_cdc_on_the_same_keys(self) -> None:
        """DST-kit-15: rows loaded by load_snapshot are updated / deleted by later CDC like any other row."""
        d, _ = self.fresh()
        d.load_snapshot("orders", INFO, [{**ROW1}, {**ROW2}])
        d.apply("sn", cs(e(1, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "cdc"}),
                         e(2, "DELETE", before={"ID": 2})), p(2), "b1", MAP, TABLES)
        rows = self.rows(d)
        assert norm(rows[norm(1)]["name"]) == "cdc" and norm(rows[norm(1)]["amt"]) == norm(Decimal("1.50"))
        assert truthy(rows[norm(2)][d.c_deleted]) and d.row_count("orders") == 1

    # ---- XC-07: one DELETE rule (DESIGN.md 5.2)
    def test_delete_unknown_key(self) -> None:
        """A DELETE of a key the destination does not hold writes a tombstone: key, deleted flag, synced column and
        the delivered before-image columns. Delete parity / row counts are then identical on every engine."""
        d, _ = self.fresh()
        d.apply("du", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        gone = {**ROW1, "ID": 5, "NAME": "gone", "AMT": Decimal("7.25")}
        d.apply("du", cs(e(2, "DELETE", before=gone)), p(2), "b2", MAP, TABLES)
        rows = self.rows(d)
        assert norm(5) in rows, "DELETE of an unknown key dropped silently (XC-07)"
        t = rows[norm(5)]
        assert truthy(t[d.c_deleted]) and t.get(d.c_synced) is not None
        assert norm(t["name"]) == "gone" and norm(t["amt"]) == norm(Decimal("7.25"))
        assert d.row_count("orders") == 1 and d.row_count("orders", include_deleted=True) == 2
        assert d.apply("du", cs(e(2, "DELETE", before=gone)), p(2), "b2", MAP, TABLES) == []   # replay: no-op

    def test_delete_key_only_before(self) -> None:
        """A key-only before image (default replica identity) for an unknown key: key-only tombstone, other columns
        NULL; for a known key the stored columns are kept."""
        d, _ = self.fresh()
        d.apply("dk", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d.apply("dk", cs(e(2, "DELETE", before={"ID": 6}), e(3, "DELETE", before={"ID": 1})), p(3), "b2", MAP,
                TABLES)
        rows = self.rows(d)
        assert norm(6) in rows and truthy(rows[norm(6)][d.c_deleted])
        assert rows[norm(6)].get("name") is None and rows[norm(6)].get("amt") is None
        assert truthy(rows[norm(1)][d.c_deleted]) and norm(rows[norm(1)]["name"]) == "a"   # kept
        assert d.row_count("orders") == 0 and d.row_count("orders", include_deleted=True) == 2

    def test_describe_schema(self) -> None:
        d, _ = self.fresh()
        desc = d.describe_schema(["orders"])
        names = {c["name"].lower() for c in desc["orders"]}
        assert {"id", "name", "amt", d.c_synced.lower(), d.c_deleted.lower()} <= names


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)

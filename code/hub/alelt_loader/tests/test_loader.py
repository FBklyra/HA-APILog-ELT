"""alelt_loader: policy, output profile, final-op-per-key, key change, keyless surrogate, replay filter."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from alelt_common.models import CanonicalType, ChangeEvent, ChangeKind, ChangeSet, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.spi import ApplyStats, DestinationConnector
from alelt_loader import Loader, hash_value, load_policy, surrogate_id, system_columns
from alelt_loader.policy import output_profiles

K = TableRef(schema="S", name="KEYED")
L = TableRef(schema="S", name="LOG")


def cols(*spec: tuple[str, CanonicalType, int | None]) -> list[ColumnInfo]:
    return [ColumnInfo(name=n, ordinal=i, engine_type=t.value, canonical_type=t, key_seq=k)
            for i, (n, t, k) in enumerate(spec)]


TABLES = {
    K: TableInfo(table=K, columns=cols(("ID", CanonicalType.INT, 1), ("NAME", CanonicalType.STRING, None),
                                       ("SECRET", CanonicalType.STRING, None), ("DROPME", CanonicalType.STRING, None))),
    L: TableInfo(table=L, columns=cols(("MSG", CanonicalType.STRING, None))),
}


class Recorder(DestinationConnector):
    engine = "rec"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.ensured: list[str] = []

    def bootstrap(self) -> None: ...
    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        self.ensured.append(dest_name)
    def add_column(self, dest_name: str, col: ColumnInfo) -> None: ...
    def read_checkpoint(self, stream: str) -> Position | None: return None
    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Any) -> int: return 0

    def apply(self, stream: str, changesets: list[ChangeSet], to_pos: Position, batch_id: str,
              table_map: dict[TableRef, str], tables: dict[TableRef, TableInfo]) -> list[ApplyStats]:
        self.calls.append({"sets": changesets, "to": to_pos.text, "map": table_map, "tables": tables})
        return []


def cmp(a: Position, b: Position) -> int:
    return (int(a.text) > int(b.text)) - (int(a.text) < int(b.text))


def ev(n: int, kind: str, table: TableRef, before: Any = None, after: Any = None) -> ChangeEvent:
    return ChangeEvent(position=Position(engine="x", text=str(n)), txid="t", kind=ChangeKind(kind), table=table,
                       before=before, after=after)


def cs(n: int, *events: ChangeEvent) -> ChangeSet:
    return ChangeSet(txid=f"t{n}", commit_position=Position(engine="x", text=str(n)), commit_ts=None,
                     events=list(events))


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    p = tmp_path / "tables.yaml"
    p.write_text("""
connection_id: c1
output_profile: alelt
landing_schema: LAND
schemas:
  S:
    enabled: true
    tables:
      KEYED:
        name_in_destination: keyed_t
        columns:
          SECRET: {hashed: true}
          DROPME: {enabled: false}
""", encoding="utf-8")
    return p


def test_profile_defaults_and_unknown() -> None:
    sc = system_columns(None)
    assert (sc.synced, sc.deleted, sc.row_id) == ("_ALELT_SYNCED", "_ALELT_DELETED", "_ALELT_ID")
    with pytest.raises(ValueError):
        system_columns("nope")


def test_extension_profile_file(tmp_path: Path) -> None:
    p = tmp_path / "ext.yaml"
    p.write_text("profiles:\n  legacy:\n    synced_column: _X_SYNCED\n    deleted_column: _X_DELETED\n"
                 "    id_column: _X_ID\n", encoding="utf-8")
    prof = output_profiles(str(p))
    assert prof["legacy"].synced == "_X_SYNCED" and prof["alelt"].synced == "_ALELT_SYNCED"


def test_policy_from_tables_yaml(cfg: Path) -> None:
    pol = load_policy("c1", cfg)
    assert pol.dest_schema == "LAND" and pol.dest_name(K) == "keyed_t"
    assert pol.table(TableRef(schema="OTHER", name="X")).enabled is False   # schema not listed
    assert pol.table(L).enabled is True                                      # listed schema, new table
    proj = pol.project(TABLES[K])
    assert [c.name for c in proj.columns] == ["ID", "NAME", "SECRET"]
    assert proj.columns[2].length == 64
    row = pol.apply_row(K, {"ID": 1, "SECRET": "pw", "DROPME": "x"})
    assert row == {"ID": 1, "SECRET": hash_value("pw")}
    off = load_policy("c1", cfg, overrides={"S.KEYED": {"enabled": False}})
    assert off.table(K).enabled is False


def test_final_op_per_key_and_key_change(cfg: Path) -> None:
    d = Recorder()
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp)
    ld.add([cs(1, ev(1, "INSERT", K, after={"ID": 1, "NAME": "a"}), ev(2, "UPDATE", K, before={"ID": 1},
                                                                      after={"ID": 1, "NAME": "b"}))],
           Position(engine="x", text="2"))
    ld.add([cs(3, ev(3, "UPDATE", K, before={"ID": 1, "NAME": "b"}, after={"ID": 2, "NAME": "b"}),
               ev(4, "INSERT", K, after={"ID": 5, "NAME": "z"}), ev(5, "DELETE", K, before={"ID": 5, "NAME": "z"}))],
           Position(engine="x", text="5"))
    ld.flush(TABLES, "b1")
    call = d.calls[0]
    assert call["to"] == "5" and call["map"] == {K: "keyed_t"}
    out = [(e.kind.value, ((e.before if e.kind is ChangeKind.DELETE else e.after) or {}).get("ID"))
           for e in call["sets"][0].events]
    assert out == [("DELETE", 1), ("UPDATE", 2), ("DELETE", 5)]
    assert not ld.pending()


def test_keyless_surrogate_and_replay_skip(cfg: Path) -> None:
    d = Recorder()
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp, max_latency_s=0)
    ld.add([cs(1, ev(1, "INSERT", L, after={"MSG": "x"})), cs(2, ev(2, "INSERT", L, after={"MSG": "y"}))],
           Position(engine="x", text="2"), checkpoint=Position(engine="x", text="1"))
    ld.flush(TABLES, "b2")
    sets = d.calls[0]["sets"][0].events
    assert len(sets) == 1 and sets[0].after == {"MSG": "y"}             # replayed cs(1) skipped
    assert d.calls[0]["tables"][L].key_columns() == []                  # destination writes the surrogate id
    assert surrogate_id({"MSG": "y"}, ["MSG"]) == hashlib.sha256(b'["y"]').hexdigest()[:32]


def test_empty_flush_opens_no_destination_transaction(cfg: Path) -> None:
    """RC4 / ADR-005 amendment: nothing to apply -> no destination transaction (the hub mirror advances; a
    checkpoint-only apply catches the destination up every ALELT_CHECKPOINT_IDLE_S)."""
    d = Recorder()
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp, max_latency_s=0)
    ld.add([], Position(engine="x", text="9"))
    assert ld.flush(TABLES, "b3") == [] and d.calls == [] and ld.last_flush_empty and not ld.pending()
    ld.apply_checkpoint(Position(engine="x", text="9"), "ckpt")
    assert d.calls[0]["sets"] == [] and d.calls[0]["to"] == "9"


def test_partial_image_merge(cfg: Path) -> None:
    d = Recorder()
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp, max_latency_s=0)
    ld.add([cs(1, ev(1, "UPDATE", K, before={"ID": 1}, after={"ID": 1, "NAME": "lob-ok"})),
            cs(2, ev(2, "UPDATE", K, before={"ID": 1}, after={"ID": 1}))],        # NAME omitted (lob_unavailable)
           Position(engine="x", text="2"))
    ld.flush(TABLES, "b4")
    assert d.calls[0]["sets"][0].events[0].after == {"ID": 1, "NAME": "lob-ok"}


def test_core_profiles_from_config() -> None:
    prof = output_profiles("")
    assert prof["alelt"].synced == "_ALELT_SYNCED"
    assert prof["custom"].row_id == "_ROW_ID"
    gold = Path(__file__).resolve().parents[3] / "config" / "golden" / "columns.csv"
    assert ("golden_spec" in prof) == gold.exists()       # never assumed without a golden spec (R8)


def test_ddl_failure_backs_off_and_names_the_table(cfg: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """DST-loader-01: a failing ensure_table raises TableDDLError (table named, buffer kept, nothing applied) and
    is not retried on every flush; it succeeds once the backoff expired and the DDL works."""
    from alelt_loader import loader as LM

    d = Recorder()
    fail = {"on": True}

    def ensure(table: TableInfo, dest: str, golden: str | None) -> None:
        d.ensured.append(dest)
        if fail["on"]:
            raise RuntimeError("Row size too large")
    d.ensure_table = ensure  # type: ignore[method-assign]
    clock = {"t": 1000.0}
    monkeypatch.setattr(LM.time, "monotonic", lambda: clock["t"])
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp, max_latency_s=0)
    ld.add([cs(1, ev(1, "INSERT", K, after={"ID": 1, "NAME": "a"}))], Position(engine="x", text="1"))
    with pytest.raises(LM.TableDDLError) as ei:
        ld.flush(TABLES, "b1")
    assert ei.value.dest == "keyed_t" and ei.value.attempts == 1 and not d.calls and ld.pending()
    with pytest.raises(LM.TableDDLError):
        ld.flush(TABLES, "b1")                    # inside the backoff: no DDL sent
    assert d.ensured == ["keyed_t"]
    clock["t"] += 6
    fail["on"] = False
    ld.flush(TABLES, "b1")
    assert d.ensured == ["keyed_t", "keyed_t"] and d.calls and not ld.pending()


def test_shape_signature_includes_length_scale_nullable(cfg: Path) -> None:
    """DST-loader-02: a longer VARCHAR / wider DECIMAL re-runs ensure_table (destination widening fires)."""
    d = Recorder()
    ld = Loader(d, load_policy("c1", cfg), "c1", cmp, max_latency_s=0)
    t1 = dict(TABLES)
    ld.add([cs(1, ev(1, "INSERT", K, after={"ID": 1, "NAME": "a"}))], Position(engine="x", text="1"))
    ld.flush(t1, "b1")
    wider = TABLES[K].model_copy(update={"columns": [c.model_copy(update={"length": 200}) if c.name == "NAME" else c
                                                     for c in TABLES[K].columns]})
    ld.add([cs(2, ev(2, "INSERT", K, after={"ID": 2, "NAME": "b"}))], Position(engine="x", text="2"))
    ld.flush({**TABLES, K: wider}, "b2")
    assert d.ensured.count("keyed_t") == 2

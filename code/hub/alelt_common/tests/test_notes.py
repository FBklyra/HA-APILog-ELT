"""XC-36: one note grammar - every engine's forms parse to the same column lists."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from alelt_common import notes as N  # noqa: E402


def test_engine_forms_parse_the_same() -> None:
    pg = "unchanged:a;unchanged:b"                       # Postgres / SQL Server per-column tokens
    my = "unchanged:a,b"                                 # MySQL / Oracle comma lists
    assert N.columns(pg, "unchanged") == N.columns(my, "unchanged") == ["a", "b"]
    db2 = "absent:X;lob_unavailable:L;precision_lost:T12;row_ref:0001000A"
    p = N.parse(db2)
    assert p == {"absent": ["X"], "lob_unavailable": ["L"], "precision_lost": ["T12"], "row_ref": ["0001000A"]}
    assert N.parse("alelt-signal:{\"cls\":\"unsupported\"}") == {}
    assert N.parse("no_before_image;unchanged:a") == {"no_before_image": [], "unchanged": ["a"]}


def test_format_round_trip_is_canonical() -> None:
    p = N.parse("unchanged:a;unchanged:b;precision_lost:t;no_before_image")
    assert N.format_note(p) == "unchanged:a,b;precision_lost:t;no_before_image"
    assert N.parse(N.format_note(p)) == p

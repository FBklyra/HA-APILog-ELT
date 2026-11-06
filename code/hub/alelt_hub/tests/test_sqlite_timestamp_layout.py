"""HC-29: SQLite server-default timestamps use the same text layout (6 fractional digits) as bound values."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, select, text

from alelt_hub.dialect import UTCDateTime, utc_now


def test_server_default_and_bound_timestamps_share_one_text_layout() -> None:
    eng = create_engine("sqlite://")
    md = MetaData()
    t = Table("t", md, Column("id", Integer, primary_key=True),
              Column("ts", UTCDateTime(), server_default=utc_now(), nullable=False))
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(insert(t).values(id=1))
        c.execute(insert(t).values(id=2, ts=datetime(2026, 11, 4, 12, 0, 0, 123456, tzinfo=timezone.utc)))
        raw = [r[0] for r in c.execute(text("SELECT ts FROM t ORDER BY id"))]
        vals = [r[0] for r in c.execute(select(t.c.ts).order_by(t.c.id))]
    assert len(raw[0]) == len(raw[1]) == len("2026-11-04 12:00:00.123456"), raw
    assert all(v.tzinfo is not None for v in vals)

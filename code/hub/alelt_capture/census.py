"""`alelt census` / `python -m alelt_capture.census`: source record census (X-132, DESIGN-REVIEW F-02).

Reads the source log between two positions with the connector's own stream (READ-ONLY: nothing is acknowledged, no
batch is written, the hub is not needed) and reports how every record was classified:
decoded / ignored(reason) / needs_keyed_reread / needs_catalog_refresh / unsupported, plus "unexplained" (a record
kind the connector has no rule for). Exit code 0 = every record explained, 1 = unexplained records, 2 = error.

  alelt census --source mysql --settings '{"host": "...", "port": 3306, "user": "...", "databases": "db"}' \
      --password-env SRC_PW --from '<pos>' [--to '<pos>'] [--idle-s 5] [--max-s 600] [--json out.json]
  alelt census --source postgres --connection <cid> ...        (settings + secret from the hub's pipeline)

PostgreSQL: a logical slot only streams from its confirmed position - use a dedicated slot for a census
(`--settings '{"slot": "census_x", "create_slot": true, ...}'`), created before the changes to classify.
Edge engines (Db2) override `SourceConnector.census_scan(from_pos, to_pos, idle_s, max_s)`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from typing import Any

from alelt_common.completeness import Census, signal_of
from alelt_common.models import ChangeKind, Position
from alelt_common.spi import SourceConnector


def run_census(src: SourceConnector, from_pos: Position | None, to_pos: Position | None, idle_s: float = 5.0,
               max_s: float = 600.0) -> dict[str, Any]:
    """Stream (never ack) until a COMMIT at/after `to_pos`, `idle_s` without events, or `max_s`."""
    scan = getattr(src, "census_scan", None)
    if callable(scan):
        return scan(from_pos, to_pos, idle_s, max_s)  # type: ignore[no-any-return]
    src.census.reset()
    events = Counter()
    sigs: Counter[tuple[str, str, str]] = Counter()
    last: str | None = None
    t0 = time.monotonic()
    idle_since = t0
    reason = "max_s"
    try:
        for ev in src.stream(from_pos):
            now = time.monotonic()
            if ev is None:
                if now - idle_since >= idle_s:
                    reason = "idle"
                    break
                if now - t0 >= max_s:
                    break
                continue
            idle_since = now
            events[ev.kind.value] += 1
            s = signal_of(ev)
            if s is not None:
                sigs[(s.cls.value, s.table.fqn if s.table else "*", s.reason.split(":", 1)[0][:100])] += 1
            if ev.kind in (ChangeKind.COMMIT, ChangeKind.ROLLBACK):
                last = ev.position.text
                if to_pos is not None and src.compare(ev.position, to_pos) >= 0:
                    reason = "to_pos"
                    break
            if now - t0 >= max_s:
                break
    finally:
        try:
            src.close()
        except Exception:  # noqa: BLE001
            pass
    rep = src.census.report()
    rep.update({"from": from_pos.text if from_pos else None, "to": to_pos.text if to_pos else None,
                "last_commit": last, "stopped_by": reason, "events": dict(events),
                "signals": [{"class": c, "table": t, "reason": r, "count": n} for (c, t, r), n in sorted(sigs.items())],
                "seconds": round(time.monotonic() - t0, 1)})
    return rep


def _connector(args: argparse.Namespace) -> SourceConnector:
    from alelt_common.util import load

    if args.connection:
        from alelt_hub.db import get_engine

        from .config import RemoteConnection, connections

        rc = connections(get_engine()).get(args.connection)
        if rc is None:
            raise SystemExit(f"connection {args.connection!r} not found")
        settings = {**rc.settings, **json.loads(args.settings or "{}")}
        return RemoteConnection(rc.connection_id, rc.engine, settings, rc.password_ref).factory()()
    ref = os.environ.get("ALELT_SOURCES")
    refs = json.loads(ref) if ref else {}
    if args.source not in refs:
        from alelt_hub.settings import get_settings

        refs = get_settings().sources
    cls = load(refs[args.source])
    pw = os.environ.get(args.password_env) if args.password_env else None
    return cls(settings=json.loads(args.settings or "{}"), password=pw,  # type: ignore[no-any-return]
               connection_id=args.connection_id)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="alelt census", description="source record census (read-only)")
    ap.add_argument("--source", required=True, help="engine (postgres, mysql, mariadb, oracle, sqlserver, ...)")
    ap.add_argument("--from", dest="from_pos", help="start position (engine text form); default: source default")
    ap.add_argument("--to", dest="to_pos", help="stop at the first commit at/after this position")
    ap.add_argument("--settings", help="connector settings JSON (merged over the connection's settings)")
    ap.add_argument("--password-env", help="environment variable holding the source password")
    ap.add_argument("--connection", help="hub pipeline connection id (settings + secret from the hub)")
    ap.add_argument("--connection-id", default="census")
    ap.add_argument("--idle-s", type=float, default=5.0)
    ap.add_argument("--max-s", type=float, default=600.0)
    ap.add_argument("--json", help="write the report here")
    a = ap.parse_args(argv)
    try:
        src = _connector(a)
        rep = run_census(src, src.parse_position(a.from_pos) if a.from_pos else None,
                         src.parse_position(a.to_pos) if a.to_pos else None, a.idle_s, a.max_s)
    except Exception as e:  # noqa: BLE001
        print(f"census failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    text = json.dumps(rep, indent=1, default=str)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            f.write(text)
    print(text)
    return 1 if rep.get("unexplained") else 0


if __name__ == "__main__":
    raise SystemExit(main())

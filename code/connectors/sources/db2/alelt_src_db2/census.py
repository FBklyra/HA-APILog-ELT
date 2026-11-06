"""Db2 record census: classify every record of a log range (or of stored raw buffers) with the decoder's rules.

Every (record type, component, function, sub-record types) class is counted per RecordClass (decoded / ignored /
needs_keyed_reread / needs_catalog_refresh / unsupported) and reason; `unexplained` = no rule (must be 0).
The report has the shape of alelt_common.completeness.Census.report() plus the range, signals and quarantine reasons,
so the generic `census --source db2` command can print it. Stdlib only (runs on the x64 Python next to the client).

  python -m alelt_src_db2.census --db <DATABASE> --schemas <SCHEMA> --from 31568:0000000000242B17 --out census.json --md census.md
  python -m alelt_src_db2.census --catalog catalog.json case1.bin case2.bin        (offline, stored raw buffers)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from .catalog import specs_from_dict
from .logdecode import RawDecode, TableSpec, decode_buffer

EXPLAINED = ("decoded", "ignored", "needs_keyed_reread", "needs_catalog_refresh")


class CensusRun:
    """Accumulates decode results of consecutive buffers (foreign ids carried like the connector does)."""

    def __init__(self, tables: Mapping[tuple[int, int], TableSpec], catalog_lri: str | None) -> None:
        self.tables, self.catalog_lri = tables, catalog_lri
        self.census: Counter[tuple[str, str, str]] = Counter()
        self.signals: Counter[tuple[str, str]] = Counter()
        self.quarantine: Counter[str] = Counter()
        self.samples: dict[tuple[str, str, str], str] = {}
        self.foreign: set[tuple[int, int]] = set()
        self.buffers = self.bytes = 0
        self.first: str | None = None
        self.last: str | None = None

    def add(self, raw: bytes) -> RawDecode:
        r = decode_buffer(raw, self.tables, catalog_lri=self.catalog_lri, foreign=self.foreign)
        self.foreign |= r.foreign_ids
        self.foreign -= r.created_ids
        self.census.update(r.census)
        for e in r.events:
            if e.signal is not None:
                self.signals[(e.signal.cls, e.signal.reason)] += 1
                self.samples.setdefault((e.signal.record, e.signal.cls, e.signal.reason), e.lri)
            self.first = self.first or e.lri
            self.last = e.lri
        for q in r.quarantined:
            self.quarantine[q.reason.split(":")[0][:100]] += 1
        self.buffers += 1
        self.bytes += len(raw)
        return r

    def report(self, **extra: Any) -> dict[str, Any]:
        by: Counter[str] = Counter()
        for (_, c, _), n in self.census.items():
            by[c] += n
        total = sum(by.values())
        rows = [{"record": f"db2:{k}", "class": c, "reason": why, "count": n,
                 "sample_position": self.samples.get((f"db2:{k}", c, why))}
                for (k, c, why), n in sorted(self.census.items())]
        return {"engine": "db2", **extra, "buffers": self.buffers, "raw_bytes": self.bytes,
                "first_position": self.first, "last_position": self.last,
                "total": total, "by_class": dict(by), "unexplained": by.get("unexplained", 0),
                "classified_pct": round(100.0 * (total - by.get("unexplained", 0)) / total, 4) if total else 100.0,
                "blocking": by.get("unsupported", 0),
                "rows": rows,
                "signals": [{"class": c, "reason": r, "count": n} for (c, r), n in sorted(self.signals.items())],
                "quarantine": dict(self.quarantine)}


def markdown(rep: dict[str, Any]) -> str:
    out = [f"# Db2 record census ({rep.get('database', 'offline')})", "",
           f"Range `{rep.get('from')}` .. `{rep.get('to') or rep.get('last_position')}` · {rep['buffers']} buffers · "
           f"{rep['raw_bytes']:,} bytes · {rep['total']:,} records · classified {rep['classified_pct']} % · "
           f"unexplained {rep['unexplained']} · blocking (unsupported) {rep['blocking']}", "",
           "| Class | Records |", "|---|---:|"]
    out += [f"| {c} | {n:,} |" for c, n in sorted(rep["by_class"].items())]
    out += ["", "| Record (type/component/function/sub-record types) | Class | Reason | Count |", "|---|---|---|---:|"]
    out += [f"| `{r['record']}` | {r['class']} | {r['reason']} | {r['count']:,} |" for r in rep["rows"]]
    if rep["signals"]:
        out += ["", "| Signal | Reason | Count |", "|---|---|---:|"]
        out += [f"| {s['class']} | {s['reason']} | {s['count']:,} |" for s in rep["signals"]]
    if rep["quarantine"]:
        out += ["", "| Quarantine reason | Count |", "|---|---:|"]
        out += [f"| {k} | {n:,} |" for k, n in sorted(rep["quarantine"].items())]
    return "\n".join(out) + "\n"


def iter_log(conn: Any, start: str, end: str | None = None, buf_size: int = 4 * 1024 * 1024,
             max_seconds: float = 3600.0) -> Iterator[tuple[bytes, str]]:
    """Stream db2ReadLog READ (filter ON) buffers from start to end/current: (raw, nextStartLRI)."""
    import ctypes as C

    from . import db2api as A

    def mk(t: str) -> A.LRI:
        p1, p2 = t.split(":")
        return A.LRI(1, int(p1), int(p2, 16))

    s = mk(start)
    e = mk(end) if end else A.LRI(1, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
    info = A.ReadLogInfo()
    buf = C.create_string_buffer(buf_size)
    req = A._ReadLogStruct(iCallerAction=A.RL_READ, piStartLRI=C.pointer(s), piEndLRI=C.pointer(e),
                           poLogBuffer=C.cast(buf, C.c_void_p), iLogBufferSize=buf_size,
                           iFilterOption=A.RL_FILTER_ON, poReadLogInfo=C.pointer(info))
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_seconds:
        code = conn._readlog(req)
        yield buf.raw[:info.logBytesWritten], info.nextStartLRI.text()
        if code == A.RL_READ_TO_CURRENT or info.logRecsWritten == 0:
            return
        C.memmove(C.byref(s), C.byref(info.nextStartLRI), C.sizeof(A.LRI))


def run_files(paths: Iterable[Path], catalog: Mapping[str, Any]) -> dict[str, Any]:
    pos = (catalog.get("position") or {}).get("text")
    cr = CensusRun(specs_from_dict(catalog), pos)
    for p in paths:
        cr.add(Path(p).read_bytes())
    return cr.report(source="files", catalog_position=pos)


def run_db(database: str, schemas: list[str], start: str, end: str | None = None,
           catalog: Mapping[str, Any] | None = None, max_seconds: float = 3600.0) -> dict[str, Any]:
    from .catalog import read_catalog_dict
    from .db2api import Db2Connection

    with Db2Connection(database) as conn:
        cat = catalog or read_catalog_dict(conn, schemas, connection_id="census")
        pos = (cat.get("position") or {}).get("text")
        cr = CensusRun(specs_from_dict(cat), pos)
        nxt = start
        for raw, nxt in iter_log(conn, start, end, max_seconds=max_seconds):
            cr.add(raw)
        return cr.report(source="db2ReadLog", database=database, schemas=schemas, catalog_position=pos,
                         tables=len(cat["tables"]), **{"from": start, "to": end, "next": nxt})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Db2 record census (every log record classified)")
    ap.add_argument("files", nargs="*", type=Path, help="stored raw db2ReadLog buffers (offline mode)")
    ap.add_argument("--db"), ap.add_argument("--schemas", default="")
    ap.add_argument("--from", dest="start"), ap.add_argument("--to", dest="end")
    ap.add_argument("--catalog", type=Path, help="catalog snapshot JSON (default: read now, live mode)")
    ap.add_argument("--max-seconds", type=float, default=3600.0)
    ap.add_argument("--out", type=Path), ap.add_argument("--md", type=Path)
    a = ap.parse_args(argv)
    cat = json.loads(a.catalog.read_text(encoding="utf-8")) if a.catalog else None
    if a.files:
        if cat is None:
            ap.error("offline mode needs --catalog")
        rep = run_files(a.files, cat)
    else:
        if not (a.db and a.start):
            ap.error("live mode needs --db and --from")
        rep = run_db(a.db, [s for s in a.schemas.upper().split(",") if s], a.start, a.end, cat, a.max_seconds)
    if a.out:
        a.out.write_text(json.dumps(rep, indent=1), encoding="utf-8")
    if a.md:
        a.md.write_text(markdown(rep), encoding="utf-8")
    print(json.dumps({k: rep[k] for k in ("total", "by_class", "unexplained", "classified_pct", "blocking",
                                          "quarantine")}, indent=1))
    return 1 if rep["unexplained"] else 0


if __name__ == "__main__":
    sys.exit(main())

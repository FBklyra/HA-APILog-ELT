"""Local soak / volume harness (DESIGN-REVIEW §4.4 #4, F-22 published throughput) - no external account needed.

In-process hub (TestClient) + one worker + a throw-away Postgres landing database. Ships committed transactions of
`--txn-rows` rows for `--seconds`, plus one long transaction that stays open for the whole run (spill path, restart_seq)
and a worker restart every `--restart-every` batches. Measures receive and apply throughput, commit -> apply latency
p95 from the recorded flush evidence (F-19), metadata growth; checks exact parity (count + id range) at the end.

  ALELT_TEST_PG_URL=postgresql://user:pw@localhost:15435 python tests/ladder/L6/soak_local.py --seconds 120

Writes tests/ladder/L6/result.soak-local.json (level L6, part soak-local). This is NOT the L6 lab gate (restored
production-shaped Db2 + chaos): it is the local volume evidence that feeds the published throughput table.
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
for p in ("hub", "connectors/destinations/postgres", "connectors/sources/db2", "tests/ladder"):
    sys.path.insert(0, str(ROOT / p))

from _common.ladder_result import LadderResult  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--txn-rows", type=int, default=200)
    ap.add_argument("--batch-txns", type=int, default=5)
    ap.add_argument("--restart-every", type=int, default=25)
    ap.add_argument("--metadata", choices=["sqlite", "postgres"], default="sqlite")
    args = ap.parse_args()
    res = LadderResult("L6", f"local soak/volume ({args.metadata} metadata, Postgres landing)", environment="dev")
    admin = os.environ.get("ALELT_TEST_PG_URL")
    if not admin:
        res.skip("soak", "ALELT_TEST_PG_URL (admin URL of a throw-away Postgres) not set")
        res.write(HERE / "result.soak-local.json")
        return 0
    import psycopg

    tag = uuid.uuid4().hex[:8]
    land, meta = f"alelt_soak_land_{tag}", f"alelt_soak_meta_{tag}"
    tmp = Path(tempfile.mkdtemp(prefix="alelt-soak-"))

    def admin_sql(sql: str) -> None:
        with psycopg.connect(f"{admin}/postgres", autocommit=True) as c:
            c.execute(sql)  # type: ignore[arg-type]

    admin_sql(f"CREATE DATABASE {land}")
    if args.metadata == "postgres":
        admin_sql(f"CREATE DATABASE {meta}")
        db_url = f"{admin.replace('postgresql://', 'postgresql+psycopg://')}/{meta}"
    else:
        db_url = f"sqlite:///{(tmp / 'meta.db').as_posix()}"
    (tmp / "tables.yaml").write_text("connection_id: fakeconn\noutput_profile: alelt\nlanding_schema: LAND\n"
                                     "schemas:\n  S:\n    enabled: true\n", encoding="utf-8")
    os.environ.update({"ALELT_DB_URL": db_url, "ALELT_DEST_URL": f"{admin}/{land}", "ALELT_SPOOL_DIR": str(tmp / "spool"),
                       "ALELT_TABLES_CONFIG": str(tmp / "tables.yaml"), "ALELT_SOURCES": '{"fake": "alelt_hub.testing:FakeSource"}',
                       "ALELT_AGENT_TOKENS": "soak-token", "ALELT_DEV_AUTH": "1", "ALELT_ENV": "test",
                       "ALELT_LOAD_MAX_LATENCY_S": "0", "ALELT_RATE_LIMIT_PER_MIN": "0", "ALELT_AUTH_FAIL_LIMIT_PER_MIN": "0",
                       "ALELT_MAX_UNLOADED_BATCHES": "100000", "ALELT_WORKER_METRICS_PORT": "0"})
    try:
        from fastapi.testclient import TestClient
        from sqlalchemy import func, select

        from alelt_hub import certification
        from alelt_hub.app import create_app
        from alelt_hub.db import batch, get_engine, job
        from alelt_hub.settings import get_settings
        from alelt_hub.testing import FakeAgent, ev, fake_catalog
        from alelt_hub.worker import Worker

        with TestClient(create_app()) as client:
            a = FakeAgent(client, "soak-token")
            a.register()
            a.catalog(fake_catalog(a.connection_id))
            w = Worker(get_engine(), get_settings(), "soak-0")
            pos, nid, batches, restarts = 1, 1, 0, 0
            long_ids: list[int] = []
            t_recv = t_apply = 0.0
            t0 = time.monotonic()
            while time.monotonic() - t0 < args.seconds:
                evs = []
                for t in range(args.batch_txns):
                    txid = f"T{batches}.{t}"
                    for _ in range(args.txn_rows):
                        evs.append(ev(pos, txid, "INSERT", "S.KEYED", after={"ID": nid, "NAME": f"n{nid}", "AMT": "1.00"}))
                        pos += 1
                        nid += 1
                    evs.append(ev(pos, txid, "COMMIT"))
                    pos += 1
                evs.append(ev(pos, "LONG", "INSERT", "S.KEYED", after={"ID": 10_000_000 + batches, "NAME": "long"}))
                long_ids.append(10_000_000 + batches)
                pos += 1
                s = time.monotonic()
                r = a.send(evs)
                t_recv += time.monotonic() - s
                if r.status_code != 200:
                    res.check("every batch accepted", False, f"batch {batches}: HTTP {r.status_code}")
                    break
                batches += 1
                s = time.monotonic()
                while w.run_once():
                    pass
                t_apply += time.monotonic() - s
                if args.restart_every and batches % args.restart_every == 0:
                    w = Worker(get_engine(), get_settings(), f"soak-{batches}")    # worker lost, LONG still open
                    restarts += 1
            a.send([ev(pos, "LONG", "COMMIT")])
            while w.run_once():
                pass
            wall = time.monotonic() - t0
            rows = nid - 1 + len(long_ids)
            with psycopg.connect(f"{admin}/{land}") as c:
                n, lo, hi = c.execute("SELECT count(*), min(id), max(id) FROM land.keyed WHERE id < 10000000").fetchone()  # type: ignore[misc]
                nl = c.execute("SELECT count(*) FROM land.keyed WHERE id >= 10000000").fetchone()[0]  # type: ignore[index]
                audits = c.execute("SELECT count(*) FROM alelt_control.audit_batch").fetchone()[0]  # type: ignore[index]
            with get_engine().connect() as c:
                p95 = certification.latency_p95(c, a.connection_id, "S.KEYED")
                nb = c.execute(select(func.count()).select_from(batch)).scalar_one()
                nj = c.execute(select(func.count()).select_from(job).where(job.c.status != "DONE")).scalar_one()
            res.check("parity: committed rows landed exactly once", (n, lo, hi) == (nid - 1, 1, nid - 1),
                      f"count={n} min={lo} max={hi} expected={nid - 1}")
            res.check("long transaction open across all batches and restarts landed", nl == len(long_ids),
                      f"{nl}/{len(long_ids)} rows, {restarts} worker restarts")
            res.check("job queue drained", nj == 0, f"not DONE: {nj}")
            res.check("throughput measured", rows > 0,
                      f"rows={rows} batches={batches} wall={wall:.1f}s receive={rows / max(t_recv, 1e-9):.0f} rows/s "
                      f"apply={rows / max(t_apply, 1e-9):.0f} rows/s end-to-end={rows / wall:.0f} rows/s "
                      f"commit->apply p95={p95 if p95 is None else round(p95, 2)}s audit_rows={audits} batch_rows={nb}")
    finally:
        try:
            from alelt_hub.db import get_engine as _ge

            _ge().dispose()
        except Exception:  # noqa: BLE001
            pass
        admin_sql(f"DROP DATABASE IF EXISTS {land} WITH (FORCE)")
        if args.metadata == "postgres":
            admin_sql(f"DROP DATABASE IF EXISTS {meta} WITH (FORCE)")
    out = res.write(HERE / "result.soak-local.json")
    print(out, res.status)
    return 0 if res.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

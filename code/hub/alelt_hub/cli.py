"""`alelt <role>` - single product entrypoint (container and bare-metal).

Roles:
  api       console + agent gateway (uvicorn, ALELT_PORT, default 8080)
  worker    decode -> txn -> load worker
  migrate   wait for the metadata database, create the landing database if missing, upgrade the schema
  all       migrate, then api + worker supervised in one process group (small installs)
  version   print version information
  capture   remote-capture runners (hub-side sources: postgres, ...; alelt_capture, connectors/sources/_spi/REMOTE.md)
  census    --source <engine> --from <pos> --to <pos>: read-only record census of a source (alelt_capture/census.py)
  topology  status | plan | apply | abort | lease-clear   (HA <-> standalone switch, alelt_hub/topology.py)
  migrate-metadata --from URL --to URL                    (SQLite <-> PostgreSQL, verified per table)
  spool-copy --from URL --to URL                          (local <-> S3 hub spool, verified per object)

Secrets: every `ALELT_<NAME>_FILE=/path` is read into `ALELT_<NAME>` before start (Docker / Kubernetes secrets);
nothing is written back to disk or logged (R9).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from typing import Any

ROLES = ("api", "worker", "migrate", "all", "version")


def _set_in_process(env: Any, key: str, value: str) -> None:
    """DEP-12: make a secret visible to this process's `os.environ` lookups WITHOUT exporting it: the OS-level
    environment (putenv) is not touched, so child processes, `/proc/<pid>/environ` and crash dumps of the
    environment block never contain it. Other mappings (tests) are written normally."""
    if env is os.environ:
        data = getattr(env, "_data", None)
        if isinstance(data, dict):
            data[env.encodekey(key)] = env.encodevalue(value)   # type: ignore[attr-defined]
            return
    env[key] = value


def load_secret_files(env: dict[str, str] | None = None) -> list[str]:
    """ALELT_X_FILE -> ALELT_X (only if ALELT_X is not already set), in-process only (DEP-12). Returns the names loaded
    (never values)."""
    env = os.environ if env is None else env
    loaded = []
    for k in [k for k in env if k.startswith("ALELT_") and k.endswith("_FILE")]:
        target = k[: -len("_FILE")]
        if env.get(target):
            continue
        path = env[k]
        if path and os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                _set_in_process(env, target, f.read().strip())
            loaded.append(target)
    return loaded


def _wait_db(timeout_s: float) -> None:
    from sqlalchemy import text

    from .db import get_engine

    end = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < end:
        try:
            with get_engine().connect() as c:
                c.execute(text("SELECT 1"))
            return
        except Exception as e:  # noqa: BLE001 - retried until timeout
            last = e
            time.sleep(2)
    raise SystemExit(f"metadata database not reachable after {timeout_s:.0f}s: {type(last).__name__}")


def _ensure_landing_db() -> None:
    """Create the Postgres landing database named in ALELT_DEST_URL when it does not exist (needs CREATEDB)."""
    url = os.environ.get("ALELT_DEST_URL", "")
    if not url.startswith(("postgresql://", "postgres://")) or os.environ.get("ALELT_CREATE_LANDING_DB", "1") != "1":
        return
    import psycopg
    from psycopg import sql

    base, _, name = url.rpartition("/")
    name = name.split("?", 1)[0]
    if not name:
        return
    try:
        with psycopg.connect(f"{base}/postgres", autocommit=True, connect_timeout=10) as c:
            if c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone() is None:
                c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                print(f"landing database {name!r} created")
    except psycopg.Error as e:
        print(f"landing database check skipped ({type(e).__name__}); create {name!r} manually if needed")


def migrate() -> int:
    from . import migrate as m

    _wait_db(float(os.environ.get("ALELT_DB_WAIT_S", "120")))
    _ensure_landing_db()
    sys.argv = [sys.argv[0]]
    return m.main()


def api() -> None:
    import uvicorn

    uvicorn.run("alelt_hub.app:app", host=os.environ.get("ALELT_HOST", "0.0.0.0"), port=int(os.environ.get("ALELT_PORT", "8080")),
                # X-Forwarded-For is applied in-app after the real peer was captured (security.PeerCaptureMiddleware)
                proxy_headers=False,
                server_header=False, access_log=os.environ.get("ALELT_ACCESS_LOG", "0") == "1",
                log_level=os.environ.get("ALELT_LOG_LEVEL", "info").lower())


def worker() -> None:
    from . import worker as w

    _wait_db(float(os.environ.get("ALELT_DB_WAIT_S", "120")))
    sys.argv = [sys.argv[0]]
    w.main()


def run_all() -> int:
    """migrate, then api + worker as child processes; stop both when either exits or on SIGTERM."""
    rc = migrate()
    if rc:
        return rc
    me = [sys.executable, "-m", "alelt_hub.cli"]
    kids = [subprocess.Popen([*me, "api"]), subprocess.Popen([*me, "worker"])]
    stopping = False

    def stop(*_: Any) -> None:
        nonlocal stopping
        stopping = True
        for k in kids:
            if k.poll() is None:
                k.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while all(k.poll() is None for k in kids):
        time.sleep(1)
    stop()
    for k in kids:
        try:
            k.wait(timeout=30)
        except subprocess.TimeoutExpired:
            k.kill()
    return 0 if stopping else 1


def version_info() -> dict[str, Any]:
    from .product import version_info as vi

    return vi()


def completion_cli(argv: list[str]) -> int:
    """`completion rebuild <cid>`: recreate the out-of-row completion entries of a connection from the spool after a
    metadata loss (DR, ADR-005 amendment); `completion status [<cid>]`: pending entries per table."""
    ap = argparse.ArgumentParser(prog="alelt completion")
    ap.add_argument("action", choices=("rebuild", "status"))
    ap.add_argument("connection_id", nargs="?")
    a = ap.parse_args(argv)
    from .db import get_engine, init_db

    eng = get_engine()
    init_db(eng)
    from . import completion

    if a.action == "status":
        print(json.dumps({f"{c}:{t}": n for (c, t), n in completion.pending_counts(eng, a.connection_id).items()},
                         indent=2))
        return 0
    if not a.connection_id:
        ap.error("rebuild needs a connection id")
    from .settings import get_settings
    from .worker import Worker

    n = completion.rebuild(Worker(eng, get_settings(), "cli:completion").pipeline(a.connection_id))
    print(json.dumps({"connection_id": a.connection_id, "entries": n}))
    return 0


def main(argv: list[str] | None = None) -> int:
    rest = list(sys.argv[1:] if argv is None else argv)
    if rest and rest[0] in ("topology", "migrate-metadata", "spool-copy"):
        load_secret_files()
        from .topology import main as topology_main

        return topology_main(rest)
    if rest and rest[0] == "capture":                   # alelt capture [--connection ID] [--once]
        load_secret_files()
        _wait_db(float(os.environ.get("ALELT_DB_WAIT_S", "120")))
        from alelt_capture.__main__ import main as capture_main

        return capture_main(rest[1:])
    if rest and rest[0] == "config":                    # alelt config export|import|diff|status (F-18)
        load_secret_files()
        from .config_api import main as config_main

        return config_main(rest[1:])
    if rest and rest[0] == "census":                    # alelt census --source <engine> --from <pos> --to <pos>
        load_secret_files()
        from alelt_capture.census import main as census_main

        return census_main(rest[1:])
    if rest and rest[0] == "completion":                # alelt completion rebuild <cid> | status [<cid>]
        load_secret_files()
        return completion_cli(rest[1:])
    if rest and rest[0] == "diagnose":                  # alelt diagnose export-patch <incident> ...
        load_secret_files()
        from alelt_diagnose.cli import main as diagnose_main

        return diagnose_main(rest[1:])
    ap = argparse.ArgumentParser(prog="alelt", description="ALELT CDC replication platform")
    ap.add_argument("role", choices=ROLES, nargs="?", default=os.environ.get("ALELT_ROLE", "all"))
    args = ap.parse_args(argv)
    load_secret_files()
    if args.role == "version":
        print(json.dumps(version_info(), indent=2))
        return 0
    if args.role == "migrate":
        return migrate()
    if args.role == "api":
        api()
        return 0
    if args.role == "worker":
        worker()
        return 0
    return run_all()


if __name__ == "__main__":
    sys.exit(main())

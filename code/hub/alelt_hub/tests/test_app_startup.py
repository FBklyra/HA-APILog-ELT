"""The API process starts on an EMPTY metadata database within a bounded time and never blocks silently: libpq
connects are bounded (`ALELT_DB_CONNECT_TIMEOUT_S`), the migration lock wait is bounded (`ALELT_MIGRATE_LOCK_WAIT_S`)."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

HUB = Path(__file__).resolve().parents[2]
START_S = 45


def _port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _start(db_url: str, tmp: Path) -> float:
    port = _port()
    env = {**os.environ, "ALELT_DB_URL": db_url, "ALELT_SPOOL_DIR": str(tmp / "spool"), "ALELT_AGENT_TOKENS": "t",
           "ALELT_DEV_AUTH": "1", "ALELT_ENV": "test", "ALELT_AUTO_MIGRATE": "1", "PYTHONPATH": str(HUB)}
    env.pop("ALELT_TABLES_CONFIG", None)
    log = open(tmp / "hub.log", "wb")
    p = subprocess.Popen([sys.executable, "-m", "uvicorn", "alelt_hub.app:app", "--host", "127.0.0.1", "--port",
                          str(port), "--no-access-log"], env=env, stdout=log, stderr=subprocess.STDOUT, cwd=str(HUB))
    t0 = time.monotonic()
    try:
        while time.monotonic() - t0 < START_S:
            if p.poll() is not None:
                raise AssertionError((tmp / "hub.log").read_text(errors="replace")[-2000:])
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2):
                    return time.monotonic() - t0
            except OSError:
                time.sleep(0.3)
        raise AssertionError(f"not ready after {START_S}s:\n" + (tmp / "hub.log").read_text(errors="replace")[-2000:])
    finally:
        p.terminate()
        try:
            p.wait(10)
        except subprocess.TimeoutExpired:
            p.kill()
        log.close()


def test_api_starts_on_an_empty_sqlite_database() -> None:
    with tempfile.TemporaryDirectory() as d:
        assert _start(f"sqlite:///{(Path(d) / 'meta.db').as_posix()}", Path(d)) < START_S


@pytest.mark.skipif(not os.environ.get("ALELT_TEST_PG_URL"), reason="needs ALELT_TEST_PG_URL")
def test_api_starts_on_an_empty_postgres_database() -> None:
    import psycopg

    admin = os.environ["ALELT_TEST_PG_URL"].rstrip("/")
    db = f"alelt_boot_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{admin}/postgres", autocommit=True) as c:
        c.execute(f"CREATE DATABASE {db}")  # type: ignore[arg-type]
    try:
        with tempfile.TemporaryDirectory() as d:
            assert _start(f"{admin.replace('postgresql://', 'postgresql+psycopg://')}/{db}", Path(d)) < START_S
    finally:
        with psycopg.connect(f"{admin}/postgres", autocommit=True) as c:
            c.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")  # type: ignore[arg-type]


def test_unreachable_metadata_server_fails_fast_instead_of_hanging(monkeypatch: pytest.MonkeyPatch) -> None:
    from sqlalchemy import text

    from alelt_hub.dialect import make_engine

    monkeypatch.setenv("ALELT_DB_CONNECT_TIMEOUT_S", "2")
    eng = make_engine("postgresql+psycopg://u:p@10.255.255.1:5432/x")    # non-routable: a black hole
    t0 = time.monotonic()
    with pytest.raises(Exception):
        with eng.connect() as c:
            c.execute(text("SELECT 1"))
    assert time.monotonic() - t0 < 20

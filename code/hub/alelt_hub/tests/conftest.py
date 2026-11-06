"""Hub integration fixtures, run once per metadata backend: SQLite always, PostgreSQL when ALELT_TEST_PG_URL (admin URL,
no db) is set. The landing destination is always a fresh Postgres database (tests that load data skip without it)."""
from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[3]
for p in ("hub", "connectors/destinations/postgres", "connectors/sources/db2"):
    if str(_ROOT / p) not in sys.path:
        sys.path.insert(0, str(_ROOT / p))

ADMIN = os.environ.get("ALELT_TEST_PG_URL")
BACKENDS = ["sqlite"] + (["postgres"] if ADMIN else [])
TOKEN = "test-token"
TABLES_YAML = """
connection_id: fakeconn
output_profile: alelt
landing_schema: LAND
schemas:
  S:
    enabled: true
    tables:
      KEYED:
        name_in_destination: KEYED
        columns:
          SECRET: {hashed: true}
"""


def _admin(sql: str) -> None:
    import psycopg

    with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as c:
        c.execute(sql)  # type: ignore[arg-type]


def _reset_caches() -> None:
    from alelt_hub import app, console, store
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings

    get_engine().dispose()
    get_engine.cache_clear()
    get_settings.cache_clear()
    store.source_connector.cache_clear()
    app._policies.clear()
    console._pol_cache.clear()


@pytest.fixture(scope="session", params=BACKENDS)
def hub_env(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, str]]:
    backend: str = request.param
    tag = uuid.uuid4().hex[:8]
    meta, land = f"alelt_test_meta_{tag}", f"alelt_test_land_{tag}"
    tmp = tmp_path_factory.mktemp(f"hub-{backend}")
    if ADMIN:
        _admin(f"CREATE DATABASE {land}")
    if backend == "postgres":
        _admin(f"CREATE DATABASE {meta}")
        db_url = f"{ADMIN.replace('postgresql://', 'postgresql+psycopg://')}/{meta}"   # type: ignore[union-attr]
    else:
        db_url = f"sqlite:///{(tmp / 'meta' / 'alelt.db').as_posix()}"
    (tmp / "tables.yaml").write_text(TABLES_YAML, encoding="utf-8")
    env = {
        "ALELT_DB_URL": db_url,
        "ALELT_DEST_URL": f"{ADMIN}/{land}" if ADMIN else "",
        "ALELT_SPOOL_DIR": str(tmp / "spool"),
        "ALELT_TABLES_CONFIG": str(tmp / "tables.yaml"),
        "ALELT_SOURCES": '{"fake": "alelt_hub.testing:FakeSource"}',
        "ALELT_AGENT_TOKENS": TOKEN,
        "ALELT_DEV_AUTH": "1",
        "ALELT_ENV": "test",
        "ALELT_LOAD_MAX_LATENCY_S": "0",
        "ALELT_WORKER_METRICS_PORT": "0",
        "ALELT_RATE_LIMIT_PER_MIN": "0",              # suites send thousands of requests from one client
        "ALELT_AUTH_FAIL_LIMIT_PER_MIN": "0",         # (limits are tested in test_security.py)
        "ALELT_VERIFY_EDGE_TIMEOUT_S": "3",           # edge VERIFY_TABLE: tests answer fast or not at all
        "ALELT_INITIAL_LOAD": "none",                 # CDC suites start from empty tables; test_initial_load.py sets auto
    }
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    _reset_caches()
    yield env | {"backend": backend}
    _reset_caches()
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    if backend == "postgres":
        _admin(f"DROP DATABASE {meta} WITH (FORCE)")
    if ADMIN:
        _admin(f"DROP DATABASE {land} WITH (FORCE)")


def need_landing() -> None:
    if not ADMIN:
        pytest.skip("landing destination needs ALELT_TEST_PG_URL")


@pytest.fixture(scope="session")
def client(hub_env: dict[str, str]) -> Iterator[Any]:
    from fastapi.testclient import TestClient

    from alelt_hub.app import create_app

    with TestClient(create_app()) as c:
        yield c


@pytest.fixture(scope="session")
def worker(client: Any) -> Any:
    need_landing()
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    return Worker(get_engine(), get_settings(), "test-worker")


def drain(w: Any) -> int:
    n = 0
    while w.run_once():
        n += 1
    return n


def role(r: str, user: str = "alice") -> dict[str, str]:
    return {"X-ALELT-Dev-Role": r, "X-ALELT-Dev-User": user}

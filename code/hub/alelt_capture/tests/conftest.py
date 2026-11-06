"""alelt_capture tests: throw-away SQLite hub metadata + local blob store per test."""
from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT / "hub") not in sys.path:
    sys.path.insert(0, str(_ROOT / "hub"))


def _reset() -> None:
    from alelt_hub import blobstore, store
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings

    get_engine().dispose()
    get_engine.cache_clear()
    get_settings.cache_clear()
    store.source_connector.cache_clear()
    for name in ("get_store", "_store"):
        fn = getattr(blobstore, name, None)
        if fn is not None and hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture()
def hub(tmp_path: Path) -> Iterator[Any]:
    env = {"ALELT_DB_URL": f"sqlite:///{(tmp_path / 'meta' / 'alelt.db').as_posix()}",
           "ALELT_SPOOL_DIR": str(tmp_path / "spool"), "ALELT_ENV": "test",
           "ALELT_SOURCES": '{"fakeremote": "alelt_capture.tests.fakes:FakeRemote"}'}
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    (tmp_path / "meta").mkdir()
    _reset()
    from alelt_capture.tests.fakes import FakeRemote
    from alelt_hub.db import get_engine, init_db

    FakeRemote.reset()
    init_db()
    yield get_engine()
    _reset()
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

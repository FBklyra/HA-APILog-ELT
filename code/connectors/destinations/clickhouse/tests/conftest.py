"""Put hub/, the destination kit and this plugin on sys.path."""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
_HERE = Path(__file__).resolve().parents[1]
for p in (_ROOT / "hub", _ROOT / "connectors/destinations/_spi", _HERE):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _exclusive_writer():  # type: ignore[no-untyped-def]
    """The conformance suite owns its throw-away databases: one fence for every stream (the hub registers its
    pipeline/replay locks instead, DST-clickhouse-01)."""
    from alelt_common.spi import hold_writer, release_writer

    hold_writer("stream:*", lambda: True)
    yield
    release_writer("stream:*")

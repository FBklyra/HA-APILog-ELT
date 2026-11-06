"""Put hub/, the destination kit and this plugin on sys.path."""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
_HERE = Path(__file__).resolve().parents[1]
for p in (_ROOT / "hub", _ROOT / "connectors/destinations/_spi", _HERE):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# ---------------------------------------------------------------- bigquery-emulator guard (local tests only)
# The community emulator (lab profile `bigquery`) panics or hangs after a few hundred jobs (googlesqlite). It is
# restarted before every group of EMU_GROUP tests and after a test during which it died; a failure while the
# emulator is dead is reported as xfail "emulator limit" (never a destination defect), then the emulator restarts.
import os  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
import urllib.request  # noqa: E402
from urllib.parse import parse_qs, urlsplit  # noqa: E402

import pytest  # noqa: E402

EMU_URL = os.environ.get("ALELT_TEST_BIGQUERY_URL") or ""
EMU = "emulator=1" in EMU_URL
EMU_CONTAINER = os.environ.get("ALELT_TEST_BIGQUERY_CONTAINER", "alelt-lab-bigquery")
EMU_GROUP = int(os.environ.get("ALELT_TEST_BIGQUERY_GROUP", "4"))
_count = {"n": 0}


def _endpoint() -> str:
    return (parse_qs(urlsplit(EMU_URL).query).get("endpoint") or ["http://127.0.0.1:47050"])[0]


def emulator_alive(timeout: float = 10.0, tries: int = 3) -> bool:
    project = urlsplit(EMU_URL).netloc or "lab-project"
    for _ in range(tries):
        try:
            with urllib.request.urlopen(f"{_endpoint()}/bigquery/v2/projects/{project}/datasets", timeout=timeout):
                return True
        except Exception:  # noqa: BLE001
            time.sleep(1)
    return False


def restart_emulator() -> bool:
    try:
        subprocess.run(["docker", "restart", "-t", "2", EMU_CONTAINER], check=True, capture_output=True, timeout=120)
    except Exception:  # noqa: BLE001 - not a lab container we can restart
        return False
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if emulator_alive(2.0, 1):
            return True
        time.sleep(1)
    return False


@pytest.fixture(autouse=True)
def _emulator_guard(request):  # type: ignore[no-untyped-def]
    if not EMU or "Emulator" not in (request.cls.__name__ if request.cls else ""):
        yield
        return
    if _count["n"] % EMU_GROUP == 0 or not emulator_alive():
        if not restart_emulator() and not emulator_alive():
            pytest.skip("bigquery emulator not reachable (emulator limit)")
    _count["n"] += 1
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # type: ignore[no-untyped-def]
    out = yield
    rep = out.get_result()
    if EMU and rep.when == "call" and rep.failed and item.cls is not None and "Emulator" in item.cls.__name__:
        if not emulator_alive():
            rep.outcome = "skipped"
            rep.wasxfail = "emulator limit: bigquery-emulator crashed/hung during the test (restarted)"
            restart_emulator()
            _count["n"] = 1

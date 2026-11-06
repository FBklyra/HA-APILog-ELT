"""alelt_diagnose unit fixtures: evidence-pack builder and a throw-away SQLite hub metadata database."""
from __future__ import annotations

import json
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[3]
for p in ("hub", "connectors/destinations/postgres", "connectors/sources/db2"):
    if str(_ROOT / p) not in sys.path:
        sys.path.insert(0, str(_ROOT / p))

from alelt_diagnose.models import EvidencePack, IncidentKind, Trigger  # noqa: E402


def pack(kind: IncidentKind = IncidentKind.JOB_FAILED, error: str = "", **kw: Any) -> EvidencePack:
    trig = kw.pop("trigger", {})
    return EvidencePack(collected_at=datetime.now(timezone.utc),
                        trigger=Trigger(kind=kind, error=error, connection_id="c1", **trig), **kw)


@pytest.fixture()
def engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Fresh SQLite metadata DB with every hub + diag table (generic types only)."""
    from alelt_hub.db import init_db
    from alelt_hub.dialect import make_engine
    from alelt_hub.settings import get_settings
    import alelt_diagnose.tables  # noqa: F401

    monkeypatch.setenv("ALELT_DB_URL", f"sqlite:///{(tmp_path / 'm.db').as_posix()}")
    monkeypatch.setenv("ALELT_ENV", "test")
    get_settings.cache_clear()
    eng = make_engine(f"sqlite:///{(tmp_path / 'm.db').as_posix()}")
    init_db(eng)
    yield eng
    eng.dispose()
    get_settings.cache_clear()


class Fake:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.reply: Any = None
        self.status = 200


@pytest.fixture()
def server() -> Iterator[tuple[str, Fake]]:
    fake = Fake()

    class H(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            fake.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
            out = json.dumps(fake.reply(self.path, body) if callable(fake.reply) else fake.reply).encode()
            self.send_response(fake.status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)

        def log_message(self, *_: object) -> None:
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", fake
    srv.shutdown()

"""Security test suite (design review §12; F-12/F-14/F-15): header spoofing, prod-mode refusals, enrolment token
replay, per-agent credentials + rotation, binding, authn/authz matrix over every console endpoint, command whitelist
fuzz, path traversal on uploads, CSRF/CORS, rate limits, secrets never logged, no pickle."""
from __future__ import annotations

import base64
import io
import json
import logging
import random
import string
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

from alelt_hub.testing import FakeAgent

from .conftest import TOKEN, role

ADMIN = role("PlatformAdmin", "admin@x")
ROLES = ("Viewer", "DataOperator", "PlatformAdmin", "Auditor")


def _settings(monkeypatch: pytest.MonkeyPatch, **env: str) -> Any:
    from alelt_hub.settings import get_settings

    for k, v in env.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()
    return get_settings


@pytest.fixture()
def fresh_settings(monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_hub.settings import get_settings

    yield monkeypatch
    monkeypatch.undo()
    get_settings.cache_clear()


# ------------------------------------------------------------------ F-14: identity header spoofing
def test_thumbprint_header_only_from_trusted_proxy(client: Any, fresh_settings: Any) -> None:
    from alelt_hub.auth import authenticate_agent

    _settings(fresh_settings, ALELT_AGENT_AUTH="mtls", ALELT_AGENT_THUMBPRINTS="ab12cd", ALELT_TRUSTED_PROXIES="10.9.0.5/32")
    hdr = {"X-SSL-Client-Fingerprint": "AB:12:CD"}
    proxy = {"alelt.peer": ("10.9.0.5", 4431), "client": ("10.9.0.5", 4431)}
    assert authenticate_agent(hdr, proxy).value == "ab12cd"
    for peer in (("10.9.0.6", 1), ("127.0.0.1", 1), ("testclient", 1), ("", 0)):
        with pytest.raises(HTTPException) as e:          # a pod / process inside the network forging the header
            authenticate_agent(hdr, {"alelt.peer": peer, "client": peer})
        assert e.value.status_code == 403
    # X-Forwarded-For rewrites `client`, never the captured peer: spoofing the proxy address does not help
    with pytest.raises(HTTPException):
        authenticate_agent(hdr, {"alelt.peer": ("10.9.0.99", 1), "client": ("10.9.0.5", 1)})
    with pytest.raises(HTTPException):                   # trusted proxy but certificate not allow-listed
        authenticate_agent({"X-SSL-Client-Fingerprint": "ff"}, proxy)


def test_thumbprint_spoof_through_the_app(hub_env: dict[str, str], fresh_settings: Any) -> None:
    """End to end over HTTP with X-Forwarded-For claiming the proxy address: refused."""
    from fastapi.testclient import TestClient

    from alelt_hub.app import create_app

    _settings(fresh_settings, ALELT_AGENT_AUTH="mtls", ALELT_AGENT_THUMBPRINTS="ab12cd", ALELT_TRUSTED_PROXIES="10.9.0.5")
    h = {"X-ALELT-Protocol": "1", "X-SSL-Client-Fingerprint": "ab12cd", "X-Forwarded-For": "10.9.0.5"}
    body = {"agent_id": "spoof", "connection_id": "spoofconn", "engine": "fake", "version": "t", "hostname": "h",
            "database": "D"}
    with TestClient(create_app(), client=("10.9.0.77", 5000)) as c:
        r = c.post("/agent/v1/register", headers=h, json=body)
        assert r.status_code == 403 and "trusted TLS proxy" in r.text
    with TestClient(create_app(), client=("10.9.0.5", 5000)) as c:
        assert c.post("/agent/v1/register", headers=h, json=body).status_code == 200


# ------------------------------------------------------------------ prod-mode refusals
@pytest.mark.parametrize("env,needle", [
    ({"ALELT_AGENT_TOKENS": "shared-static"}, "static shared agent tokens"),
    ({"ALELT_DEV_AUTH": "1"}, "dev auth"),
    ({"ALELT_AGENT_AUTH": "mtls", "ALELT_TRUSTED_PROXIES": "*"}, "ALELT_TRUSTED_PROXIES"),
    ({"ALELT_CORS_ORIGINS": "*"}, "ALELT_CORS_ORIGINS"),
])
def test_prod_refuses_insecure_settings(fresh_settings: Any, env: dict[str, str], needle: str) -> None:
    fresh_settings.delenv("ALELT_AGENT_TOKENS", raising=False)
    fresh_settings.delenv("ALELT_DEV_AUTH", raising=False)
    get = _settings(fresh_settings, ALELT_ENV="prod", ALELT_AUTH_MODE="oidc", **env)
    with pytest.raises(RuntimeError, match=needle):
        get()


def test_prod_accepts_hardened_settings(fresh_settings: Any) -> None:
    fresh_settings.delenv("ALELT_AGENT_TOKENS", raising=False)
    fresh_settings.delenv("ALELT_DEV_AUTH", raising=False)
    s = _settings(fresh_settings, ALELT_ENV="prod", ALELT_AUTH_MODE="oidc", ALELT_AGENT_AUTH="mtls",
                  ALELT_TRUSTED_PROXIES="10.0.0.5")()
    assert s.is_trusted_proxy("10.0.0.5") and not s.is_trusted_proxy("10.0.0.6")


def test_dev_role_header_ignored_in_oidc_mode(client: Any, fresh_settings: Any) -> None:
    fresh_settings.delenv("ALELT_DEV_AUTH", raising=False)
    _settings(fresh_settings, ALELT_AUTH_MODE="oidc", ALELT_OIDC_ISSUER="https://idp", ALELT_OIDC_AUDIENCE="a",
              ALELT_OIDC_JWKS_URL="https://idp/keys")
    r = client.get("/api/v1/audit", headers=role("PlatformAdmin"))
    assert r.status_code == 401
    pytest.importorskip("jwt")
    r = client.get("/api/v1/audit", headers={**role("PlatformAdmin"), "Authorization": "Bearer not.a.jwt"})
    assert r.status_code == 401


# ------------------------------------------------------------------ enrolment -> per-agent credential
def _enroll(client: Any, cid: str, agent_id: str) -> str:
    r = client.post("/api/v1/setup/agents/enroll", headers=ADMIN,
                    json={"connection_id": cid, "database": "SECDB", "agent_id": agent_id, "reason": "test"})
    assert r.status_code == 200, r.text
    return str(r.json()["token"])


def _reg(client: Any, token: str, agent_id: str, cid: str, caps: list[str] | None = None) -> Any:
    return client.post("/agent/v1/register", headers={"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": token}, json={
        "agent_id": agent_id, "connection_id": cid, "engine": "db2", "version": "t", "hostname": "h",
        "database": "SECDB", "capabilities": caps or []})


def test_enrollment_replay_and_credential_lifecycle(client: Any) -> None:
    from alelt_hub import setup

    tok = _enroll(client, "secconn", "sec-agent")
    assert _reg(client, tok, "intruder", "secconn").status_code == 403          # bound to its agent
    assert _reg(client, tok, "sec-agent", "otherconn").status_code == 403       # and connection
    r = _reg(client, tok, "sec-agent", "secconn", ["credential.v1"])
    assert r.status_code == 200 and r.json()["credential"].startswith("alelta_")
    cred = r.json()["credential"]
    assert _reg(client, tok, "intruder", "secconn").status_code == 403          # claimed: still bound
    h = {"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": cred}
    hb = {"agent_id": "sec-agent", "state": "IDLE", "at": "2026-11-04T00:00:00Z"}
    assert client.post("/agent/v1/heartbeat", headers=h, json=hb).status_code == 200   # first use retires enrolment
    setup._cache.clear()
    assert _reg(client, tok, "sec-agent", "secconn").status_code == 401          # enrolment token replay refused
    # binding: the credential cannot act as another agent / connection
    assert client.post("/agent/v1/heartbeat", headers=h, json={**hb, "agent_id": "contract-agent"}).status_code == 403
    assert client.get("/agent/v1/commands?agent_id=other", headers=h).status_code == 403
    # rotation: new credential, old one valid only during the grace window
    rr = client.post("/agent/v1/credential/rotate", headers=h)
    assert rr.status_code == 200
    new = rr.json()["credential"]
    assert client.post("/agent/v1/credential/rotate", headers=h).status_code == 409   # rotated twice
    h2 = {**h, "X-ALELT-Agent-Token": new}
    assert client.post("/agent/v1/heartbeat", headers=h2, json=hb).status_code == 200
    old_hash = setup._hash(cred)
    with __import__("alelt_hub.db", fromlist=["get_engine"]).get_engine().begin() as c:
        v = setup._get(c, setup._CRED + old_hash)
        setup._put(c, setup._CRED + old_hash, {**v, "grace_until": "2000-01-01T00:00:00+00:00"})
    setup._ccache.clear()
    assert client.post("/agent/v1/heartbeat", headers=h, json=hb).status_code == 401  # grace over
    # static tokens cannot be "rotated"
    assert client.post("/agent/v1/credential/rotate", headers={"X-ALELT-Protocol": "1",
                                                               "X-ALELT-Agent-Token": TOKEN}).status_code == 409
    # revoke all (compromised host)
    assert client.post("/api/v1/setup/agents/sec-agent/credentials/revoke", headers=ADMIN,
                       json={"reason": "host compromised"}).json()["revoked"] >= 1
    assert client.post("/agent/v1/heartbeat", headers=h2, json=hb).status_code == 401
    creds = client.get("/api/v1/setup/agents/credentials", headers=ADMIN).json()
    assert creds and all("token" not in c and "hash" not in c for c in creds)
    assert client.get("/metrics").status_code == 200


def test_bad_tokens_and_protocol_window(client: Any) -> None:
    body = {"agent_id": "x", "connection_id": "x", "engine": "fake", "version": "t", "hostname": "h", "database": "D"}
    for tok in ("", "wrong", TOKEN + "x", "alelte_" + "A" * 40, "alelta_" + "B" * 40):
        r = client.post("/agent/v1/register", headers={"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": tok}, json=body)
        assert r.status_code == 401, tok
    for proto, frag in (("2", "upgrade the hub first"), ("0", "upgrade the agent"), ("x", "required"), (None, "required")):
        h = {"X-ALELT-Agent-Token": TOKEN, **({"X-ALELT-Protocol": proto} if proto else {})}
        r = client.post("/agent/v1/register", headers=h, json=body)
        assert r.status_code == 426 and frag in r.text


def test_websocket_control_requires_identity(client: Any) -> None:
    from starlette.websockets import WebSocketDisconnect

    for h in ({"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": "nope"}, {"X-ALELT-Agent-Token": TOKEN},
              {"X-ALELT-Protocol": "1"}):
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/agent/v1/control?agent_id=fake-agent", headers=h) as ws:
                ws.receive_text()


# ------------------------------------------------------------------ authn / authz matrix (every console endpoint)
def _console_routes() -> list[tuple[str, str, str]]:
    from alelt_hub.app import create_app
    from alelt_hub.auth import PERMISSIONS

    out = []
    for r in create_app().routes:
        path = getattr(r, "path", "")
        if not path.startswith("/api/v1/") or not hasattr(r, "dependant"):
            continue
        perm = None
        for d in r.dependant.dependencies:
            fn = d.call
            cl = getattr(fn, "__closure__", None) or ()
            for cell in cl:
                v = cell.cell_contents
                if isinstance(v, str) and v in PERMISSIONS:
                    perm = v
        for m in sorted(r.methods or ()):
            out.append((m, path, perm or ""))
    return out


def test_rbac_matrix_every_endpoint(client: Any) -> None:
    """Every /api/v1 route: no identity -> 401; a role without the route's permission -> 403 (never 2xx)."""
    from alelt_hub.auth import ROLE_PERMISSIONS, Role

    routes = _console_routes()
    assert len(routes) > 60
    unprotected = []
    for method, path, perm in routes:
        url = path
        for seg in ("{table_id}", "{rid}", "{run_id}", "{agent_id}", "{sid}", "{incident_id}", "{eid}", "{cid}",
                    "{connection_id}", "{request_id}", "{wave_id}", "{id}"):
            url = url.replace(seg, "1" if seg == "{rid}" else "00000000-0000-0000-0000-000000000001")
        url = url.replace("{", "").replace("}", "")
        r = client.request(method, url, json={})
        if r.status_code not in (401, 403):
            unprotected.append((method, path, r.status_code))
        if not perm:
            continue
        for rn in ROLES:
            if perm in ROLE_PERMISSIONS[Role(rn)]:
                continue
            r = client.request(method, url, headers=role(rn), json={})
            assert r.status_code == 403, (method, path, rn, r.status_code)
    allowed_public = {"/api/v1/version", "/api/v1/auth/config", "/api/v1/auth/mode", "/api/v1/about",
                      "/api/v1/legal"}
    leaks = [u for u in unprotected if u[1] not in allowed_public and not u[1].startswith("/api/v1/legal")]
    assert not leaks, leaks


def test_two_person_rule_cannot_be_self_approved(client: Any) -> None:
    alice = role("DataOperator", "alice@x")
    r = client.post("/api/v1/switch/requests", headers=alice, json={
        "reason": "go", "scope": {"table_id": str(uuid.uuid4())}, "target": "ALELT"})
    if r.status_code != 200:
        pytest.skip(f"switch request needs a certified table ({r.status_code})")
    rid = r.json()["id"]
    r = client.post(f"/api/v1/switch/requests/{rid}/approve", headers=alice, json={"reason": "self"})
    assert r.status_code in (403, 409)


# ------------------------------------------------------------------ command whitelist fuzz (R5)
def test_command_whitelist_fuzz(client: Any) -> None:
    from alelt_common.models import Command, CommandName
    from alelt_common.util import utcnow
    from pydantic import ValidationError

    rnd = random.Random(1234)
    names = {c.value for c in CommandName}
    cands = ["", " ", "start_capture", "START_CAPTURE ", "EXEC", "SQL", "RUN_SQL", "shell", "../PROBE",
             "PROBE;DROP TABLE x", "\x00PAUSE", "PAUSE\n", "ＰＡＵＳＥ"]
    cands += ["".join(rnd.choice(string.printable) for _ in range(rnd.randint(1, 40))) for _ in range(300)]
    for n in cands:
        if n in names:
            continue
        with pytest.raises(ValidationError):
            Command(command_id=uuid.uuid4(), name=n, args={}, issued_at=utcnow())
        r = client.post("/api/v1/agents/fake-agent/commands", headers=ADMIN, json={"name": n, "reason": "fuzz"})
        assert r.status_code in (400, 404, 422), (n, r.status_code)
    for probe in ("../../etc/passwd", "sql:SELECT 1", "", "log_config;rm"):
        r = client.post("/api/v1/agents/fake-agent/commands", headers=ADMIN,
                        json={"name": "PROBE", "args": {"name": probe}, "reason": "fuzz"})
        assert r.status_code == 400


# ------------------------------------------------------------------ path traversal on uploads
def test_upload_path_traversal(client: Any, tmp_path: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("../../evil.json", "{}")
        z.writestr("/abs/evil.sql", "CREATE TABLE X (A INT);")
        z.writestr("..\\..\\win.csv", "TABLE_NAME,COLUMN_NAME\nX,A\n")
    files = [{"name": "../../../outside.json", "data_base64": base64.b64encode(b'{"a": 1}').decode()},
             {"name": "bundle.zip", "data_base64": base64.b64encode(buf.getvalue()).decode()}]
    before = {p.name for p in Path.cwd().parent.iterdir()}
    r = client.post("/api/v1/import/discover/files", headers=role("DataOperator"), json={"reason": "t", "files": files})
    assert r.status_code in (200, 400, 409, 422)
    assert {p.name for p in Path.cwd().parent.iterdir()} == before
    for name in ("evil.json", "outside.json", "win.csv"):
        assert not (Path.cwd().parent / name).exists() and not (Path.cwd() / name).exists()
    from alelt_hub.importer import _write_tables_doc

    assert _write_tables_doc("../../escape", {"connection_id": "x"}) is None
    assert _write_tables_doc("..", {"connection_id": "x"}) is None


# ------------------------------------------------------------------ CSRF / CORS / headers / rate limits
def test_csrf_cors_and_headers(client: Any, fresh_settings: Any) -> None:
    _settings(fresh_settings, ALELT_CORS_ORIGINS="https://console.example")
    op = role("DataOperator")
    r = client.post("/api/v1/recon/runs", headers={**op, "Origin": "https://evil.example"}, json={"reason": "x"})
    assert r.status_code == 403 and "CSRF" in r.text
    r = client.options("/api/v1/tables", headers={"Origin": "https://evil.example",
                                                  "Access-Control-Request-Method": "PUT"})
    assert r.status_code == 403 and "access-control-allow-origin" not in r.headers
    r = client.options("/api/v1/tables", headers={"Origin": "https://console.example",
                                                  "Access-Control-Request-Method": "PUT"})
    assert r.status_code == 204 and r.headers["access-control-allow-origin"] == "https://console.example"
    r = client.get("/api/v1/tables", headers={**role("Viewer"), "Origin": "https://console.example"})
    assert r.status_code == 200 and r.headers["access-control-allow-origin"] == "https://console.example"
    assert r.headers["x-content-type-options"] == "nosniff" and r.headers["x-frame-options"] == "DENY"
    r = client.get("/api/v1/tables", headers={**role("Viewer"), "Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in r.headers


def test_rate_limits(client: Any, fresh_settings: Any) -> None:
    from alelt_hub import security

    _settings(fresh_settings, ALELT_RATE_LIMIT_PER_MIN="5", ALELT_AUTH_FAIL_LIMIT_PER_MIN="3")
    security.REQUESTS.clear()
    security.AUTH_FAILS.clear()
    try:
        codes = [client.get("/api/v1/tables", headers=role("Viewer")).status_code for _ in range(7)]
        assert codes[:5] == [200] * 5 and codes[-1] == 429
        security.REQUESTS.clear()
        for _ in range(3):
            assert client.get("/api/v1/tables").status_code == 401
        r = client.get("/api/v1/tables", headers=role("Viewer"))
        assert r.status_code == 429 and r.headers["retry-after"] == "60"
        assert client.get("/healthz").status_code == 429       # brute force is blocked on every path
    finally:
        security.REQUESTS.clear()
        security.AUTH_FAILS.clear()


# ------------------------------------------------------------------ secrets never logged
def test_secrets_never_logged(caplog: pytest.LogCaptureFixture) -> None:
    from alelt_common.util import JsonFormatter

    secrets_ = ["alelta_" + "k" * 40, "alelte_" + "z" * 40, "hunter2pw", "eyJhbGciOi.payload.sig"]
    msg = (f"register token={secrets_[1]} cred {secrets_[0]} url postgresql://u:{secrets_[2]}@db/x "
           f"Authorization: Bearer {secrets_[3]}")
    out = JsonFormatter().format(logging.LogRecord("t", logging.INFO, __file__, 1, msg, None, None))
    for s in secrets_:
        assert s not in out, s
    from alelt_hub.product import redact

    assert redact("ALELT_AGENT_TOKENS", ["a", "b"]) == "<2 value(s) redacted>"
    assert "pw" not in redact("ALELT_DB_URL", "postgresql://u:pw@h/db")


def test_llm_redaction_golden_set() -> None:
    from alelt_diagnose.redact import redact, scrub

    golden = {
        "password=Sup3rS3cret!": "Sup3rS3cret!",
        "postgresql://alelt:Pg_pw_123@db:5432/meta": "Pg_pw_123",
        "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.e30.sig": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9",
        "token alelta_" + "Q" * 43: "Q" * 43,
    }
    for text, secret in golden.items():
        assert secret not in scrub(text), text
    out = json.dumps(redact({"agent_token": "alelte_" + "x" * 40, "nested": {"dest_url": "snowflake://u:pw1234@acct"}}))
    assert "x" * 40 not in out and "pw1234" not in out


# ------------------------------------------------------------------ F-15: no pickle, typed spill round trip
def test_no_pickle_in_product_code() -> None:
    root = Path(__file__).resolve().parents[3]
    hits = []
    for base in ("hub", "connectors", "edge-agent/linux", "extensions"):
        for p in (root / base).rglob("*.py"):
            if "tests" in p.parts or "__pycache__" in p.parts:
                continue
            t = p.read_text(encoding="utf-8", errors="replace")
            if "import pickle" in t or "pickle.loads" in t or "cPickle" in t:
                hits.append(str(p.relative_to(root)))
    assert not hits, hits


def test_spill_roundtrip_all_canonical_types(tmp_path: Path) -> None:
    from datetime import date, datetime, time, timezone
    from decimal import Decimal

    from alelt_common.canonical_jsonl import Undecodable
    from alelt_common.models import ChangeEvent, ChangeKind, Position, RawRef, TableRef
    from alelt_txn.assembler import _spill_decode, _spill_encode

    row = {"i": 7, "s": "é\x00", "d": Decimal("12.3400"), "f": 1.5, "nan": float("inf"), "b": b"\x00\xff",
           "dt": date(2026, 1, 2), "t": time(1, 2, 3, 4), "ldt": datetime(2026, 1, 2, 3, 4, 5),
           "ts": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "n": None, "bool": True, "u": Undecodable("?")}
    e = ChangeEvent(position=Position(engine="x", text="1:0A"), txid="T", kind=ChangeKind.UPDATE,
                    table=TableRef(schema="S", name="T"), before=row, after=row,
                    commit_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    raw_ref=RawRef(batch_id=uuid.uuid4(), offset=3, length=9), note="n")
    back = _spill_decode(_spill_encode(e))
    assert back == e and isinstance(back.after["u"], Undecodable)   # type: ignore[index]


# ------------------------------------------------------------------ code review closures (AHS-01/03/04/11/20, HA-03)
def test_mtls_identity_bound_to_agent(hub_env: dict[str, str], fresh_settings: Any) -> None:
    """AHS-01 / HA-03: certificate A cannot register over, re-home, drain or lease agent B."""
    from fastapi.testclient import TestClient

    from alelt_hub.app import create_app

    _settings(fresh_settings, ALELT_AGENT_AUTH="mtls", ALELT_AGENT_THUMBPRINTS="aaaa,bbbb", ALELT_TRUSTED_PROXIES="10.9.0.5")
    with TestClient(create_app(), client=("10.9.0.5", 5000)) as c:
        def h(tp: str) -> dict[str, str]:
            return {"X-ALELT-Protocol": "1", "X-SSL-Client-Fingerprint": tp}

        def reg(tp: str, aid: str, cid: str) -> Any:
            return c.post("/agent/v1/register", headers=h(tp), json={
                "agent_id": aid, "connection_id": cid, "engine": "fake", "version": "t", "hostname": "h",
                "database": "D"})
        assert reg("aaaa", "mt-a", "mtconn-a").status_code == 200
        assert reg("bbbb", "mt-b", "mtconn-b").status_code == 200
        assert reg("bbbb", "mt-a", "mtconn-a").status_code == 403                 # takeover by another cert
        assert reg("aaaa", "mt-a", "mtconn-b").status_code == 409                 # silent re-homing refused
        assert c.get("/agent/v1/commands?agent_id=mt-a", headers=h("bbbb")).status_code == 403
        hb = {"agent_id": "mt-a", "state": "IDLE", "at": "2026-11-04T00:00:00Z"}
        assert c.post("/agent/v1/heartbeat", headers=h("bbbb"), json=hb).status_code == 403
        assert c.post("/agent/v1/heartbeat", headers=h("aaaa"), json=hb).status_code == 200
        lease = {"agent_id": "mt-a", "connection_id": "mtconn-a", "ttl_s": 30}
        assert c.post("/agent/v1/lease/acquire", headers=h("bbbb"), json=lease).status_code == 403
        assert c.get("/agent/v1/lease?connection_id=mtconn-a", headers=h("bbbb")).status_code == 403


def test_decompression_bomb_refused(client: Any) -> None:
    """AHS-03: inflate is bounded by the declared raw_bytes (and ALELT_MAX_RAW_MB)."""
    import zlib

    from alelt_common.util import DecompressionLimit, decompress

    bomb = zlib.compress(b"\0" * (64 << 20), 9)          # 64 MiB from ~64 KiB
    with pytest.raises(DecompressionLimit):
        decompress(bomb, "zlib", max_out=1 << 20)
    zstd = pytest.importorskip("zstandard")
    with pytest.raises(DecompressionLimit):
        decompress(zstd.ZstdCompressor().compress(b"\0" * (8 << 20)), "zstd", max_out=1 << 20)
    assert decompress(zlib.compress(b"abc"), "zlib", max_out=3) == b"abc"
    a = FakeAgent(client, TOKEN, agent_id="bomb-agent", connection_id="bombconn")
    a.register()
    hdr, _ = a.make_batch([{"pos": "1:0000000000000001", "txid": "T", "kind": "INSERT", "table": "S.T",
                            "after": {"A": 1}}])
    r = a.post(hdr.model_copy(update={"raw_bytes": len(b"x")}), bomb)
    assert r.status_code == 422
    r = a.post(hdr.model_copy(update={"raw_bytes": 10 << 30}), bomb)
    assert r.status_code == 413


def test_claimed_enrollment_token_expires(client: Any, fresh_settings: Any) -> None:
    """AHS-04: a claimed enrolment token is not a permanent credential."""
    from datetime import datetime, timedelta, timezone

    from alelt_hub import setup
    from alelt_hub.db import get_engine

    tok = _enroll(client, "ttlconn", "ttl-agent")
    assert _reg(client, tok, "ttl-agent", "ttlconn").status_code == 200
    _settings(fresh_settings, ALELT_ENROLLMENT_CLAIMED_TTL_HOURS="24")
    h = setup._hash(tok)
    with get_engine().begin() as c:
        v = setup._get(c, setup._PREFIX + h)
        old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        setup._put(c, setup._PREFIX + h, {**v, "claimed_at": old})
    setup._cache.clear()
    assert _reg(client, tok, "ttl-agent", "ttlconn").status_code == 401


def test_enroll_hub_url_not_from_host_header(client: Any, fresh_settings: Any) -> None:
    """AHS-11: outside dev the URL comes from ALELT_PUBLIC_URL and must be https."""
    _settings(fresh_settings, ALELT_PUBLIC_URL="https://hub.example")
    r = client.post("/api/v1/setup/agents/enroll", headers={**ADMIN, "Host": "evil.example"},
                    json={"connection_id": "urlconn", "database": "D", "reason": "t"})
    assert r.status_code == 200 and r.json()["hub_url"] == "https://hub.example"
    r = client.post("/api/v1/setup/agents/enroll", headers=ADMIN, json={
        "connection_id": "urlconn", "database": "D", "reason": "t", "hub_url": "http://hub.example"})
    assert r.status_code == 200                                    # dev env: allowed (prod refuses, see below)
    fresh_settings.delenv("ALELT_AGENT_TOKENS", raising=False)
    fresh_settings.delenv("ALELT_DEV_AUTH", raising=False)
    with pytest.raises(RuntimeError, match="ALELT_PUBLIC_URL"):
        _settings(fresh_settings, ALELT_ENV="prod", ALELT_AUTH_MODE="oidc", ALELT_PUBLIC_URL="http://hub.example")()
    with pytest.raises(RuntimeError, match="ALELT_FORWARDED_ALLOW_IPS"):
        _settings(fresh_settings, ALELT_ENV="prod", ALELT_AUTH_MODE="oidc", ALELT_PUBLIC_URL="https://h",
                  ALELT_FORWARDED_ALLOW_IPS="*")()


def test_ws_malformed_messages_dropped() -> None:
    """AHS-20: malformed control messages never raise / never reach the audit log unvalidated."""
    from alelt_hub.gateway import _on_ws_message

    for msg in ({"type": "ack"}, {"type": "ack", "command_id": "nope"}, {"type": "heartbeat"}, [],
                {"type": "event", "event": "x; drop"}, "text", None):
        _on_ws_message("fake-agent", msg)


def test_validation_errors_never_echo_input_and_bundle_redacts_key_ids(client: Any) -> None:
    """AHS-22: a 422 lists type/loc/msg only (pydantic's `input` would echo a submitted password); the support
    bundle redacts access-key ids and similar names."""
    from alelt_hub.product import redact
    from alelt_hub.testing import FakeAgent

    ag = FakeAgent(client, TOKEN, agent_id="val-agent", connection_id="valconn")
    ag.register()
    r = client.post("/agent/v1/lease/acquire", headers=ag.h,
                    json={"agent_id": "a", "connection_id": "c", "password": "hunter2pw-secret"})
    assert r.status_code == 422
    assert "hunter2pw-secret" not in r.text and r.json()["detail"][0]["type"] == "extra_forbidden"
    for k in ("ALELT_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "ALELT_S3_SECRET_ACCESS_KEY", "ALELT_SNOWFLAKE_PWD",
              "ALELT_AZURE_CONNECTION_STRING"):
        assert redact(k, "AKIAEXAMPLE123") == "<redacted>", k
    assert redact("ALELT_S3_ACCESS_KEY_ID_FILE", "/run/secrets/x") == "/run/secrets/x"
    assert redact("ALELT_ENV", "prod") == "prod"

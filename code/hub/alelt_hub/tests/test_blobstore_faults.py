"""S3 blob store fault injection (HA-06/07/16/17/18): 5xx retried, lost / unconfirmed PUT, truncated GET, SSE-KMS ETag,
expired token with credential reload, HEAD 403 treated as unknown, cached + capped usage listing. A scripted in-process
S3 endpoint: each request may consume one queued fault for its method."""
from __future__ import annotations

import hashlib
import http.server
import threading
from collections.abc import Iterator
from typing import Any

import pytest


class _S3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.faults: dict[str, list[str]] = {}
        self.requests: list[tuple[str, str, str]] = []      # (method, path, access key)
        self.valid_keys = {"AK1"}

    def fault(self, method: str, *kinds: str) -> None:
        self.faults.setdefault(method, []).extend(kinds)


@pytest.fixture()
def s3(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[_S3, Any]]:
    state = _S3()

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a: Any) -> None:
            pass

        def _send(self, code: int, body: bytes = b"", headers: dict[str, str] | None = None,
                  length: int | None = None) -> None:
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body) if length is None else length))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _handle(self) -> None:
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n) if n else b""
            auth = self.headers.get("Authorization", "")
            key = auth.split("Credential=")[1].split("/")[0] if "Credential=" in auth else ""
            state.requests.append((self.command, self.path, key))
            f = state.faults.get(self.command) or []
            kind = f.pop(0) if f else ""
            if key not in state.valid_keys:
                return self._send(403, b"<Error><Code>ExpiredToken</Code></Error>")
            path = self.path.split("?")[0]
            if kind == "503":
                return self._send(503, b"<Error><Code>SlowDown</Code></Error>")
            if kind == "403":
                return self._send(403, b"<Error><Code>AccessDenied</Code></Error>")
            if self.command == "PUT":
                if kind != "lost":
                    state.objects[path] = body
                md5 = hashlib.md5(body, usedforsecurity=False).hexdigest()
                if kind == "kms":
                    return self._send(200, headers={"ETag": '"0123456789abcdef0123456789abcdef"',
                                                    "x-amz-server-side-encryption": "aws:kms"})
                if kind in ("badetag", "lost"):
                    return self._send(200, headers={"ETag": '"ffffffffffffffffffffffffffffffff"'})
                return self._send(200, headers={"ETag": f'"{md5}"'})
            if self.command in ("GET", "HEAD"):
                if "list-type" in self.path:
                    from urllib.parse import parse_qs, urlsplit

                    pre = parse_qs(urlsplit(self.path).query).get("prefix", [""])[0]
                    items = "".join(f"<Contents><Key>{k.split('/', 2)[2]}</Key><Size>{len(v)}</Size></Contents>"
                                    for k, v in sorted(state.objects.items()) if k.split("/", 2)[2].startswith(pre))
                    return self._send(200, f"<ListBucketResult><IsTruncated>false</IsTruncated>{items}"
                                           "</ListBucketResult>".encode())
                if path not in state.objects:
                    return self._send(404)
                data = state.objects[path]
                if kind == "truncate":
                    self._send(200, data[: len(data) // 2], length=len(data))
                    self.close_connection = True
                    return None
                return self._send(200, data)
            if self.command == "DELETE":
                state.objects.pop(path, None)
                return self._send(204)
            return self._send(400)

        do_GET = do_PUT = do_HEAD = do_DELETE = _handle

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    for k in ("ALELT_S3_SESSION_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("ALELT_S3_ENDPOINT", f"http://127.0.0.1:{srv.server_address[1]}")
    monkeypatch.setenv("ALELT_S3_ACCESS_KEY_ID", "AK1")
    monkeypatch.setenv("ALELT_S3_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("ALELT_S3_ADDRESSING", "path")
    monkeypatch.setenv("ALELT_S3_REGION", "us-east-1")
    monkeypatch.setenv("ALELT_S3_TIMEOUT_S", "5")
    from alelt_hub import blobstore

    monkeypatch.setattr(blobstore.time, "sleep", lambda s: None)         # retries without waiting
    blobstore.reset_cache()
    st = blobstore.S3BlobStore("s3://bkt/spool")
    try:
        yield state, st
    finally:
        srv.shutdown()
        blobstore.reset_cache()


def test_5xx_retried_then_ok(s3: tuple[_S3, Any]) -> None:
    state, st = s3
    state.fault("PUT", "503", "503")
    assert st.put("a/1", b"hello").endswith("/a/1")
    state.fault("GET", "503")
    assert st.get("a/1") == b"hello"
    assert sum(1 for r in state.requests if r[0] == "PUT") == 3


def test_5xx_forever_raises(s3: tuple[_S3, Any]) -> None:
    from alelt_hub.blobstore import BlobError

    state, st = s3
    state.fault("PUT", *["503"] * 4)
    with pytest.raises(BlobError, match="failed after retries"):
        st.put("a/2", b"x")


def test_unconfirmed_put_is_an_error(s3: tuple[_S3, Any]) -> None:
    from alelt_hub.blobstore import BlobError

    state, st = s3
    state.fault("PUT", "lost")                     # 200 but the object never landed / ETag is not our MD5
    with pytest.raises(BlobError, match="write not confirmed"):
        st.put("a/3", b"data")


def test_sse_kms_etag_is_not_md5(s3: tuple[_S3, Any]) -> None:
    state, st = s3
    state.fault("PUT", "kms")
    assert st.put("a/4", b"encrypted") == st.ref("a/4")    # integrity is enforced by Content-MD5, not the ETag


def test_truncated_get_retried(s3: tuple[_S3, Any]) -> None:
    from alelt_hub.blobstore import BlobError

    state, st = s3
    st.put("a/5", b"0123456789" * 100)
    state.fault("GET", "truncate")
    assert st.get("a/5") == b"0123456789" * 100
    state.fault("GET", *["truncate"] * 4)
    with pytest.raises(BlobError):
        st.get("a/5")


def test_expired_credentials_reloaded_from_file(s3: tuple[_S3, Any], tmp_path: Any,
                                                monkeypatch: pytest.MonkeyPatch) -> None:
    state, st = s3
    st.put("a/6", b"one")
    state.valid_keys = {"AK2"}                     # STS rotation: the old key is now ExpiredToken
    f = tmp_path / "ak"
    f.write_text("AK2\n", encoding="utf-8")
    monkeypatch.delenv("ALELT_S3_ACCESS_KEY_ID")
    monkeypatch.setenv("ALELT_S3_ACCESS_KEY_ID_FILE", str(f))
    assert st.get("a/6") == b"one"                 # 403 ExpiredToken -> reload -> retry succeeds
    assert st.cfg.access_key == "AK2" and state.requests[-1][2] == "AK2"


def test_head_403_is_unknown_and_copy_rewrites(s3: tuple[_S3, Any]) -> None:
    from alelt_hub import blobstore

    state, st = s3
    st.put("b/1", b"payload")
    state.fault("HEAD", "403")
    assert st.size("b/1") is None
    other = blobstore.S3BlobStore("s3://bkt/other")
    state.fault("HEAD", "403")
    rep = blobstore.copy_store(st, other)
    assert rep.ok() and rep.copied == 1 and other.get("b/1") == b"payload"


def test_usage_listing_cached_and_capped(s3: tuple[_S3, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import blobstore

    state, st = s3
    for i in range(5):
        st.put(f"c/{i}", b"x" * (i + 1))
    monkeypatch.setenv("ALELT_SPOOL_URL", "s3://bkt/spool")
    blobstore.reset_cache()
    n_list = lambda: sum(1 for r in state.requests if "list-type" in r[1])   # noqa: E731
    before = n_list()
    assert blobstore.used_bytes() == 15 and blobstore.object_count() == 5
    assert blobstore.used_bytes() == 15                                      # cached: one listing
    assert n_list() == before + 1
    monkeypatch.setenv("ALELT_S3_LIST_CAP", "2")
    assert blobstore.object_count(max_age_s=0) == 2                          # capped

"""Blob store contract (X-46): the same behaviour for the local spool and any S3-compatible store."""
from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from alelt_hub import blobstore
from alelt_hub.blobstore import BlobError, LocalBlobStore, S3BlobStore, S3Config

from .s3fake import bucket, s3_endpoint


@pytest.fixture(scope="module")
def s3env() -> Iterator[dict[str, str]]:
    with s3_endpoint() as env:
        yield env


@pytest.fixture(params=["local", "s3"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    if request.param == "local":
        yield LocalBlobStore(tmp_path / "spool")
        return
    request.getfixturevalue("s3env")
    yield bucket(f"s3://alelt-test-{uuid.uuid4().hex[:8]}/hub/spool")


def test_contract(store: Any) -> None:
    key = blobstore.batch_key("tm test/1", "ab" + uuid.uuid4().hex, "zlib")
    assert key.startswith("tm_test_1/ab/")
    data = b"\x00\x01raw" * 1000
    ref = store.put(key, data)
    assert store.get(key) == data and store.size(key) == len(data)
    assert store.key_of(ref) == key and store.ref(key) == ref
    assert blobstore.read_ref(ref) == data if store.kind == "local" else True
    store.put(key, b"overwritten")                               # put is idempotent / last write wins
    assert store.get(key) == b"overwritten"
    k2 = blobstore.snapshot_key(str(uuid.uuid4()), 3, "zlib")
    store.put(k2, b"chunk")
    assert sorted(k for k, _ in store.list()) == sorted([key, k2])
    assert [k for k, _ in store.list("snapshots/")] == [k2]
    store.delete(key)
    assert store.size(key) is None
    with pytest.raises(BlobError):
        store.get(key)
    store.delete(key)                                            # deleting a missing object is fine
    store.probe()
    assert store.key_of("s3://other-bucket/x/y") is None


def test_read_ref_any_store(s3env: dict[str, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = bucket("s3://alelt-ref-test/p")
    ref = s3.put("c/ab/x.zlib", b"in s3")                        # type: ignore[attr-defined]
    monkeypatch.setenv("ALELT_SPOOL_URL", "s3://alelt-ref-test/p")
    assert blobstore.read_ref(ref) == b"in s3"
    loc = LocalBlobStore(tmp_path / "old")
    lref = loc.put("c/ab/y.zlib", b"legacy local")
    assert blobstore.read_ref(lref) == b"legacy local"            # absolute path refs (earlier releases) stay readable
    assert blobstore.read_ref(loc.url + "/c/ab/y.zlib") == b"legacy local"
    assert blobstore.describe()["kind"] == "s3" and blobstore.ready()


def test_copy_both_ways_verified(s3env: dict[str, str], tmp_path: Path) -> None:
    loc = LocalBlobStore(tmp_path / "a")
    for i in range(25):
        loc.put(blobstore.batch_key("conn", f"{i:02x}" + uuid.uuid4().hex, "zlib"), bytes([i]) * (i * 100 + 1))
    s3 = bucket("s3://alelt-copy-test/spool")
    r1 = blobstore.copy_store(loc, s3)                              # type: ignore[arg-type]
    assert r1.ok() and r1.objects == r1.copied == 25
    r2 = blobstore.copy_store(loc, s3)                              # type: ignore[arg-type]
    assert r2.ok() and r2.skipped == 25 and r2.copied == 0          # idempotent re-run
    back = LocalBlobStore(tmp_path / "b")
    r3 = blobstore.copy_store(s3, back)                             # type: ignore[arg-type]
    assert r3.ok() and r3.copied == 25
    digest = {k: hashlib.sha256(loc.get(k)).hexdigest() for k, _ in loc.list()}
    assert digest == {k: hashlib.sha256(back.get(k)).hexdigest() for k, _ in back.list()}
    s3.put(next(iter(digest)), b"tampered")                         # type: ignore[attr-defined]
    assert not blobstore.copy_store(loc, s3, verify_only=True).ok()  # type: ignore[arg-type]


def test_local_rejects_escape(tmp_path: Path) -> None:
    with pytest.raises(BlobError):
        LocalBlobStore(tmp_path / "s").put("../outside", b"x")


def test_sigv4_known_vector() -> None:
    """AWS documented example (S3 SigV4, GET Object with Range)."""
    cfg = S3Config(bucket="examplebucket", prefix="", endpoint="https://s3.amazonaws.com", region="us-east-1",
                   access_key="AKIAIOSFODNN7EXAMPLE", secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                   session_token="", addressing="virtual", timeout_s=5)
    st = S3BlobStore("s3://examplebucket", cfg)
    h = st._sign("GET", "/test.txt", {}, {"Range": "bytes=0-9"}, hashlib.sha256(b"").hexdigest(),
                 now=datetime(2013, 5, 24, tzinfo=timezone.utc))
    assert h["authorization"].endswith(
        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
    assert "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date" in h["authorization"]

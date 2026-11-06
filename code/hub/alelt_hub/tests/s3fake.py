"""S3-compatible endpoint for tests: ALELT_TEST_S3_ENDPOINT (+ ALELT_TEST_S3_ACCESS_KEY_ID / ALELT_TEST_S3_SECRET_ACCESS_KEY),
e.g. a MinIO / SeaweedFS / Ceph container, else an in-process moto server if moto is installed, else skip."""
from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

S3_ENV = ("ALELT_S3_ENDPOINT", "ALELT_S3_ACCESS_KEY_ID", "ALELT_S3_SECRET_ACCESS_KEY", "ALELT_S3_REGION", "ALELT_S3_ADDRESSING",
          "ALELT_SPOOL_URL")


@contextmanager
def s3_endpoint() -> Iterator[dict[str, str]]:
    old = {k: os.environ.get(k) for k in S3_ENV}
    server = None
    if os.environ.get("ALELT_TEST_S3_ENDPOINT"):
        env = {"ALELT_S3_ENDPOINT": os.environ["ALELT_TEST_S3_ENDPOINT"],
               "ALELT_S3_ACCESS_KEY_ID": os.environ.get("ALELT_TEST_S3_ACCESS_KEY_ID", ""),
               "ALELT_S3_SECRET_ACCESS_KEY": os.environ.get("ALELT_TEST_S3_SECRET_ACCESS_KEY", "")}
    else:
        try:
            from moto.server import ThreadedMotoServer
        except ImportError:
            pytest.skip("no S3 endpoint: set ALELT_TEST_S3_ENDPOINT or pip install 'moto[server]'")
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
        server.start()
        env = {"ALELT_S3_ENDPOINT": f"http://127.0.0.1:{port}", "ALELT_S3_ACCESS_KEY_ID": "alelttest",
               "ALELT_S3_SECRET_ACCESS_KEY": "alelttestsecret"}
    env |= {"ALELT_S3_REGION": "us-east-1", "ALELT_S3_ADDRESSING": "path"}
    os.environ.update(env)
    from alelt_hub import blobstore

    blobstore.reset_cache()
    try:
        yield env
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        blobstore.reset_cache()
        if server is not None:
            server.stop()


def bucket(url: str) -> object:
    from alelt_hub import blobstore

    st = blobstore.open_store(url)
    st.ensure_bucket()  # type: ignore[attr-defined]
    return st

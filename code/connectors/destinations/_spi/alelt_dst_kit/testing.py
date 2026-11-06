"""Test helpers: an S3 endpoint from moto run in a SEPARATE process (Rust/C++ object-store clients hold the GIL
during I/O, so an in-process server would deadlock). Apache-2.0 moto[server]; skipped when not installed."""
from __future__ import annotations

import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def moto_s3(bucket: str = "lake") -> Iterator[str]:
    """Yields the endpoint URL (http://127.0.0.1:<port>) with `bucket` created; credentials x/x, us-east-1."""
    import boto3

    with socket.socket() as so:
        so.bind(("127.0.0.1", 0))
        port = so.getsockname()[1]
    proc = subprocess.Popen([sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ep = f"http://127.0.0.1:{port}"
    try:
        for _ in range(150):
            try:
                urllib.request.urlopen(ep, timeout=1)
                break
            except OSError:
                time.sleep(0.2)
        boto3.client("s3", endpoint_url=ep, region_name="us-east-1", aws_access_key_id="x",
                     aws_secret_access_key="x").create_bucket(Bucket=bucket)
        yield ep
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()

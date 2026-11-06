"""Durable hub spool: raw batches are written (tmp + fsync + rename) before the batch row is inserted."""
from __future__ import annotations

import os
from pathlib import Path


def durable_write(path: Path, data: bytes) -> None:
    """Write `data` to `path` atomically and durably."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    if os.name != "nt":                      # directory fsync (POSIX only)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def batch_path(root: Path, connection_id: str, batch_id: str, codec: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in connection_id)
    return root / safe / batch_id[:2] / f"{batch_id}.{codec}"

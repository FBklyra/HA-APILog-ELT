"""Durable local spool: one file per entry, `<seq hex16>.batch` (wire batch) or `<seq hex16>.catalog` (catalog
snapshot to post before the batches that follow it). File = header JSON line + body.

Every write: tmp file -> fsync -> rename -> fsync(directory), so an entry either exists complete or not at all.
Batches are deleted only up to the hub's durable position (ACK); a full spool pauses reading (never drops).

Robustness (LNX-01/07/29): an entry that cannot be parsed is moved to `quarantine/` (never deleted, never fatal);
a full disk (ENOSPC/EDQUOT) is reported as `SpoolFull` (backpressure, like the size cap); headers are read from
the first line only and cached, so a large backlog is not re-read on every release.
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

KINDS = (".batch", ".catalog")
MAX_HEADER = 1 << 20                         # a header line longer than this is corruption
DISK_RESERVE = 64 << 20                      # keep this much free on the spool volume before reading more
_NOSPACE = {errno.ENOSPC, getattr(errno, "EDQUOT", errno.ENOSPC)}


class SpoolFull(OSError):
    """The spool volume has no space left: backpressure (stop reading, keep everything), not fatal."""


class CorruptEntry(ValueError):
    """A spool entry whose header or body cannot be trusted."""


def no_space(e: BaseException) -> bool:
    return isinstance(e, OSError) and e.errno in _NOSPACE


def fsync_dir(path: Path) -> None:
    if os.name == "nt":                      # directories cannot be opened for fsync on Windows
        return
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _valid_name(p: Path) -> bool:
    return len(p.stem) == 16 and all(c in "0123456789abcdef" for c in p.stem)


class Spool:
    def __init__(self, root: str, reserve_bytes: int = DISK_RESERVE) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.qdir = self.root / "quarantine"
        self.reserve = reserve_bytes
        self.quarantined: List[Tuple[str, str]] = []          # (file name, reason) since start
        self._hdr: Dict[Path, Dict[str, Any]] = {}
        for t in self.root.glob("*.tmp"):     # interrupted writes: never renamed => never part of the spool
            t.unlink()
        ents: List[Path] = []
        for p in sorted(x for x in self.root.iterdir() if x.is_file() and x.suffix in KINDS):
            if _valid_name(p):
                ents.append(p)
            else:
                self._move_to_quarantine(p, "unexpected file name")
        self._entries: List[Path] = ents
        self._bytes = sum(p.stat().st_size for p in self._entries)
        self._seq = int(self._entries[-1].stem, 16) if self._entries else 0

    # ------------------------------------------------------------------ inventory
    def entries(self) -> List[Path]:
        return list(self._entries)

    def batches(self) -> List[Path]:
        return [p for p in self._entries if p.suffix == ".batch"]

    def bytes(self) -> int:
        return self._bytes

    def full(self, cap_bytes: int) -> bool:
        return self._bytes >= cap_bytes or self.disk_low()

    def disk_low(self) -> bool:
        """Free space on the spool volume below the reserve (statvfs via shutil.disk_usage)."""
        if self.reserve <= 0:
            return False
        try:
            return shutil.disk_usage(str(self.root)).free < self.reserve
        except OSError:
            return False

    # ------------------------------------------------------------------ write
    def _put(self, ext: str, hdr: Dict[str, Any], body: bytes) -> Path:
        self._seq += 1
        p = self.root / f"{self._seq:016x}{ext}"
        tmp = p.with_suffix(ext + ".tmp")
        data = json.dumps(hdr, separators=(",", ":")).encode() + b"\n" + body
        try:
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, p)
            fsync_dir(self.root)
        except OSError as e:
            self._seq -= 1
            try:
                tmp.unlink()
            except OSError:
                pass
            if no_space(e):
                raise SpoolFull(e.errno, f"spool volume full: {e.strerror}") from None
            raise
        self._entries.append(p)
        self._bytes += len(data)
        self._hdr[p] = hdr
        return p

    def put_batch(self, hdr: Dict[str, Any], body: bytes) -> Path:
        return self._put(".batch", hdr, body)

    def put_catalog(self, snapshot: Dict[str, Any]) -> Path:
        return self._put(".catalog", {"kind": "catalog"}, json.dumps(snapshot).encode())

    # ------------------------------------------------------------------ read
    @staticmethod
    def _parse_header(line: bytes, p: Path) -> Dict[str, Any]:
        try:
            hdr = json.loads(line)
        except ValueError as e:
            raise CorruptEntry(f"{p.name}: header is not JSON ({e})") from None
        if not isinstance(hdr, dict):
            raise CorruptEntry(f"{p.name}: header is not an object")
        if p.suffix == ".batch":
            for k in ("from_pos", "to_pos"):
                if not isinstance(hdr.get(k), dict) or not isinstance(hdr[k].get("text"), str):
                    raise CorruptEntry(f"{p.name}: header.{k} missing or malformed")
            nxt = hdr.get("next_pos")
            if nxt is not None and not (isinstance(nxt, dict) and isinstance(nxt.get("text"), str)):
                raise CorruptEntry(f"{p.name}: header.next_pos malformed")
        return hdr

    def header(self, p: Path) -> Dict[str, Any]:
        """Header only (first line, cached). CorruptEntry when it cannot be parsed (LNX-29)."""
        h = self._hdr.get(p)
        if h is not None:
            return h
        try:
            with open(p, "rb") as f:
                line = f.readline(MAX_HEADER + 1)
        except FileNotFoundError:
            raise
        except OSError as e:
            raise CorruptEntry(f"{p.name}: unreadable ({e.strerror})") from None
        if not line.endswith(b"\n"):
            raise CorruptEntry(f"{p.name}: header line truncated")
        h = self._parse_header(line, p)
        self._hdr[p] = h
        return h

    def get(self, p: Path) -> Tuple[Dict[str, Any], bytes]:
        try:
            data = p.read_bytes()
        except FileNotFoundError:
            raise
        except OSError as e:
            raise CorruptEntry(f"{p.name}: unreadable ({e.strerror})") from None
        line, nl, body = data.partition(b"\n")
        if not nl:
            raise CorruptEntry(f"{p.name}: no header line")
        hdr = self._parse_header(line, p)
        self._hdr[p] = hdr
        return hdr, body

    # ------------------------------------------------------------------ quarantine / delete
    def _move_to_quarantine(self, p: Path, why: str) -> None:
        try:
            self.qdir.mkdir(exist_ok=True)
            dst = self.qdir / p.name
            n = 1
            while dst.exists():
                dst = self.qdir / f"{p.name}.{n}"
                n += 1
            os.replace(p, dst)
            fsync_dir(self.root)
        except FileNotFoundError:
            return
        except OSError:                          # cannot move (read-only volume?): out of the inventory anyway
            pass
        self.quarantined.append((p.name, why))

    def quarantine(self, p: Path, why: str) -> None:
        """Move an untrusted entry aside: kept for support, never shipped, never fatal (LNX-01)."""
        try:
            size = p.stat().st_size
        except OSError:
            size = 0
        self._move_to_quarantine(p, why)
        if p in self._entries:
            self._entries.remove(p)
            self._bytes = max(0, self._bytes - size)
        self._hdr.pop(p, None)

    def entries_from(self, p: Path) -> List[Path]:
        """`p` and every entry after it (oldest first)."""
        return [q for q in self._entries if q.name >= p.name]

    def delete(self, p: Path) -> None:
        try:
            size = p.stat().st_size
            p.unlink()
        except FileNotFoundError:
            size = 0
        if p in self._entries:
            self._entries.remove(p)
            self._bytes = max(0, self._bytes - size)
        self._hdr.pop(p, None)

    def release(self, durable_pos: str, cmp: Callable[[str, str], int]) -> int:
        """Delete batches with to_pos <= durable_pos (only after a hub ACK). Catalog entries in front of a released
        batch were posted already and go too."""
        n = 0
        for p in list(self._entries):
            if p.suffix == ".catalog":
                continue
            try:
                hdr = self.header(p)
            except CorruptEntry:
                break                            # the sender quarantines it when it reaches it
            except FileNotFoundError:
                self.delete(p)
                continue
            if cmp(hdr["to_pos"]["text"], durable_pos) > 0:
                break
            for q in [q for q in self._entries if q.suffix == ".catalog" and q.name < p.name]:
                self.delete(q)
            self.delete(p)
            n += 1
        return n

    def purge(self) -> int:
        n = len(self._entries)
        for p in list(self._entries):
            self.delete(p)
        fsync_dir(self.root)
        return n

    def last_next(self) -> Optional[str]:
        """next_pos of the newest readable spooled batch (resume point after a restart)."""
        for p in reversed(list(self._entries)):
            if p.suffix == ".batch":
                try:
                    hdr = self.header(p)
                except (CorruptEntry, FileNotFoundError) as e:
                    self.quarantine(p, str(e))
                    continue
                nxt = hdr.get("next_pos") or hdr["to_pos"]
                return str(nxt["text"])
        return None


def verify_body(hdr: Dict[str, Any], body: bytes, max_raw: int = 1 << 30) -> None:
    """LNX-02: the body still decodes to `raw_bytes` bytes with `sha256` (bit rot, truncated file, bad codec)."""
    codec = hdr.get("codec") or "none"
    try:
        if codec == "zlib":
            import zlib
            d = zlib.decompressobj()
            raw = d.decompress(body, max_raw + 1)
            if d.unconsumed_tail or len(raw) > max_raw:
                raise CorruptEntry("body inflates beyond the limit")
            if not d.eof:                        # truncated stream (incl. a lost adler32 trailer)
                raise CorruptEntry("zlib body truncated")
        elif codec == "zstd":
            import zstandard
            raw = zstandard.ZstdDecompressor().decompress(body, max_output_size=max_raw)
        elif codec == "none":
            raw = body
        else:
            raise CorruptEntry(f"unknown codec {codec!r}")
    except CorruptEntry:
        raise
    except Exception as e:  # noqa: BLE001 - zlib.error / zstd.ZstdError / ImportError
        raise CorruptEntry(f"body does not decode ({type(e).__name__}: {e})") from None
    if hdr.get("raw_bytes") is not None and len(raw) != int(hdr["raw_bytes"]):
        raise CorruptEntry(f"body is {len(raw)} bytes, header says raw_bytes={hdr['raw_bytes']}")
    if hdr.get("sha256") and hashlib.sha256(raw).hexdigest() != str(hdr["sha256"]).lower():
        raise CorruptEntry("body sha256 does not match the header")

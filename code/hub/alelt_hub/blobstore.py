"""Hub spool / raw archive blob store: `local` (one host, non-HA) or `s3` (any S3-compatible store, HA).

Configured by one URL (X-46):
  ALELT_SPOOL_URL=file:///var/lib/alelt/spool     (default: file:// + ALELT_SPOOL_DIR)
  ALELT_SPOOL_URL=s3://bucket/prefix                   + ALELT_S3_ENDPOINT, ALELT_S3_REGION, ALELT_S3_ACCESS_KEY_ID,
                                                      ALELT_S3_SECRET_ACCESS_KEY, ALELT_S3_SESSION_TOKEN, ALELT_S3_ADDRESSING
`put()` returns only after the store confirmed the write (local: fsync + atomic rename; S3: PUT 200 with Content-MD5
and a signed sha256 payload hash), so the hub ACKs a batch only once it is durable in the configured store.

Every object has a self-describing **ref** stored in the metadata (`batch.spool_path`): an absolute file path for the
local store (unchanged from earlier releases), `s3://bucket/prefix/key` for S3. `read_ref()` reads any ref regardless
of the store currently configured, so a ref written before a topology switch stays readable while objects are copied.
The S3 client is a small built-in AWS Signature V4 implementation (stdlib only; no vendor SDK).

Robustness (HA-06/07/16/17/18): integrity of a PUT is enforced by the store through `Content-MD5` (a mismatch is a
400 BadDigest); the ETag is compared only when the object is not SSE-KMS / SSE-C encrypted (their ETag is not the
MD5). Credentials are re-read from the environment / `*_FILE` secrets every ALELT_S3_CRED_TTL_S (300 s) and at once on
ExpiredToken / InvalidAccessKeyId / InvalidToken / SignatureDoesNotMatch (rotated STS / IRSA keys need no restart).
Transport errors (OSError, timeouts, http.client.HTTPException such as IncompleteRead) and truncated GET bodies
(Content-Length mismatch) are retried. A HEAD answered 403 (key scoped without ListBucket) is "unknown", never an
error: the copy then re-writes the object (idempotent). Bucket listings for usage are cached (ALELT_SPOOL_USAGE_TTL_S,
300 s) and capped (ALELT_S3_LIST_CAP objects).
"""
from __future__ import annotations

import base64
import dataclasses
import hashlib
import hmac
import http.client
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from .spool import durable_write


class BlobError(RuntimeError):
    """Store unreachable, write not confirmed, or object missing."""


class BlobStore(Protocol):
    url: str
    kind: str           # "local" | "s3"
    ha: bool            # usable by several hosts at once

    def ref(self, key: str) -> str: ...
    def key_of(self, ref: str) -> str | None: ...
    def put(self, key: str, data: bytes) -> str: ...
    def get(self, key: str) -> bytes: ...
    def size(self, key: str) -> int | None: ...
    def delete(self, key: str) -> None: ...
    def list(self, prefix: str = "") -> Iterator[tuple[str, int]]: ...
    def probe(self) -> None: ...


def _safe(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in s)


def batch_key(connection_id: str, batch_id: str, codec: str) -> str:
    """Same relative layout as the historical local spool: <connection>/<xx>/<batch_id>.<codec>."""
    return f"{_safe(connection_id)}/{batch_id[:2]}/{batch_id}.{codec}"


def snapshot_key(snapshot_id: str, chunk_no: int, codec: str) -> str:
    return f"snapshots/{snapshot_id}/{chunk_no:08d}.{codec}"


# ------------------------------------------------------------------ local
class LocalBlobStore:
    kind = "local"
    ha = False

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.url = "file://" + ("/" if not self.root.as_posix().startswith("/") else "") + self.root.as_posix()

    def _path(self, key: str) -> Path:
        p = (self.root / key).resolve()
        if self.root not in p.parents and p != self.root:
            raise BlobError(f"key escapes the spool root: {key!r}")
        return p

    def ref(self, key: str) -> str:
        return str(self._path(key))

    def key_of(self, ref: str) -> str | None:
        try:
            rel = Path(ref).resolve().relative_to(self.root)
        except (ValueError, OSError):
            return None
        return rel.as_posix()

    def put(self, key: str, data: bytes) -> str:
        p = self._path(key)
        durable_write(p, data)
        return str(p)

    def get(self, key: str) -> bytes:
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError:
            raise BlobError(f"object not found: {key}") from None

    def size(self, key: str) -> int | None:
        p = self._path(key)
        return p.stat().st_size if p.is_file() else None

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def list(self, prefix: str = "") -> Iterator[tuple[str, int]]:
        if not self.root.exists():
            return
        for f in sorted(self.root.rglob("*")):
            if f.is_file() and not f.name.endswith(".tmp"):
                k = f.relative_to(self.root).as_posix()
                if k.startswith(prefix) and not k.startswith("txn-spill/"):
                    yield k, f.stat().st_size

    def probe(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        k = f".probe/{os.getpid()}-{time.time_ns()}"
        self.put(k, b"probe")
        if self.get(k) != b"probe":
            raise BlobError("local spool read-back mismatch")
        self.delete(k)


# ------------------------------------------------------------------ S3 (Signature V4, stdlib)
@dataclass(frozen=True)
class S3Config:
    bucket: str
    prefix: str
    endpoint: str          # scheme://host[:port], empty = AWS regional endpoint
    region: str
    access_key: str
    secret_key: str
    session_token: str
    addressing: str        # path | virtual
    timeout_s: float

    @classmethod
    def from_env(cls, url: str) -> "S3Config":
        u = urllib.parse.urlsplit(url)
        if not u.netloc:
            raise BlobError(f"S3 URL needs a bucket: {url!r} (s3://bucket/prefix)")
        e = os.environ
        endpoint = e.get("ALELT_S3_ENDPOINT", "").rstrip("/")
        region = e.get("ALELT_S3_REGION") or e.get("AWS_REGION") or e.get("AWS_DEFAULT_REGION") or "us-east-1"
        return cls(bucket=u.netloc, prefix=u.path.strip("/"), endpoint=endpoint, region=region,
                   access_key=_secret("ALELT_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
                   secret_key=_secret("ALELT_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
                   session_token=_secret("ALELT_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"),
                   addressing=e.get("ALELT_S3_ADDRESSING") or ("path" if endpoint else "virtual"),
                   timeout_s=float(e.get("ALELT_S3_TIMEOUT_S", "30")))


def _secret(*names: str) -> str:
    """First non-empty value of NAME or the file named by NAME_FILE (re-read on every call: rotated secrets)."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
        f = os.environ.get(n + "_FILE")
        if f:
            try:
                return Path(f).read_text(encoding="utf-8").strip()
            except OSError:
                pass
    return ""


_CRED_ERRORS = (b"ExpiredToken", b"InvalidAccessKeyId", b"InvalidToken", b"SignatureDoesNotMatch",
                b"TokenRefreshRequired")
_SSE_OPAQUE_ETAG = ("aws:kms", "aws:kms:dsse")


def _q(s: str, safe: str = "-_.~") -> str:
    return urllib.parse.quote(s, safe=safe)


class S3BlobStore:
    kind = "s3"
    ha = True

    def __init__(self, url: str, cfg: S3Config | None = None) -> None:
        self._from_env = cfg is None
        self._src_url = url
        self.cfg = cfg or S3Config.from_env(url)
        self._cred_at = time.monotonic()
        self.url = f"s3://{self.cfg.bucket}" + (f"/{self.cfg.prefix}" if self.cfg.prefix else "")
        ep = self.cfg.endpoint or f"https://s3.{self.cfg.region}.amazonaws.com"
        sp = urllib.parse.urlsplit(ep)
        self._scheme, self._host = sp.scheme or "https", sp.netloc
        if self.cfg.addressing == "virtual":
            self._host = f"{self.cfg.bucket}.{self._host}"
            self._base = ""
        else:
            self._base = "/" + _q(self.cfg.bucket)

    # refs
    def _obj(self, key: str) -> str:
        return f"{self.cfg.prefix}/{key}" if self.cfg.prefix else key

    def ref(self, key: str) -> str:
        return f"{self.url}/{key}"

    def key_of(self, ref: str) -> str | None:
        return ref[len(self.url) + 1:] if ref.startswith(self.url + "/") else None

    # signing
    def _sign(self, method: str, path: str, query: dict[str, str], headers: dict[str, str],
              payload_hash: str, now: datetime | None = None) -> dict[str, str]:
        now = now or datetime.now(timezone.utc)
        amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
        h = {k.lower(): v.strip() for k, v in headers.items()}
        h.update({"host": self._host, "x-amz-date": amz, "x-amz-content-sha256": payload_hash})
        if self.cfg.session_token:
            h["x-amz-security-token"] = self.cfg.session_token
        names = sorted(h)
        cq = "&".join(f"{_q(k)}={_q(v)}" for k, v in sorted(query.items()))
        creq = "\n".join([method, path, cq, "".join(f"{n}:{h[n]}\n" for n in names), ";".join(names), payload_hash])
        scope = f"{day}/{self.cfg.region}/s3/aws4_request"
        sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
        k = ("AWS4" + self.cfg.secret_key).encode()
        for part in (day, self.cfg.region, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        h["authorization"] = (f"AWS4-HMAC-SHA256 Credential={self.cfg.access_key}/{scope}, "
                              f"SignedHeaders={';'.join(names)}, Signature={sig}")
        del h["host"]
        return h

    def _req(self, method: str, key: str | None, query: dict[str, str] | None = None, body: bytes = b"",
             headers: dict[str, str] | None = None, ok: tuple[int, ...] = (200,)) -> tuple[int, bytes, dict[str, str]]:
        path = (self._base + "/" + _q(self._obj(key), safe="-_.~/")) if key is not None else (self._base or "/")
        q = query or {}
        ph = hashlib.sha256(body).hexdigest()
        last: Exception | None = None
        self.reload_credentials()
        reloaded = False
        for attempt in range(4):
            hdrs = self._sign(method, path, q, headers or {}, ph)
            qs = "&".join(f"{_q(k)}={_q(v)}" for k, v in sorted(q.items()))
            url = f"{self._scheme}://{self._host}{path}" + ("?" + qs if qs else "")
            req = urllib.request.Request(url, data=body if method in ("PUT", "POST") else None, method=method,
                                         headers=hdrs)
            try:
                with urllib.request.urlopen(req, timeout=self.cfg.timeout_s) as r:
                    data = r.read()
                    rh = {k.lower(): v for k, v in r.headers.items()}
                    cl = rh.get("content-length")
                    if method == "GET" and cl is not None and cl.isdigit() and int(cl) != len(data):
                        last = BlobError(f"S3 GET {key}: truncated body ({len(data)} of {cl} bytes)")   # HA-18
                    else:
                        return r.status, data, rh
            except urllib.error.HTTPError as e:
                data = e.read()
                if e.code in ok:
                    return e.code, data, {k.lower(): v for k, v in e.headers.items()}
                if e.code in (400, 403) and not reloaded and any(x in data for x in _CRED_ERRORS):
                    reloaded = True                                         # HA-07: rotated / expired keys
                    self.reload_credentials(force=True)
                    last = BlobError(f"S3 {method} {key or '/'}: HTTP {e.code} (credentials reloaded)")
                    continue
                if e.code < 500 and e.code not in (408, 429):
                    raise BlobError(f"S3 {method} {key or '/'}: HTTP {e.code} {data[:300]!r}") from None
                last = BlobError(f"S3 {method} {key or '/'}: HTTP {e.code}")
            except (OSError, TimeoutError, http.client.HTTPException) as e:      # HA-18: IncompleteRead & co
                last = e
            time.sleep(min(4.0, 0.25 * 2 ** attempt))
        raise BlobError(f"S3 {method} {key or '/'} failed after retries: {last}")

    def reload_credentials(self, force: bool = False) -> bool:
        """HA-07: pick up rotated keys (env / *_FILE). Returns True when the credentials changed."""
        ttl = float(os.environ.get("ALELT_S3_CRED_TTL_S", "300"))
        if not self._from_env or (not force and time.monotonic() - self._cred_at < ttl):
            return False
        self._cred_at = time.monotonic()
        new = S3Config.from_env(self._src_url)
        changed = (new.access_key, new.secret_key, new.session_token) != (
            self.cfg.access_key, self.cfg.secret_key, self.cfg.session_token)
        if changed:
            self.cfg = dataclasses.replace(self.cfg, access_key=new.access_key, secret_key=new.secret_key,
                                           session_token=new.session_token)
        return changed

    # operations
    def ensure_bucket(self) -> None:
        """Create the bucket if missing (demo / tests; production buckets are provisioned by the operator)."""
        st, _, _ = self._req("HEAD", None, ok=(200, 404, 403))
        if st == 404:
            self._req("PUT", None, ok=(200, 409))

    def put(self, key: str, data: bytes) -> str:
        md5 = base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode()
        st, _, hdr = self._req("PUT", key, body=data, headers={
            "Content-MD5": md5, "Content-Type": "application/octet-stream", "Content-Length": str(len(data))})
        etag = hdr.get("etag", "").strip('"')
        opaque = (hdr.get("x-amz-server-side-encryption", "").lower() in _SSE_OPAQUE_ETAG
                  or "x-amz-server-side-encryption-customer-algorithm" in hdr
                  or os.environ.get("ALELT_S3_ETAG_CHECK", "1") == "0")       # HA-06: SSE-KMS / SSE-C ETag != MD5
        if st != 200 or (not opaque and etag and "-" not in etag
                         and etag != hashlib.md5(data, usedforsecurity=False).hexdigest()):
            raise BlobError(f"S3 PUT {key}: write not confirmed (status {st}, etag {etag!r})")
        return self.ref(key)

    def get(self, key: str) -> bytes:
        st, data, _ = self._req("GET", key, ok=(200, 404))
        if st == 404:
            raise BlobError(f"object not found: {self.ref(key)}")
        return data

    def size(self, key: str) -> int | None:
        """Object size, None when absent or unknown (HEAD 403 without s3:ListBucket, HA-17)."""
        st, _, hdr = self._req("HEAD", key, ok=(200, 404, 403))
        return int(hdr.get("content-length", "0")) if st == 200 else None

    def delete(self, key: str) -> None:
        self._req("DELETE", key, ok=(200, 204, 404))

    def list(self, prefix: str = "") -> Iterator[tuple[str, int]]:
        base = (self.cfg.prefix + "/" if self.cfg.prefix else "") + prefix
        token: str | None = None
        while True:
            q = {"list-type": "2", "prefix": base, "max-keys": "1000"}
            if token:
                q["continuation-token"] = token
            _, data, _ = self._req("GET", None, query=q)
            root = ET.fromstring(data)
            ns = root.tag[: root.tag.index("}") + 1] if root.tag.startswith("{") else ""
            strip = len(self.cfg.prefix) + 1 if self.cfg.prefix else 0
            for c in root.findall(f"{ns}Contents"):
                k = c.findtext(f"{ns}Key") or ""
                if not k.endswith("/"):
                    yield k[strip:], int(c.findtext(f"{ns}Size") or 0)
            if (root.findtext(f"{ns}IsTruncated") or "false").lower() != "true":
                return
            token = root.findtext(f"{ns}NextContinuationToken")

    def probe(self) -> None:
        if not self.cfg.access_key or not self.cfg.secret_key:
            raise BlobError("S3 credentials missing (ALELT_S3_ACCESS_KEY_ID / ALELT_S3_SECRET_ACCESS_KEY)")
        k = f".probe/{os.getpid()}-{time.time_ns()}"
        self.put(k, b"probe")
        if self.get(k) != b"probe":
            raise BlobError("S3 read-back mismatch")
        self.delete(k)


# ------------------------------------------------------------------ factory
_stores: dict[str, BlobStore] = {}
_lock = threading.Lock()


def spool_url() -> str:
    """Configured spool URL (ALELT_SPOOL_URL, else file:// + ALELT_SPOOL_DIR)."""
    from .settings import get_settings

    url = os.environ.get("ALELT_SPOOL_URL", "").strip()
    return url or "file://" + Path(get_settings().spool_dir).resolve().as_posix()


def _file_root(url: str) -> Path:
    p = urllib.parse.unquote(urllib.parse.urlsplit(url).path)
    if len(p) >= 3 and p[0] == "/" and p[2] == ":":      # file:///C:/x on Windows
        p = p[1:]
    return Path(p)


def open_store(url: str) -> BlobStore:
    """Store for `url` (file:// or s3://), cached per URL."""
    with _lock:
        st = _stores.get(url)
        if st is None:
            scheme = urllib.parse.urlsplit(url).scheme
            if scheme == "file":
                st = LocalBlobStore(_file_root(url))
            elif scheme == "s3":
                st = S3BlobStore(url)
            else:
                raise BlobError(f"unsupported spool URL {url!r} (file:// or s3://)")
            _stores[url] = st
        return st


def get_store() -> BlobStore:
    return open_store(spool_url())


def reset_cache() -> None:
    with _lock:
        _stores.clear()
        _usage.clear()


def read_ref(ref: str) -> bytes:
    """Read any stored ref: s3://bucket/prefix/key, file:///path or an absolute (legacy) file path."""
    if ref.startswith("s3://"):
        st = get_store()
        key = st.key_of(ref)
        if key is None:                                   # ref of another bucket/prefix: bucket = netloc, key = path
            u = urllib.parse.urlsplit(ref)
            return open_store(f"s3://{u.netloc}").get(u.path.lstrip("/"))
        return st.get(key)
    if ref.startswith("file://"):
        ref = str(_file_root(ref))
    try:
        return Path(ref).read_bytes()
    except FileNotFoundError:
        raise BlobError(f"object not found: {ref}") from None


def delete_ref(ref: str) -> None:
    st = get_store()
    key = st.key_of(ref)
    if key is not None:
        st.delete(key)
    elif not ref.startswith("s3://"):
        Path(ref).unlink(missing_ok=True)


_usage: dict[str, tuple[float, int, int]] = {}
_usage_lock = threading.Lock()


def _usage_stats(max_age_s: float | None = None) -> tuple[int, int]:
    """(bytes, objects) of the configured store, cached per store URL (HA-16: one listing per TTL per process, capped
    at ALELT_S3_LIST_CAP objects; a concurrent caller waits for the listing instead of starting another)."""
    ttl = float(os.environ.get("ALELT_SPOOL_USAGE_TTL_S", "300")) if max_age_s is None else max_age_s
    cap = int(os.environ.get("ALELT_S3_LIST_CAP", "2000000"))
    st = get_store()
    with _usage_lock:
        hit = _usage.get(st.url)
        if hit is None or time.monotonic() - hit[0] > ttl:
            try:
                total = n = 0
                for _, size in st.list():
                    total += size
                    n += 1
                    if n >= cap:
                        break
                hit = (time.monotonic(), total, n)
            except (BlobError, OSError):
                hit = (time.monotonic(), hit[1] if hit else 0, hit[2] if hit else 0)
            _usage[st.url] = hit
    return hit[1], hit[2]


def used_bytes(max_age_s: float | None = None) -> int:
    """Total bytes in the configured store (cached; listing a bucket is not free)."""
    return _usage_stats(max_age_s)[0]


def object_count(max_age_s: float | None = None) -> int:
    """Objects in the configured store (same cached listing as used_bytes)."""
    return _usage_stats(max_age_s)[1]


def ready() -> bool:
    st = get_store()
    if isinstance(st, LocalBlobStore):
        st.root.mkdir(parents=True, exist_ok=True)
        return st.root.is_dir()
    try:
        next(iter(st.list(".probe/")), None)
        return True
    except (BlobError, OSError):
        return False


def describe(url: str | None = None) -> dict[str, object]:
    st = open_store(url) if url else get_store()
    d: dict[str, object] = {"kind": st.kind, "url": st.url, "ha": st.ha}
    if isinstance(st, S3BlobStore):
        d.update(endpoint=st.cfg.endpoint or "(provider default)", region=st.cfg.region,
                 addressing=st.cfg.addressing)
    return d


# ------------------------------------------------------------------ copy (topology switch, spool-copy CLI)
@dataclass
class CopyReport:
    objects: int = 0
    copied: int = 0
    skipped: int = 0
    bytes: int = 0
    mismatched: list[str] | None = None

    def ok(self) -> bool:
        return not self.mismatched


def copy_store(src: BlobStore, dst: BlobStore, verify: bool = True, verify_only: bool = False,
               progress: object = None) -> CopyReport:
    """Copy every object src -> dst (same keys). Idempotent: objects already present with the same size and sha256
    are skipped. With verify, every copied object is read back from dst and compared by sha256."""
    rep = CopyReport(mismatched=[])
    for key, size in src.list():
        if key.startswith(".probe/"):
            continue
        rep.objects += 1
        data = src.get(key)
        digest = hashlib.sha256(data).hexdigest()
        have = dst.size(key)
        if have == size and hashlib.sha256(dst.get(key)).hexdigest() == digest:
            rep.skipped += 1
            continue
        if verify_only:
            rep.mismatched.append(key)                      # type: ignore[union-attr]
            continue
        dst.put(key, data)
        rep.copied += 1
        rep.bytes += size
        if verify and hashlib.sha256(dst.get(key)).hexdigest() != digest:
            rep.mismatched.append(key)                      # type: ignore[union-attr]
        if callable(progress) and rep.objects % 500 == 0:
            progress(rep)
    return rep

"""Destination URLs (ALELT_DEST_URL). The pipeline passes one URL string; each plugin documents its form, e.g.

  clickhouse://user:pw@host:8123/database?secure=0
  kafka://host:9092,host2:9092?topic_prefix=alelt&security_protocol=SASL_SSL
  iceberg+rest://catalog-host:8181/?warehouse=s3://bucket/wh&s3.endpoint=http://minio:9000
  delta+s3://bucket/prefix?endpoint=http://minio:9000   |  delta+file:///C:/lake
Secrets may be given as `${ENV_NAME}` (resolved here) so they never sit in config files (R9).
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, unquote, urlsplit

_ENV = re.compile(r"\$\{([A-Z0-9_]+)\}")


class UnresolvedSecretError(ValueError):
    """A `${ENV}` placeholder of the destination URL names an unset environment variable (DST-kit-08)."""


def _env(v: str) -> str:
    def one(m: re.Match[str]) -> str:
        name = m.group(1)
        val = os.environ.get(name)
        if val is None and os.environ.get(f"{name}_FILE"):          # secret file convention (deploy/)
            with open(os.environ[f"{name}_FILE"], encoding="utf-8") as f:
                val = f.read().strip()
        if val is None:
            raise UnresolvedSecretError(f"destination URL: environment variable {name} is not set (${{{name}}})")
        return val
    return _ENV.sub(one, v)


@dataclass
class DestUrl:
    scheme: str
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    hosts: str = ""                     # raw netloc host list (kafka bootstrap)
    path: str = ""
    params: dict[str, str] = field(default_factory=dict)

    def get(self, name: str, default: str | None = None) -> str | None:
        return self.params.get(name, default)

    def flag(self, name: str, default: bool = False) -> bool:
        v = self.params.get(name)
        return default if v is None else v.lower() in ("1", "true", "yes", "on")


def parse_url(url: str) -> DestUrl:
    u = urlsplit(url)
    netloc = u.netloc.rsplit("@", 1)[-1]
    first = netloc.split(",")[0]
    try:
        port = u.port if "," not in netloc else (int(first.rsplit(":", 1)[1]) if ":" in first else None)
    except ValueError:
        port = None
    return DestUrl(
        scheme=u.scheme.lower(),
        user=_env(unquote(u.username)) if u.username else None,
        password=_env(unquote(u.password)) if u.password else None,
        host=u.hostname if "," not in netloc else first.rsplit(":", 1)[0],
        port=port, hosts=netloc, path=unquote(u.path),
        params={k: _env(v) for k, v in parse_qsl(u.query, keep_blank_values=True)})

"""Read-only query seam for importers. Only SELECT/SHOW/DESCRIBE/GET_DDL statements are allowed."""
from __future__ import annotations

import os
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

_READ = re.compile(r"^\s*(SELECT|SHOW|DESCRIBE|DESC|WITH)\b", re.I)


class Query(Protocol):
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]: ...


class ReadOnlyViolation(RuntimeError):
    pass


class ReadOnly:
    """Wraps any executor and refuses non-read statements (defence in depth on top of a read-only role)."""

    def __init__(self, inner: Query) -> None:
        self.inner = inner

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        if not _READ.match(sql) or ";" in sql.strip().rstrip(";"):
            raise ReadOnlyViolation(f"importer may only read: {sql[:60]!r}")
        return self.inner.execute(sql, params)


def snowflake_from_env(prefix: str = "ALELT_IMPORT_SNOWFLAKE_") -> Query:
    """Key-pair connection from environment (R9): ACCOUNT, USER, ROLE, WAREHOUSE, PRIVATE_KEY_PATH | PRIVATE_KEY."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    env = os.environ
    pem = Path(env[prefix + "PRIVATE_KEY_PATH"]).read_bytes() if env.get(prefix + "PRIVATE_KEY_PATH") \
        else env[prefix + "PRIVATE_KEY"].encode()
    pw = env.get(prefix + "PRIVATE_KEY_PASSPHRASE")
    key = serialization.load_pem_private_key(pem, password=pw.encode() if pw else None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    conn = snowflake.connector.connect(account=env[prefix + "ACCOUNT"], user=env[prefix + "USER"],
                                       role=env.get(prefix + "ROLE"), warehouse=env.get(prefix + "WAREHOUSE"),
                                       private_key=der, application="ALELT-import")

    class _Q:
        def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                return [tuple(r) for r in cur.fetchall()]
            finally:
                cur.close()

    return ReadOnly(_Q())

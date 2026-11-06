"""SQL execution seam. `Executor` is the only I/O boundary; tests use a recording fake.

Real connections use key-pair auth; secrets come only from the environment / secret store mount (R9).
"""
from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class Executor(Protocol):
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    private_key_path: str | None = None
    private_key_pem_env: str = "ALELT_SNOWFLAKE_PRIVATE_KEY"
    passphrase_env: str = "ALELT_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
    env: Mapping[str, str] | None = field(default=None, repr=False, compare=False)   # None = os.environ

    def getenv(self, k: str) -> str | None:
        return (os.environ if self.env is None else self.env).get(k)

    @classmethod
    def from_env(cls, prefix: str = "ALELT_SNOWFLAKE_", env: Mapping[str, str] | None = None) -> SnowflakeSettings:
        """Non-secret identifiers from `env` (default os.environ); the key is read from a file path or the same
        mapping at connect time."""
        src = os.environ if env is None else env

        def need(k: str) -> str:
            v = src.get(prefix + k)
            if not v:
                raise RuntimeError(f"missing environment variable {prefix}{k}")
            return v

        return cls(account=need("ACCOUNT"), user=need("USER"), role=src.get(prefix + "ROLE", "ALELT_LOADER"),
                   warehouse=src.get(prefix + "WAREHOUSE", "ALELT_WH"),
                   database=need("DATABASE"), private_key_path=src.get(prefix + "PRIVATE_KEY_PATH"),
                   private_key_pem_env=prefix + "PRIVATE_KEY", passphrase_env=prefix + "PRIVATE_KEY_PASSPHRASE",
                   env=None if env is None else dict(env))


def _private_key_der(s: SnowflakeSettings) -> bytes:
    from cryptography.hazmat.primitives import serialization

    if s.private_key_path:
        pem = Path(s.private_key_path).read_bytes()
    else:
        raw = s.getenv(s.private_key_pem_env)
        if not raw:
            raise RuntimeError("no Snowflake private key: set *_PRIVATE_KEY_PATH or *_PRIVATE_KEY")
        pem = raw.encode()
    pw = s.getenv(s.passphrase_env)
    key = serialization.load_pem_private_key(pem, password=pw.encode() if pw else None)
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


class SnowflakeExecutor:
    """snowflake-connector-python wrapper (lazy import), autocommit on; transactions are explicit BEGIN/COMMIT."""

    def __init__(self, settings: SnowflakeSettings) -> None:
        import snowflake.connector

        self._conn = snowflake.connector.connect(
            account=settings.account, user=settings.user, role=settings.role, warehouse=settings.warehouse,
            database=settings.database, private_key=_private_key_der(settings), autocommit=True,
            application="ALELT", session_parameters={"QUERY_TAG": "alelt"})

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            return [tuple(r) for r in cur.fetchall()]  # Snowflake returns a status row for DDL/DML
        finally:
            cur.close()

    def close(self) -> None:
        self._conn.close()

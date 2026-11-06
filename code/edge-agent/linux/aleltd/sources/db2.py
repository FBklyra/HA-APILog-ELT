"""Db2 LUW source: raw db2ReadLog blocks (filter ON), shipped verbatim - decoding happens on the hub (ADR-002).
Same capture code as the dev edge (alelt_db2edge). Read-only (R1). Needs the IBM Data Server client on the host;
`source.dll_dirs` = client library directories (Linux: <sqllib>/lib64, Windows: <SQLLIB>\\BIN)."""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, Optional

from ..config import Config
from .base import CaptureSource, SourceError, Step


def _lib():                                   # alelt_db2edge lives next to the dev edge
    try:
        import alelt_db2edge
    except ImportError:
        here = os.path.dirname(os.path.abspath(__file__))
        for up in (4, 5):
            cand = os.path.join(here, *[".."] * up, "connectors", "sources", "db2", "devedge")
            if os.path.isdir(os.path.join(cand, "alelt_db2edge")):
                sys.path.insert(0, os.path.normpath(cand))
                break
        import alelt_db2edge
    return alelt_db2edge


def _default_lib_dir() -> str:
    if sys.platform == "win32":
        return r"C:\Program Files\IBM\SQLLIB\BIN"
    home = os.environ.get("DB2_HOME") or os.path.expanduser("~db2inst1/sqllib")
    return os.path.join(home, "lib64")


class Db2Capture(CaptureSource):
    engine = "db2"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.lib = _lib()
        self.db: Any = None
        self.cursor: Optional[str] = None
        s = cfg.source.settings
        self.schemas = list(s.get("catalog_schemas") or s.get("schemas") or [])
        if not self.schemas:
            raise SourceError("source.settings.catalog_schemas: at least one schema")

    def targets(self) -> Dict[str, Any]:
        return {**self.cfg.source.settings, "database": self.cfg.source.database}

    def compare(self, a: str, b: str) -> int:
        ka, kb = self.lib.lri_key(a), self.lib.lri_key(b)
        return (ka > kb) - (ka < kb)

    def validate(self, pos: str) -> str:
        p1, p2 = self.lib.lri_key(pos)
        return f"{p1}:{p2:016X}"

    def _conn(self) -> Any:
        if self.db is None:
            src = self.cfg.source
            user = os.environ.get(src.user_env) if src.user_env else None
            lib_dir = next((d for d in src.dll_dirs if d and os.path.isdir(d)), "") or _default_lib_dir()
            try:
                db = self.lib.Db2(src.database, lib_dir, user, src.password())
            except self.lib.Db2Error as e:
                raise SourceError(str(e)) from None
            if self.on_connect is not None:      # deny-list re-check on the server really reached (LNX-08)
                try:
                    self.on_connect(db.server_info())
                except BaseException:
                    try:
                        db.close()
                    except Exception:  # noqa: BLE001
                        pass
                    raise
            self.db = db
        return self.db

    def catalog(self, valid_from: Optional[str]) -> Dict[str, Any]:
        try:
            return dict(self.lib.read_catalog(self._conn(), self.schemas, self.cfg.connection_id))
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None

    def initial_position(self, start: str) -> Optional[str]:
        db = self._conn()
        try:
            db.commit()
            initial, current = db.query_lri()
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None
        if start == "current":
            return str(current)
        if start == "oldest":
            return str(initial)
        return self.validate(start)

    def reset(self, chain: Optional[str]) -> None:
        self.cursor = chain

    def step(self, chain: Optional[str], limits: Dict[str, int]) -> Step:
        if chain is None:
            raise SourceError("db2 capture needs a start position")
        if self.cursor is None:
            self.cursor = chain
        buf = max(16, int(limits.get("batch_kb", 1024))) * 1024
        try:
            raw, recs, nxt, code = self._conn().read(self.cursor, buf)
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None
        st = Step()
        if recs:
            hdr, body, _ = self.lib.make_batch(raw, recs, nxt, chain, self.cfg.connection_id, self.cfg.agent_id)
            st.batches.append((hdr, body))
        self.cursor = nxt
        if recs == 0 or code == self.lib.RL_READ_TO_CURRENT:
            st.wait_s = max(0.05, limits.get("poll_ms", 1000) / 1000)
        return st

    def probe(self, name: str) -> Dict[str, Any]:
        try:
            return dict(self._conn().probe(name))
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None

    def verify_table(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Server-side SELECT ... WITH CS (the C plugin's statements, alelt_db2edge.verify)."""
        db = self._conn()

        def pos() -> Optional[str]:
            db.commit()
            return str(db.query_lri()[1])
        try:
            return dict(self.lib.verify_table(db, args, pos))
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None

    def snapshot_start(self) -> str:
        db = self._conn()
        try:
            db.commit()
            return str(db.query_lri()[1])
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None

    def snapshot_chunks(self, schema: str, table: str, chunk_rows: int, key_from: Any = None) -> Any:
        """Keyset-paged SELECT ... WITH CS (alelt_db2edge.snapshot_chunks = the C plugin's statements)."""
        try:
            yield from self.lib.snapshot_chunks(self._conn(), schema, table, chunk_rows, key_from=key_from)
        except self.lib.Db2Error as e:
            self.close()
            raise SourceError(str(e)) from None

    def close(self) -> None:
        db, self.db = self.db, None
        self.cursor = None
        if db is not None:
            try:
                db.close()
            except Exception:  # noqa: BLE001
                pass

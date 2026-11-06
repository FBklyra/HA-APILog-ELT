"""Chosen handling of known source-data issues (REQ-16, X-62), applied by the pipeline before transaction assembly.

`hub_setting` `diag.source_policy` = {"<S.T>.<COL|*>": {"<ISSUE>": {"option": QUARANTINE_SKIP|COERCE|ACCEPT_AS_IS, ...}}}
(recorded by the incident action SOURCE_DATA_POLICY, audited there). Issues are detected on row images with the same
checks as the diagnosis (`alelt_diagnose.source_data.check_events`):

- QUARANTINE_SKIP: the change is not applied; a quarantine record keeps table, position, reason and raw reference (R7).
- COERCE: the offending value is replaced - NULL for INVALID_DATE / FUTURE_TIMESTAMP / OUT_OF_RANGE numbers, the type's
  neutral value for NULL_IN_NOT_NULL, truncation to the declared length for OUT_OF_RANGE strings.
- ACCEPT_AS_IS: applied unchanged (the default without a policy).
Tables without a policy entry are not inspected (no cost on the data path).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.models import CanonicalType, ChangeEvent, ColumnInfo, QuarantineRecord, TableInfo, TableRef

KEY = "diag.source_policy"
OPTIONS = ("QUARANTINE_SKIP", "COERCE", "ACCEPT_AS_IS")
_NEUTRAL: dict[CanonicalType, Any] = {
    CanonicalType.STRING: "", CanonicalType.SHORT: 0, CanonicalType.INT: 0, CanonicalType.LONG: 0,
    CanonicalType.DECIMAL: Decimal(0), CanonicalType.FLOAT: 0.0, CanonicalType.DOUBLE: 0.0,
    CanonicalType.BOOLEAN: False, CanonicalType.BINARY: b"", CanonicalType.LOCALDATE: date(1970, 1, 1),
    CanonicalType.LOCALDATETIME: datetime(1970, 1, 1), CanonicalType.LOCALTIME: dtime(0, 0),
}


@dataclass
class Outcome:
    event: ChangeEvent | None                      # None = skipped (quarantined)
    quarantine: QuarantineRecord | None = None
    coerced: list[str] | None = None


class SourcePolicy:
    def __init__(self, doc: dict[str, Any] | None) -> None:
        self.doc = doc if isinstance(doc, dict) else {}
        import hashlib

        # revision of the policy document (XC-30): recorded per batch at its first decode, replays reuse it
        self.rev = hashlib.sha256(json.dumps(self.doc, sort_keys=True, default=str).encode()).hexdigest()[:16]             if self.doc else ""
        self.rules: dict[str, dict[str, dict[str, str]]] = {}     # fqn -> column|'*' -> issue -> option
        for k, issues in (doc or {}).items():
            fqn, _, col = str(k).rpartition(".")
            if not fqn or not isinstance(issues, dict):
                continue
            for issue, choice in issues.items():
                opt = (choice or {}).get("option") if isinstance(choice, dict) else choice
                if opt in OPTIONS:
                    self.rules.setdefault(fqn, {}).setdefault(col or "*", {})[str(issue)] = str(opt)

    def __bool__(self) -> bool:
        return bool(self.rules)

    def option(self, fqn: str, column: str, issue: str) -> str | None:
        t = self.rules.get(fqn) or {}
        return (t.get(column) or {}).get(issue) or (t.get("*") or {}).get(issue)

    def apply(self, ev: ChangeEvent, info: TableInfo | None) -> Outcome:
        if ev.table is None or info is None or not ev.after or ev.table.fqn not in self.rules:
            return Outcome(ev)
        from alelt_diagnose.source_data import check_events

        cols = {c.name: c for c in info.columns}
        after = dict(ev.after)
        coerced: list[str] = []
        for f in check_events(info, [ev], limit=1):
            opt = self.option(ev.table.fqn, f["column"], f["issue"])
            if opt == "QUARANTINE_SKIP":
                raw = json.dumps({"key": {k: str(v) for k, v in (f.get("key") or {}).items()}, "column": f["column"]},
                                 ensure_ascii=False).encode()
                return Outcome(None, QuarantineRecord(
                    position=ev.position, table=ev.table, raw_ref=ev.raw_ref, raw_hex=raw[:256].hex(),
                    reason=f"source policy QUARANTINE_SKIP: {f['issue']} in {f['column']}"))
            if opt == "COERCE":
                after[f["column"]] = _coerce(f["issue"], after.get(f["column"]), cols.get(f["column"]))
                coerced.append(f"{f['column']}:{f['issue']}")
        if not coerced:
            return Outcome(ev)
        return Outcome(ev.model_copy(update={"after": after}), coerced=coerced)


def _coerce(issue: str, v: Any, col: ColumnInfo | None) -> Any:
    if issue == "NULL_IN_NOT_NULL":
        return _NEUTRAL.get(col.canonical_type) if col is not None else None
    if issue == "OUT_OF_RANGE" and isinstance(v, str) and col is not None and col.length:
        return v[: col.length]
    return None


class PolicyCache:
    """`diag.source_policy` re-read at most every `ttl_s` seconds."""

    def __init__(self, engine: Engine, ttl_s: float = 10.0) -> None:
        self.engine, self.ttl = engine, ttl_s
        self._at = 0.0
        self._pol = SourcePolicy(None)
        self._revs: dict[str, SourcePolicy] = {}

    def get(self) -> SourcePolicy:
        if time.monotonic() - self._at > self.ttl:
            from .db import hub_setting

            with self.engine.connect() as c:
                doc = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == KEY)).scalar()
            self._pol, self._at = SourcePolicy(doc if isinstance(doc, dict) else None), time.monotonic()
        return self._pol

    def remember(self, pol: SourcePolicy) -> None:
        """Keep the document of revision `pol.rev` (hub_setting `diag.source_policy@<rev>`) for later replays."""
        if not pol.rev or pol.rev in self._revs:
            return
        from .db import hub_setting
        from .dialect import insert

        with self.engine.begin() as c:
            c.execute(insert(hub_setting).values(key=f"{KEY}@{pol.rev}", value=pol.doc)
                      .on_conflict_do_nothing(index_elements=["key"]))
        self._revs[pol.rev] = pol

    def revision(self, rev: str | None) -> SourcePolicy:
        """Policy a batch was first decoded with (XC-30). None = batch decoded before revisions were recorded:
        the current policy; "" = no policy then."""
        if rev is None:
            return self.get()
        if rev == "":
            return SourcePolicy(None)
        if rev not in self._revs:
            from .db import hub_setting

            with self.engine.connect() as c:
                doc = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == f"{KEY}@{rev}")).scalar()
            self._revs[rev] = SourcePolicy(doc if isinstance(doc, dict) else None) if doc is not None else self.get()
        return self._revs[rev]


__all__ = ["KEY", "Outcome", "PolicyCache", "SourcePolicy", "TableRef"]

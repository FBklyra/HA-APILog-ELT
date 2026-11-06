"""Config as code round trip (F-18): `/api/v1/config/...` and `alelt config export|import|diff|status`.

The metadata DB holds the active revision; these are the only ways a YAML document gets in or out.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from . import config_store as cs
from . import store
from .auth import Principal, require_perm
from .console import _pol_cache, diff, err, need_reason
from .db import get_engine
from .settings import get_settings

router = APIRouter(prefix="/api/v1/config", tags=["config"])


class ImportDoc(BaseModel):
    reason: str | None = None
    yaml: str = Field(min_length=1, max_length=8 << 20)
    dry_run: bool = False


@router.get("/connections")
def list_active(_: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        out = []
        for cid in cs.known_connections(c):
            a = cs.active(c, cid)
            out.append({"connection_id": cid, "revision": a[0] if a else None, "sha256": cs.digest(a[1]) if a else None})
        return out


@router.get("/connections/{cid}/export", response_class=PlainTextResponse)
def export(cid: str, revision: int | None = None,
           _: Principal = Depends(require_perm("table.configure"))) -> PlainTextResponse:
    with get_engine().connect() as c:
        if revision is None:
            a = cs.active(c, cid)
            rid, doc = (a[0], a[1]) if a else (None, None)
        else:
            rid, doc = revision, cs.doc_at(c, cid, revision)
    if doc is None:
        raise err(404, "NOT_FOUND", f"no config revision for {cid}")
    return PlainTextResponse(cs.export_yaml(doc, rid), media_type="text/yaml")


@router.post("/connections/{cid}/import")
def import_doc(cid: str, body: ImportDoc, p: Principal = Depends(require_perm("import.run"))) -> dict[str, Any]:
    """YAML document -> new revision (or a diff with dry_run). Unsupported modes are refused (X-127)."""
    import yaml

    try:
        doc = yaml.safe_load(body.yaml)
        want = cs.validate_doc(cid, doc)
    except yaml.YAMLError as e:
        raise err(422, "INVALID_YAML", type(e).__name__) from None
    except cs.ConfigError as e:
        raise err(409, "INVALID_CONFIG", str(e)) from None
    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        cur = cs.active_doc(c, cid) or {}
        changes = diff(cur.get("schemas") or {}, want.get("schemas") or {})
        if body.dry_run:
            return {"connection_id": cid, "revision": None, "changes": changes, "dry_run": True}
        rid = cs.commit(c, cid, want, p.subject, reason, "yaml-import")
        if rid is not None:
            store.audit(c, p.subject, "config.import", cid, {"revision": rid, "reason": reason,
                                                             "changes": len(changes)}, p.primary_role)
    _pol_cache.pop(cid, None)
    return {"connection_id": cid, "revision": rid, "changes": changes, "unchanged": rid is None}


@router.get("/drift")
def file_drift(_: Principal = Depends(require_perm("table.configure"))) -> list[dict[str, Any]]:
    """ALELT_TABLES_CONFIG documents that differ from the active revision (they are NOT applied)."""
    with get_engine().connect() as c:
        return cs.drift(c, get_settings().tables_config)


# ------------------------------------------------------------------ CLI
def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(prog="alelt config", description="config revisions <-> YAML (F-18)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="write the active (or --revision) document as YAML")
    e.add_argument("connection_id")
    e.add_argument("--revision", type=int)
    e.add_argument("--out", help="file (default stdout)")
    i = sub.add_parser("import", help="YAML document -> new config revision")
    i.add_argument("file")
    i.add_argument("--connection", help="default: connection_id in the file")
    i.add_argument("--reason", required=True)
    i.add_argument("--author", default="cli")
    i.add_argument("--dry-run", action="store_true")
    sub.add_parser("diff", help="ALELT_TABLES_CONFIG files vs active revisions")
    sub.add_parser("status", help="active revision per connection")
    a = ap.parse_args(argv)
    from .db import init_db

    eng = get_engine()
    init_db(eng)
    if a.cmd == "export":
        with eng.connect() as c:
            doc = cs.doc_at(c, a.connection_id, a.revision) if a.revision else cs.active_doc(c, a.connection_id)
            rid = a.revision or (cs.active(c, a.connection_id) or (None,))[0]
        if doc is None:
            print(f"no config revision for {a.connection_id}", file=sys.stderr)
            return 1
        text = cs.export_yaml(doc, rid)
        if a.out:
            Path(a.out).write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text)
        return 0
    if a.cmd == "import":
        import yaml

        doc = yaml.safe_load(Path(a.file).read_text(encoding="utf-8")) or {}
        cid = a.connection or doc.get("connection_id")
        if not cid:
            print("no connection_id (pass --connection)", file=sys.stderr)
            return 2
        try:
            want = cs.validate_doc(cid, doc)
        except cs.ConfigError as ex:
            print(f"refused: {ex}", file=sys.stderr)
            return 2
        with eng.begin() as c:
            changes = diff((cs.active_doc(c, cid) or {}).get("schemas") or {}, want.get("schemas") or {})
            rid = None if a.dry_run else cs.commit(c, cid, want, a.author, a.reason, "yaml-import")
            if rid is not None:
                store.audit(c, a.author, "config.import", cid, {"revision": rid, "reason": a.reason, "file": a.file})
        print(json.dumps({"connection_id": cid, "revision": rid, "changes": changes}, indent=2, default=str))
        return 0
    with eng.connect() as c:
        if a.cmd == "diff":
            d = cs.drift(c, get_settings().tables_config)
            print(json.dumps(d, indent=2, default=str))
            return 1 if d else 0
        print(json.dumps([{"connection_id": x, "revision": (cs.active(c, x) or (None,))[0]}
                          for x in cs.known_connections(c)], indent=2))
    return 0


# ------------------------------------------------------------------ archive-log retention (X-129)
class RetentionUpdate(BaseModel):
    reason: str | None = None
    hours: float = Field(ge=0, le=24 * 3650)        # 0 = unknown (alert ArchiveRetentionUnknown)


_RET = "archive_retention_h:"


def retention_hours(c: Any, connection_id: str) -> float:
    """DBA-owned archive-log retention of a connection: console value, else ALELT_ARCHIVE_RETENTION_HOURS."""
    from .setup import _get

    v = _get(c, _RET + connection_id)
    return float(v["hours"]) if v else get_settings().retention_hours(connection_id)


@router.get("/archive-retention")
def get_retention(_: Principal = Depends(require_perm("hub.view"))) -> list[dict[str, Any]]:
    with get_engine().connect() as c:
        return [{"connection_id": cid, "hours": retention_hours(c, cid)} for cid in cs.known_connections(c)]


@router.put("/archive-retention/{cid}")
def put_retention(cid: str, body: RetentionUpdate, p: Principal = Depends(require_perm("retention.edit"))) -> dict[str, Any]:
    """Record the archive retention the DBA guarantees (the hub never deletes archive logs, R3)."""
    from .setup import _put

    reason = need_reason(body.reason)
    with get_engine().begin() as c:
        _put(c, _RET + cid, {"hours": body.hours, "by": p.subject})
        store.audit(c, p.subject, "archive_retention.update", cid, {"hours": body.hours, "reason": reason},
                    p.primary_role)
    return {"connection_id": cid, "hours": body.hours}

"""Self code-fix proposal for PLATFORM_DEFECT incidents (Stage B only; never applied by the hub).

Flow: PROPOSED (LLM patch, validated) -> REVIEWED | REJECTED (human, console) -> EXPORTED (`alelt diagnose
export-patch <incident>` writes the .patch + regression case for the dev team's normal PR process). Reviewing a fix
proposes "re-decode affected range from raw archive" (to approve after the fixed release).

Validation: the patch may touch only ALELT_DIAG_PATCH_ALLOWED path prefixes, must apply cleanly (`git apply --check`
against ALELT_DIAG_SOURCE_DIR, default the repo / image source), and should add or change a test.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError
from sqlalchemy import Engine, insert, select, update

from alelt_common.util import utcnow
from alelt_hub import store
from alelt_hub.db import quarantine_event

from . import llm
from .models import CodeFixAnswer, Finding
from .redact import scrub
from .tables import action, code_fix, incident, incident_event

DEFAULT_ALLOWED = ("connectors/sources/", "connectors/destinations/", "decoder/", "hub/alelt_", "tests/golden/",
                   "tests/ladder/")
DENIED = re.compile(r"(^|/)(\.git|\.github|deploy|secrets?|\.env)(/|$)|\.(pem|key|pfx|p12)$", re.I)
MODULES_BY_RULE: dict[str, list[str]] = {
    "decoder_defect": ["connectors/sources/db2/alelt_src_db2/rowimage.py", "connectors/sources/db2/alelt_src_db2/logdecode.py"],
    "codepage_mismatch": ["connectors/sources/db2/alelt_src_db2/rowimage.py"],
    "unsupported_type": ["connectors/sources/db2/alelt_src_db2/rowimage.py", "connectors/sources/db2/alelt_src_db2/connector.py"],
    "value_compression": ["connectors/sources/db2/alelt_src_db2/logdecode.py"],
    "out_of_row_lob": ["connectors/sources/db2/alelt_src_db2/rowimage.py"],
    "catalog_drift": ["hub/alelt_hub/pipeline.py", "connectors/sources/db2/alelt_src_db2/logdecode.py"],
}
MAX_FILE_CHARS = 30_000

CODE_SYSTEM = """You are a senior engineer on ALELT (log-based CDC replication). An incident was classified as a \
ALELT defect. Propose the smallest correct fix as a unified diff against the given files (repo-relative paths, \
a/ and b/ prefixes, valid hunks with context), plus a regression test (golden corpus case or unit test) in the same \
diff. Never touch deployment, secrets or CI files. Never write to the source database.
Answer with ONE JSON object only: {"summary": str, "explanation": str, "files": [str], "patch": str, \
"test_path": str|null, "confidence": number 0..1}"""


class CodeFixError(RuntimeError):
    pass


def source_root() -> Path:
    return Path(os.environ.get("ALELT_DIAG_SOURCE_DIR") or Path(__file__).resolve().parents[2])


def allowed_prefixes() -> tuple[str, ...]:
    raw = os.environ.get("ALELT_DIAG_PATCH_ALLOWED")
    return tuple(x.strip() for x in raw.split(",") if x.strip()) if raw else DEFAULT_ALLOWED


def patch_paths(patch: str) -> list[str]:
    out: list[str] = []
    for line in patch.splitlines():
        m = re.match(r"^(?:\+\+\+|---) (?:[ab]/)?(\S+)", line) or re.match(r"^diff --git a/(\S+) b/\S+", line)
        if m and m.group(1) != "/dev/null" and m.group(1) not in out:
            out.append(m.group(1))
    return out


def validate(patch: str, root: Path | None = None) -> dict[str, Any]:
    """{paths, paths_ok, disallowed, has_test, apply_check: ok|failed|unverified, message}."""
    root = root or source_root()
    paths = patch_paths(patch)
    pre = allowed_prefixes()
    bad = [p for p in paths if ".." in Path(p).parts or p.startswith("/") or DENIED.search(p)
           or not p.startswith(pre)]
    has_test = any("/tests/" in f"/{p}" or Path(p).name.startswith("test_") or p.startswith("tests/") for p in paths)
    res: dict[str, Any] = {"paths": paths, "paths_ok": bool(paths) and not bad, "disallowed": bad,
                           "has_test": has_test, "apply_check": "unverified", "message": ""}
    if not paths:
        res["message"] = "no file headers in patch"
        return res
    git = shutil.which("git")
    if git is None or not root.is_dir():
        res["message"] = "git or source tree not available on this host"
        return res
    try:
        env = {**os.environ, "GIT_CEILING_DIRECTORIES": str(root.resolve().parent)}   # never a parent repo
        r = subprocess.run([git, "apply", "--check", "--verbose", "-"], cwd=root, input=patch.encode(),
                           capture_output=True, timeout=30, env=env)
    except (OSError, subprocess.TimeoutExpired) as e:
        res["message"] = f"git apply failed to run: {type(e).__name__}"
        return res
    res["apply_check"] = "ok" if r.returncode == 0 else "failed"
    res["message"] = scrub((r.stderr or r.stdout).decode(errors="replace"))[-1500:]
    return res


def _files(rule_id: str | None, extra: list[str] | None = None) -> dict[str, str]:
    root = source_root()
    out = {}
    for rel in (MODULES_BY_RULE.get(rule_id or "", []) + (extra or []))[:4]:
        f = root / rel
        if f.is_file() and not DENIED.search(rel):
            out[rel] = f.read_text(encoding="utf-8", errors="replace")[:MAX_FILE_CHARS]
    return out


def regression_case(engine: Engine, row: dict[str, Any]) -> dict[str, Any]:
    """Golden-corpus case skeleton from the quarantined records (raw refs + layout; values filled by the developer
    from a SELECT on a dev copy - production values never go into the repo)."""
    samples = []
    if row.get("batch_id"):
        with engine.connect() as c:
            samples = [{"position": lri, "reason": scrub(reason), "raw_ref": ref}
                       for lri, reason, ref in c.execute(select(quarantine_event.c.lri, quarantine_event.c.reason,
                                                                quarantine_event.c.raw_ref)
                                                         .where(quarantine_event.c.batch_id == uuid.UUID(row["batch_id"]))
                                                         .limit(10))]
    case = {"incident": row["id"], "table": row.get("table_fqn"), "rule": row.get("rule_id"),
            "records": samples, "raw_hex": None, "expected": None,
            "how_to": "alelt diagnose export-patch <incident> --with-raw writes raw bytes from the hub raw "
                      "archive; fill `expected` from a SELECT on the dev copy."}
    return {"path": f"tests/golden/db2/incident-{row['id'][:8]}/case.json", "content": case}


def propose(engine: Engine, iid: str, actor: str, role: str | None, reason: str,
            client: httpx.Client | None = None) -> dict[str, Any]:
    """Ask the configured LLM for a patch. Requires classification PLATFORM_DEFECT."""
    from .incidents import evidence_for
    from .rules import evaluate

    with engine.connect() as c:
        row = c.execute(select(incident).where(incident.c.id == iid)).mappings().first()
        if row is None:
            raise LookupError(iid)
        row = dict(row)
        cfg = llm.load_config(c)
    if row.get("classification") != "PLATFORM_DEFECT":
        raise CodeFixError(f"code fix only for PLATFORM_DEFECT incidents (this one is {row.get('classification')})")
    if not cfg.configured:
        raise CodeFixError("AI diagnosis not configured")
    pack = evidence_for(engine, iid)
    findings: list[Finding] = evaluate(pack)
    files = _files(row.get("rule_id"))
    if not files:
        raise CodeFixError(f"no source files available for rule {row.get('rule_id')} under {source_root()}")
    user = json.dumps({"incident": {"rule_id": row["rule_id"], "root_cause": row["root_cause"]},
                       "evidence": json.loads(pack.model_dump_json()),
                       "rule_findings": [f.model_dump(mode="json", exclude={"action", "source_data"}) for f in findings[:3]],
                       "files": files}, ensure_ascii=False, default=str)
    user = scrub(user, pack.redaction)
    with engine.begin() as c:
        if cfg.max_tokens_day and llm.tokens_used_today(c) + len(user) // 4 > cfg.max_tokens_day:
            raise CodeFixError(f"daily token budget exhausted ({cfg.max_tokens_day})")
    try:
        comp = llm.provider_for(cfg, client).complete(CODE_SYSTEM, user, max(cfg.max_output_tokens, 4000))
    except llm.LlmError as e:
        raise CodeFixError(str(e)) from None
    m = re.search(r"\{.*\}", comp.text, re.S)
    try:
        ans = CodeFixAnswer.model_validate_json(m.group(0) if m else "")
    except ValidationError as e:
        raise CodeFixError(f"model answer does not match the schema: {e.errors()[:2]}") from None
    val = validate(ans.patch)
    fid = str(uuid.uuid4())
    import hashlib

    with engine.begin() as c:
        llm.add_usage(c, (comp.input_tokens + comp.output_tokens) or len(user) // 4)
        c.execute(insert(code_fix).values(
            id=fid, incident_id=iid, status="PROPOSED", summary=ans.summary[:2000], explanation=ans.explanation,
            files=ans.files or val["paths"], patch=ans.patch, test_path=ans.test_path, validation=val,
            regression_case=regression_case(engine, row), provider=cfg.provider, model=comp.model,
            prompt_hash=hashlib.sha256((CODE_SYSTEM + user).encode()).hexdigest(), confidence=ans.confidence,
            created_at=utcnow(), created_by=actor))
        c.execute(insert(incident_event).values(incident_id=iid, at=utcnow(), kind="CODE_FIX_PROPOSED", actor=actor,
                                                detail={"code_fix_id": fid, "validation": val}))
        store.audit(c, actor, "incident.code_fix.propose", iid, {"code_fix_id": fid, "reason": reason,
                                                                 "apply_check": val["apply_check"]}, role)
    return get(engine, fid)


def get(engine: Engine, fid: str) -> dict[str, Any]:
    with engine.connect() as c:
        r = c.execute(select(code_fix).where(code_fix.c.id == fid)).mappings().first()
    if r is None:
        raise LookupError(fid)
    return {k: (v.isoformat().replace("+00:00", "Z") if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}


def list_for(engine: Engine, iid: str) -> list[dict[str, Any]]:
    with engine.connect() as c:
        ids = [r[0] for r in c.execute(select(code_fix.c.id).where(code_fix.c.incident_id == iid)
                                       .order_by(code_fix.c.created_at))]
    return [get(engine, i) for i in ids]


def review(engine: Engine, fid: str, actor: str, role: str | None, reason: str, approve: bool) -> dict[str, Any]:
    with engine.begin() as c:
        r = c.execute(select(code_fix).where(code_fix.c.id == fid)).mappings().first()
        if r is None:
            raise LookupError(fid)
        if r["status"] != "PROPOSED":
            raise CodeFixError(f"code fix is {r['status']}")
        status = "REVIEWED" if approve else "REJECTED"
        c.execute(update(code_fix).where(code_fix.c.id == fid).values(
            status=status, reviewed_by=actor, reviewed_at=utcnow(), review_reason=reason))
        c.execute(insert(incident_event).values(incident_id=r["incident_id"], at=utcnow(), kind=f"CODE_FIX_{status}",
                                                actor=actor, detail={"code_fix_id": fid, "reason": reason}))
        store.audit(c, actor, f"incident.code_fix.{'review' if approve else 'reject'}", r["incident_id"],
                    {"code_fix_id": fid, "reason": reason}, role)
        if approve:
            inc = c.execute(select(incident).where(incident.c.id == r["incident_id"])).mappings().one()
            have = c.execute(select(action.c.id).where(action.c.incident_id == inc["id"],
                                                       action.c.kind == "REDECODE_RANGE",
                                                       action.c.status == "PROPOSED")).first()
            if have is None and inc["table_fqn"] and inc["position"]:
                c.execute(insert(action).values(
                    id=str(uuid.uuid4()), incident_id=inc["id"], kind="REDECODE_RANGE", safe=True, rule_id="code_fix",
                    params={"connection_id": inc["connection_id"], "table": inc["table_fqn"],
                            "from_pos": inc["position"], "to_pos": inc["position"]},
                    description="After the fixed release is deployed: re-decode the affected range from the raw "
                                "archive (idempotent).", status="PROPOSED", created_at=utcnow()))
    return get(engine, fid)


def export(engine: Engine, iid: str, out_dir: Path, with_raw: bool = False) -> list[Path]:
    """Write <incident>.patch + regression case for every REVIEWED fix; mark EXPORTED. Returns the files written."""
    from alelt_hub.db import batch

    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for fx in list_for(engine, iid):
        if fx["status"] not in ("REVIEWED", "EXPORTED"):
            continue
        p = out_dir / f"incident-{iid[:8]}-{fx['id'][:8]}.patch"
        p.write_text(f"Subject: [alelt_diagnose] {fx['summary']}\n\n{fx['explanation']}\n\nincident: {iid}\n"
                     f"validation: {json.dumps(fx['validation'])}\n---\n{fx['patch']}", encoding="utf-8")
        written.append(p)
        case = dict(fx["regression_case"] or {})
        content = dict(case.get("content") or {})
        if with_raw and content.get("records"):
            raw = []
            with engine.connect() as c:
                for rec in content["records"]:
                    ref = rec.get("raw_ref") or {}
                    sp = c.execute(select(batch.c.spool_path, batch.c.codec).where(
                        batch.c.id == uuid.UUID(str(ref.get("batch_id"))))).first() if ref.get("batch_id") else None
                    if sp is None:
                        continue
                    from alelt_common.util import decompress

                    try:
                        body = decompress(Path(sp[0]).read_bytes(), sp[1])
                        raw.append(body[int(ref["offset"]): int(ref["offset"]) + int(ref["length"])].hex())
                    except (OSError, ValueError, KeyError):
                        continue
            content["raw_hex"] = raw
        if case.get("path"):
            cp = out_dir / Path(case["path"]).name.replace("case", f"case-{iid[:8]}")
            cp.write_text(json.dumps(content, indent=2, default=str), encoding="utf-8")
            written.append(cp)
        with engine.begin() as c:
            c.execute(update(code_fix).where(code_fix.c.id == fx["id"]).values(status="EXPORTED", exported_at=utcnow()))
            store.audit(c, "cli", "incident.code_fix.export", iid, {"code_fix_id": fx["id"], "with_raw": with_raw})
    return written

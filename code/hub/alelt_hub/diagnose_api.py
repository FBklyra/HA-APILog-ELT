"""Incidents & self-diagnosis API (/api/v1) - alelt_diagnose behind the console permission matrix.

Permissions (auth.ROLE_PERMISSIONS / console rbac.ts): view.incidents (all roles), incident.act (DataOperator,
PlatformAdmin: run AI diagnosis, approve/reject actions, resolve), llm.configure (PlatformAdmin).
Every mutation requires `reason` and writes one audit_event.
"""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Query, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from alelt_diagnose import code_fix, hooks, incidents as inc, llm
from alelt_diagnose.models import ExplainResult, LlmSettingsUpdate, LlmSettingsView
from alelt_diagnose.rules import RULES

from . import store
from .auth import Principal, can, current_principal, require_perm
from .console import err, need_reason
from .db import get_engine

router = APIRouter(prefix="/api/v1", tags=["diagnose"])


class DiagnoseRequest(BaseModel):
    reason: str | None = None
    use_llm: bool = False
    probe_destination: bool = False
    question: str | None = Field(default=None, max_length=1000)


class ActionDecision(BaseModel):
    reason: str | None = None
    decision: str = Field(pattern="^(APPROVE|REJECT)$")


class ResolveRequest(BaseModel):
    reason: str | None = None
    confirmed_root_cause: str | None = Field(default=None, max_length=4000)
    fix_steps: list[str] | None = None
    save_known_issue: bool = True


class ExplainRequest(BaseModel):
    kind: str = Field(pattern="^(incident|job|batch|quarantine|agent_event|alert)$")
    ref_id: str = Field(min_length=1, max_length=200)
    labels: dict[str, str] | None = None
    use_llm: bool = True


class LlmTestRequest(LlmSettingsUpdate):
    pass


def _nf(e: Exception) -> Any:
    return err(404, "NOT_FOUND", str(e))


# ------------------------------------------------------------------ meta
@router.get("/meta")
def meta(_: Principal = Depends(current_principal)) -> dict[str, Any]:
    with get_engine().connect() as c:
        cfg = llm.load_config(c)
    return {"llm": {"configured": cfg.configured, "enabled": cfg.enabled, "provider": cfg.provider,
                    "model": cfg.model_name},
            "diagnose": {"rules": len(RULES), "auto_actions": sorted(inc.auto_actions())}}


# ------------------------------------------------------------------ incidents
@router.get("/incidents")
def list_incidents(status: str | None = Query(None), severity: str | None = Query(None),
                   limit: int = Query(200, le=1000),
                   _: Principal = Depends(require_perm("view.incidents"))) -> list[dict[str, Any]]:
    return inc.list_incidents(get_engine(), status, severity, limit)


@router.get("/incidents/summary")
def incidents_summary(_: Principal = Depends(require_perm("view.dashboard"))) -> dict[str, Any]:
    return inc.summary(get_engine())


@router.get("/incidents/{iid}")
def get_incident(iid: str, _: Principal = Depends(require_perm("view.incidents"))) -> dict[str, Any]:
    try:
        return inc.get_incident(get_engine(), iid)
    except inc.NotFound as e:
        raise _nf(e) from None


@router.get("/incidents/{iid}/evidence")
def evidence(iid: str, p: Principal = Depends(require_perm("view.incidents"))) -> Response:
    """Redacted evidence pack (download). Read-only."""
    try:
        pack = inc.evidence_for(get_engine(), iid)
    except inc.NotFound as e:
        raise _nf(e) from None
    with get_engine().begin() as c:
        store.audit(c, p.subject, "incident.evidence.download", iid, {}, p.primary_role)
    return Response(pack.model_dump_json(indent=2), media_type="application/json",
                    headers={"Content-Disposition": f'attachment; filename="evidence-{iid}.json"'})


@router.post("/incidents/{iid}/diagnose")
def diagnose(iid: str, body: DiagnoseRequest, p: Principal = Depends(require_perm("view.incidents"))) -> dict[str, Any]:
    """Re-run Stage A (any viewer); Stage B (LLM) needs incident.act and a reason (cost + data leaves the host)."""
    if body.use_llm:
        if not can(p.roles, "incident.act"):
            raise err(403, "FORBIDDEN", "requires incident.act")
        need_reason(body.reason)
    try:
        return inc.diagnose_incident(get_engine(), iid, p.subject, body.use_llm, p.primary_role,
                                     body.probe_destination, body.question)
    except inc.NotFound as e:
        raise _nf(e) from None


@router.post("/incident-actions/{aid}/decision")
def decide(aid: str, body: ActionDecision, p: Principal = Depends(require_perm("incident.act"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    try:
        return inc.decide_action(get_engine(), aid, p.subject, p.primary_role, reason, body.decision == "APPROVE")
    except inc.NotFound as e:
        raise _nf(e) from None
    except inc.Conflict as e:
        raise err(409, "ACTION_STATE", str(e)) from None


@router.post("/incidents/{iid}/resolve")
def resolve(iid: str, body: ResolveRequest, p: Principal = Depends(require_perm("incident.act"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    try:
        return inc.resolve(get_engine(), iid, p.subject, p.primary_role, reason, body.confirmed_root_cause,
                           body.fix_steps, body.save_known_issue)
    except inc.NotFound as e:
        raise _nf(e) from None
    except inc.Conflict as e:
        raise err(409, "INCIDENT_STATE", str(e)) from None


class Reasoned(BaseModel):
    reason: str | None = None


class CodeFixReview(BaseModel):
    reason: str | None = None
    decision: str = Field(pattern="^(APPROVE|REJECT)$")


@router.post("/incidents/{iid}/code-fix")
def propose_code_fix(iid: str, body: Reasoned, p: Principal = Depends(require_perm("incident.act"))) -> dict[str, Any]:
    """Stage-B code-fix proposal (PLATFORM_DEFECT only). Validated patch; never applied by the hub."""
    reason = need_reason(body.reason)
    try:
        return code_fix.propose(get_engine(), iid, p.subject, p.primary_role, reason)
    except LookupError as e:
        raise _nf(e) from None
    except code_fix.CodeFixError as e:
        raise err(409, "CODE_FIX_UNAVAILABLE", str(e)) from None


@router.post("/code-fixes/{fid}/review")
def review_code_fix(fid: str, body: CodeFixReview, p: Principal = Depends(require_perm("incident.act"))) -> dict[str, Any]:
    reason = need_reason(body.reason)
    try:
        return code_fix.review(get_engine(), fid, p.subject, p.primary_role, reason, body.decision == "APPROVE")
    except LookupError as e:
        raise _nf(e) from None
    except code_fix.CodeFixError as e:
        raise err(409, "CODE_FIX_STATE", str(e)) from None


@router.get("/code-fixes/{fid}/patch", response_class=PlainTextResponse)
def download_patch(fid: str, p: Principal = Depends(require_perm("view.incidents"))) -> PlainTextResponse:
    try:
        fx = code_fix.get(get_engine(), fid)
    except LookupError as e:
        raise _nf(e) from None
    with get_engine().begin() as c:
        store.audit(c, p.subject, "incident.code_fix.download", fx["incident_id"], {"code_fix_id": fid},
                    p.primary_role)
    return PlainTextResponse(fx["patch"], media_type="text/x-diff",
                             headers={"Content-Disposition": f'attachment; filename="incident-{fid[:8]}.patch"'})


@router.post("/incidents/{iid}/source-values")
def reveal_source_values(iid: str, body: Reasoned,
                         p: Principal = Depends(require_perm("incident.reveal"))) -> list[dict[str, Any]]:
    """Raw source values of a SOURCE_DATA incident - authorized roles only, reason required, audited."""
    reason = need_reason(body.reason)
    try:
        return inc.reveal(get_engine(), iid, p.subject, p.primary_role, reason)
    except inc.NotFound as e:
        raise _nf(e) from None


@router.get("/incidents/{iid}/regression-note", response_class=PlainTextResponse)
def regression_note(iid: str, _: Principal = Depends(require_perm("view.incidents"))) -> PlainTextResponse:
    try:
        return PlainTextResponse(inc.regression_note(get_engine(), iid), media_type="text/markdown")
    except inc.NotFound as e:
        raise _nf(e) from None


@router.post("/diagnose/scan")
def run_scan(_: Principal = Depends(require_perm("incident.act"))) -> dict[str, int]:
    return {"triggers": int(hooks.scan(get_engine()) or 0)}


@router.post("/diagnose/explain", response_model=ExplainResult)
def explain(body: ExplainRequest, p: Principal = Depends(require_perm("view.incidents"))) -> ExplainResult:
    """Explain any operational error (incident, job, batch, quarantine record, agent event, alert).
    Rules always; LLM when configured and the caller holds incident.act."""
    try:
        return inc.explain(get_engine(), body.kind, body.ref_id, p.subject,
                           body.use_llm and can(p.roles, "incident.act"), p.primary_role, body.labels)
    except (inc.NotFound, ValueError) as e:
        raise _nf(e) from None


# ------------------------------------------------------------------ admin: AI assistant
@router.get("/admin/llm", response_model=LlmSettingsView)
def get_llm(_: Principal = Depends(require_perm("llm.configure"))) -> LlmSettingsView:
    with get_engine().connect() as c:
        return llm.view(c)


@router.put("/admin/llm", response_model=LlmSettingsView)
def put_llm(body: LlmSettingsUpdate, p: Principal = Depends(require_perm("llm.configure"))) -> LlmSettingsView:
    reason = need_reason(body.reason)
    changes = body.model_dump(exclude={"reason"}, exclude_unset=True)
    if changes.get("api_key") and not llm.can_encrypt():
        raise err(409, "NO_ENCRYPTION_KEY", "set ALELT_DIAG_SECRET_KEY or ALELT_DIAG_SECRET_KEY_FILE to store an API key")
    with get_engine().begin() as c:
        llm.save_config(c, changes)
        shown = {k: ("***" if k == "api_key" and v else v) for k, v in changes.items()}
        store.audit(c, p.subject, "llm.settings.update", "diag.llm", {"changes": shown, "reason": reason},
                    p.primary_role)
        return llm.view(c)


@router.post("/admin/llm/test")
def test_llm(body: LlmTestRequest, p: Principal = Depends(require_perm("llm.configure"))) -> dict[str, Any]:
    """Connectivity + tiny prompt with the saved settings, overridden by the (unsaved) fields in the body."""
    with get_engine().begin() as c:
        cfg = llm.load_config(c)
        for k, v in body.model_dump(exclude={"reason", "api_key"}, exclude_unset=True).items():
            if v is not None:
                setattr(cfg, k, v)
        if body.api_key:
            cfg.api_key_plain = body.api_key          # used for this test only, never stored
        res = llm.check_connection(cfg)
        store.audit(c, p.subject, "llm.settings.test", "diag.llm", {"ok": res["ok"], "provider": cfg.provider,
                                                                    "model": cfg.model_name}, p.primary_role)
    return res


# ------------------------------------------------------------------ install
_AGENT_PATHS = ("/agent/v1/blocks", "/agent/v1/register", "/agent/v1/heartbeat", "/agent/v1/catalog")


def install(app: FastAPI) -> None:
    """Register the router and the agent-gateway observer (gap / backpressure / identity rejected)."""
    app.include_router(router)

    @app.middleware("http")
    async def observe_agent(request: Request, call_next: Any) -> Response:
        resp: Response = await call_next(request)
        path = request.url.path
        if path in _AGENT_PATHS and resp.status_code in (401, 403, 409, 426, 503):
            headers = {k.lower(): v for k, v in request.headers.items() if k.lower() in ("x-alelt-batch",)}
            await asyncio.to_thread(hooks.gateway_response, get_engine(), path, resp.status_code, headers)
        return resp


__all__ = ["install", "router"]

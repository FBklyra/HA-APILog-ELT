"""Agent identity (mTLS thumbprint / dev token) and console auth (Entra ID OIDC JWT / dev headers) + RBAC."""
from __future__ import annotations

import hmac
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from fastapi import Depends, Header, HTTPException, Request, status

from alelt_common import MIN_PROTOCOL_VERSION, PROTOCOL_VERSION

from .settings import Settings, get_settings


class Role(str, Enum):
    VIEWER = "Viewer"
    DATA_OPERATOR = "DataOperator"
    PLATFORM_ADMIN = "PlatformAdmin"
    AUDITOR = "Auditor"


# Permission matrix - mirrors console/src/lib/rbac.ts (ROLE_PERMISSIONS); the hub is the enforcing side.
_DATA_OPERATOR = frozenset({
    "view.dashboard", "view.tables", "view.recon", "view.certification", "table.configure", "table.resync",
    "table.reprocess", "import.run", "revision.rollback", "schema.decide", "recon.run", "cert.signoff", "wave.plan",
    "switch.request", "switch.approve", "view.incidents", "incident.act",
    "incident.reveal"})
ROLE_PERMISSIONS: dict[Role, frozenset[str]] = {
    Role.VIEWER: frozenset({"view.dashboard", "view.tables", "view.recon", "view.incidents"}),
    Role.DATA_OPERATOR: _DATA_OPERATOR,
    Role.PLATFORM_ADMIN: frozenset({"view.dashboard", "view.tables", "agent.view", "agent.command", "hub.view",
                                    "retention.edit", "users.manage", "audit.read", "audit.export",
                                    "view.incidents", "incident.act", "llm.configure"}),
    Role.AUDITOR: frozenset({"view.dashboard", "view.certification", "audit.read", "audit.export",
                             "view.incidents"}),
}
PERMISSIONS = frozenset().union(*ROLE_PERMISSIONS.values())


def can(roles: frozenset[Role], perm: str) -> bool:
    return any(perm in ROLE_PERMISSIONS[r] for r in roles)


@dataclass(frozen=True)
class Principal:
    subject: str
    roles: frozenset[Role]
    dev: bool = False
    name: str = ""
    email: str = ""

    @property
    def primary_role(self) -> str:
        order = [Role.PLATFORM_ADMIN, Role.DATA_OPERATOR, Role.AUDITOR, Role.VIEWER]
        return next((r.value for r in order if r in self.roles), "")


# ------------------------------------------------------------------ agents
@dataclass(frozen=True)
class AgentIdentity:
    kind: str                      # mtls | static | enrollment | credential
    value: str                     # thumbprint | 'token' | token sha256
    agent_id: str | None = None    # bound agent (enrollment after claim, credential)
    connection_id: str | None = None


def peer_host(scope: dict[str, Any]) -> str:
    """Immediate TCP peer, captured before X-Forwarded-For is applied (PeerCaptureMiddleware)."""
    peer = scope.get("alelt.peer") or scope.get("client") or ("", 0)
    return str(peer[0] or "")


def check_protocol(value: str | None) -> None:
    """Compatibility window (F-24, deploy/UPGRADE.md): the hub accepts agent protocol MIN..CURRENT; hub >= agent."""
    try:
        v = int(value or "")
    except ValueError:
        raise HTTPException(status.HTTP_426_UPGRADE_REQUIRED,
                            f"X-ALELT-Protocol required ({MIN_PROTOCOL_VERSION}..{PROTOCOL_VERSION})") from None
    if v > PROTOCOL_VERSION:
        raise HTTPException(status.HTTP_426_UPGRADE_REQUIRED,
                            f"agent protocol {v} is newer than this hub ({PROTOCOL_VERSION}): upgrade the hub first")
    if v < MIN_PROTOCOL_VERSION:
        raise HTTPException(status.HTTP_426_UPGRADE_REQUIRED,
                            f"agent protocol {v} is no longer supported (hub accepts {MIN_PROTOCOL_VERSION}.."
                            f"{PROTOCOL_VERSION}): upgrade the agent")


def authenticate_agent(headers: Any, scope: dict[str, Any]) -> AgentIdentity:
    """Agent identity from request headers (HTTP and WebSocket). Raises HTTPException 401/403.

    - mtls: the client-certificate thumbprint header is accepted ONLY when the TCP peer is a trusted proxy
      (ALELT_TRUSTED_PROXIES - the TLS terminator that verified the certificate; F-14). From any other peer it is
      ignored and the request is refused, so a client inside the network cannot spoof it.
    - enrollment token `alelte_...` (one-time, bound to connection + agent) and per-agent credential `alelta_...`
      (issued at registration, rotatable, expiring).
    - static shared tokens (ALELT_AGENT_TOKENS): dev/test only; refused at startup when ALELT_ENV=prod.
    """
    s: Settings = get_settings()
    tok = headers.get("x-alelt-agent-token") or ""
    if s.agent_auth == "mtls":
        if not s.is_trusted_proxy(peer_host(scope)):
            raise HTTPException(status.HTTP_403_FORBIDDEN,
                                "client certificate identity accepted only via a trusted TLS proxy (ALELT_TRUSTED_PROXIES)")
        tp = (headers.get(s.thumbprint_header) or "").replace(":", "").lower()
        if not tp or tp not in s.agent_thumbprints:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "client certificate not allow-listed")
        return AgentIdentity("mtls", tp)
    if tok.startswith("alelte_"):
        from .setup import enrollment   # binding enforced by setup middleware

        b = enrollment(tok)
        if b is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "enrollment token unknown, expired, revoked or retired")
        return AgentIdentity("enrollment", b["hash"], b.get("claimed_by"), b["connection_id"])
    if tok.startswith("alelta_"):
        from .setup import credential

        b = credential(tok)
        if b is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "agent credential unknown, expired, rotated or revoked")
        return AgentIdentity("credential", b["hash"], b["agent_id"], b["connection_id"])
    if not s.agent_tokens:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "agent token required (enroll the agent)")
    if not tok or not any(hmac.compare_digest(tok.encode(), t.encode()) for t in s.agent_tokens):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bad agent token")
    return AgentIdentity("static", "token")


def _mtls_binding(thumbprint: str, agent_id: str | None, connection_id: str | None, registering: bool) -> None:
    """AHS-01 / HA-03: a certificate is bound to the agent row that registered with it (agent.cert_thumbprint)."""
    from sqlalchemy import select

    from .db import agent, get_engine

    with get_engine().connect() as c:
        if agent_id is not None:
            row = c.execute(select(agent.c.cert_thumbprint, agent.c.connection_id).where(agent.c.id == agent_id)).first()
            if row is None:
                if registering:
                    return
                raise HTTPException(status.HTTP_403_FORBIDDEN, "agent not registered with this certificate")
            if row[0] and row[0] != thumbprint:
                raise HTTPException(status.HTTP_403_FORBIDDEN, "client certificate is not the one bound to this agent")
            if not row[0] and not registering:
                raise HTTPException(status.HTTP_403_FORBIDDEN, "agent has no certificate binding: register first")
            if connection_id is not None and row[1] != connection_id and not registering:
                raise HTTPException(status.HTTP_403_FORBIDDEN, "connection does not match the agent")
            return
        if connection_id is not None:
            ok = c.execute(select(agent.c.id).where(agent.c.connection_id == connection_id,
                                                    agent.c.cert_thumbprint == thumbprint).limit(1)).first()
            if ok is None:
                raise HTTPException(status.HTTP_403_FORBIDDEN, "no agent of this connection uses this certificate")


def require_binding(request: Request, agent_id: str | None = None, connection_id: str | None = None,
                    registering: bool = False) -> None:
    """A caller may act only as its bound agent and connection (403 otherwise): enrolment token / credential by
    their stored binding, mTLS by the certificate the agent registered with (AHS-01). Static tokens are dev-only."""
    ident: AgentIdentity | None = getattr(request.state, "alelt_agent", None)
    if ident is None:
        return
    if ident.kind == "mtls":
        _mtls_binding(ident.value, agent_id, connection_id, registering)
        return
    if agent_id is not None and ident.agent_id is not None and agent_id != ident.agent_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "agent_id does not match the agent credential")
    if connection_id is not None and ident.connection_id is not None and connection_id != ident.connection_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "connection does not match the agent credential")


def agent_identity(request: Request, x_alelt_protocol: str | None = Header(default=None)) -> str:
    """FastAPI dependency: validate protocol + identity. Returns the thumbprint (mtls) or 'token'."""
    check_protocol(x_alelt_protocol)
    ident = authenticate_agent(request.headers, request.scope)
    request.state.alelt_agent = ident
    return ident.value if ident.kind == "mtls" else "token"


# ------------------------------------------------------------------ console
_jwks_cache: dict[str, Any] = {}


def _jwks_client(url: str) -> Any:
    import jwt  # PyJWT

    c = _jwks_cache.get(url)
    if c is None or time.time() - c[1] > 3600:
        c = (jwt.PyJWKClient(url), time.time())
        _jwks_cache[url] = c
    return c[0]


def _roles(values: list[str]) -> frozenset[Role]:
    known = {r.value.lower(): r for r in Role}
    return frozenset(known[v.lower()] for v in values if v.lower() in known)


_groups_cache: list[Any] = [0.0, {}]


def _group_roles(groups: list[str]) -> frozenset[Role]:
    """F-15: identity-provider groups -> roles from `group_mapping` (console Users page), cached 60 s."""
    if not groups:
        return frozenset()
    if time.monotonic() - _groups_cache[0] > 60:
        from sqlalchemy import select

        from .db import get_engine, group_mapping

        try:
            with get_engine().connect() as c:
                m = {str(g): str(r) for g, r in c.execute(select(group_mapping.c.group_id, group_mapping.c.role))}
        except Exception:  # noqa: BLE001 - no mapping is better than no login
            m = {}
        _groups_cache[:] = [time.monotonic(), m]
    return _roles([_groups_cache[1][g] for g in groups if g in _groups_cache[1]])


def current_principal(request: Request, authorization: str | None = Header(default=None),
                      x_alelt_dev_role: str | None = Header(default=None),
                      x_alelt_dev_user: str | None = Header(default=None)) -> Principal:
    s = get_settings()
    if s.auth_mode == "dev":
        if not x_alelt_dev_role:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "dev auth: X-ALELT-Dev-Role required")
        return Principal(x_alelt_dev_user or "dev-user", _roles([r.strip() for r in x_alelt_dev_role.split(",")]), dev=True)
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bearer token required")
    if not (s.oidc_jwks_url and s.oidc_issuer and s.oidc_audience):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "OIDC not configured")
    import jwt

    token = authorization.split(" ", 1)[1]
    try:
        key = _jwks_client(s.oidc_jwks_url).get_signing_key_from_jwt(token).key
        claims = jwt.decode(token, key, algorithms=["RS256"], audience=s.oidc_audience, issuer=s.oidc_issuer)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {type(e).__name__}") from None
    subject = claims.get("preferred_username") or claims.get("upn") or claims.get("oid") or claims.get("sub", "")
    roles = _roles(list(claims.get("roles") or [])) | _group_roles([str(g) for g in claims.get("groups") or []])
    return Principal(str(claims.get("oid") or subject), roles,
                     name=str(claims.get("name") or subject), email=str(subject))


def require_perm(perm: str) -> Callable[..., Principal]:
    """FastAPI dependency factory: principal must hold permission `perm` (403 FORBIDDEN otherwise)."""
    if perm not in PERMISSIONS:
        raise ValueError(f"unknown permission {perm}")

    def dep(p: Principal = Depends(current_principal)) -> Principal:
        if not can(p.roles, perm):
            raise HTTPException(status.HTTP_403_FORBIDDEN, {"code": "FORBIDDEN", "message": f"requires {perm}"})
        return p
    return dep

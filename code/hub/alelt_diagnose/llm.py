"""Stage B - optional LLM analysis (off by default, provider-agnostic, plain HTTPS via httpx).

Providers: Anthropic Messages API, any OpenAI-compatible /chat/completions endpoint, local Ollama /api/chat.
The model only sees the redacted evidence pack + rule findings and must answer a JSON object validated as LlmAnswer.
LLM suggestions are never executed. Every call is recorded (prompt hash, provider, model, answer, tokens).

Configuration: env vars are defaults (ALELT_DIAG_LLM_*); the console (PUT /api/v1/admin/llm, PlatformAdmin) overrides
them, stored in hub_setting 'diag.llm'. The API key is write-only and encrypted at rest with a key from
ALELT_DIAG_SECRET_KEY / ALELT_DIAG_SECRET_KEY_FILE (R9); env ALELT_DIAG_LLM_API_KEY(_FILE) is used when none is stored.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import ValidationError
from sqlalchemy import Connection, insert, select, update

from alelt_hub.db import hub_setting

from .models import EvidencePack, Finding, LlmAnswer, LlmSettingsView
from .redact import scrub

SETTING_KEY = "diag.llm"
USAGE_KEY = "diag.llm.usage"
DEFAULT_URL = {"anthropic": "https://api.anthropic.com", "openai_compatible": "", "ollama": "http://localhost:11434"}
DEFAULT_MODEL = {"anthropic": "claude-sonnet-5-5", "openai_compatible": "", "ollama": ""}
MAX_PROMPT_CHARS = 60_000


class LlmError(RuntimeError):
    pass


# ------------------------------------------------------------------ secret at rest
def _fernet() -> Any:
    from cryptography.fernet import Fernet

    secret = os.environ.get("ALELT_DIAG_SECRET_KEY", "")
    f = os.environ.get("ALELT_DIAG_SECRET_KEY_FILE")
    if not secret and f and Path(f).is_file():
        secret = Path(f).read_text(encoding="utf-8").strip()
    if not secret:
        return None
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest()))


def can_encrypt() -> bool:
    return _fernet() is not None


def encrypt(plain: str) -> str:
    f = _fernet()
    if f is None:
        raise LlmError("ALELT_DIAG_SECRET_KEY / ALELT_DIAG_SECRET_KEY_FILE not set: cannot store an API key")
    return str(f.encrypt(plain.encode()).decode())


def decrypt(token: str) -> str | None:
    f = _fernet()
    if f is None:
        return None
    try:
        return str(f.decrypt(token.encode()).decode())
    except Exception:  # noqa: BLE001 - wrong key => treat as unset
        return None


# ------------------------------------------------------------------ settings
@dataclass
class LlmConfig:
    enabled: bool = False
    provider: str = "anthropic"
    url: str = ""
    model: str = ""
    timeout_s: float = 30.0
    max_tokens_day: int = 200_000
    max_output_tokens: int = 1500
    redaction: str = "strict"
    api_key_enc: str | None = None
    api_key_plain: str | None = None          # transient (admin "Test" with an unsaved key)
    source: dict[str, str] = field(default_factory=dict)

    def api_key(self) -> str | None:
        if self.api_key_plain:
            return self.api_key_plain
        if self.api_key_enc:
            return decrypt(self.api_key_enc)
        k = os.environ.get("ALELT_DIAG_LLM_API_KEY")
        f = os.environ.get("ALELT_DIAG_LLM_API_KEY_FILE")
        if not k and f and Path(f).is_file():
            k = Path(f).read_text(encoding="utf-8").strip()
        return k or None

    @property
    def endpoint(self) -> str:
        return (self.url or DEFAULT_URL.get(self.provider, "")).rstrip("/")

    @property
    def model_name(self) -> str:
        return self.model or DEFAULT_MODEL.get(self.provider, "")

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.endpoint and self.model_name
                    and (self.provider == "ollama" or self.api_key()))


_ENV = {"enabled": "ALELT_DIAG_LLM_ENABLED", "provider": "ALELT_DIAG_LLM_PROVIDER", "url": "ALELT_DIAG_LLM_URL",
        "model": "ALELT_DIAG_LLM_MODEL", "timeout_s": "ALELT_DIAG_LLM_TIMEOUT_S", "max_tokens_day": "ALELT_DIAG_LLM_MAX_TOKENS_DAY",
        "max_output_tokens": "ALELT_DIAG_LLM_MAX_OUTPUT_TOKENS", "redaction": "ALELT_DIAG_REDACTION"}


def _cast(name: str, raw: Any) -> Any:
    if name == "enabled":
        return raw if isinstance(raw, bool) else str(raw).lower() in ("1", "true", "yes", "on")
    if name in ("timeout_s",):
        return float(raw)
    if name in ("max_tokens_day", "max_output_tokens"):
        return int(raw)
    return str(raw)


def _get_setting(c: Connection, key: str) -> dict[str, Any]:
    v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar()
    return dict(v or {})


def _put_setting(c: Connection, key: str, value: dict[str, Any]) -> None:
    if c.execute(update(hub_setting).where(hub_setting.c.key == key).values(value=value)).rowcount == 0:
        c.execute(insert(hub_setting).values(key=key, value=value))


def load_config(c: Connection | None) -> LlmConfig:
    cfg = LlmConfig()
    for name, env in _ENV.items():
        cfg.source[name] = "default"
        if os.environ.get(env):
            setattr(cfg, name, _cast(name, os.environ[env]))
            cfg.source[name] = "env"
    stored = _get_setting(c, SETTING_KEY) if c is not None else {}
    for name in _ENV:
        if name in stored and stored[name] is not None:
            setattr(cfg, name, _cast(name, stored[name]))
            cfg.source[name] = "console"
    cfg.api_key_enc = stored.get("api_key_enc")
    cfg.source["api_key"] = "console" if cfg.api_key_enc else "env" if cfg.api_key() else "default"
    return cfg


def save_config(c: Connection, changes: dict[str, Any]) -> LlmConfig:
    """Persist console overrides. `api_key`: "" clears, a value is encrypted, absent keeps."""
    stored = _get_setting(c, SETTING_KEY)
    for k, v in changes.items():
        if k == "api_key":
            if v == "":
                stored.pop("api_key_enc", None)
            elif v is not None:
                stored["api_key_enc"] = encrypt(v)
        elif k in _ENV and v is not None:
            stored[k] = v
    _put_setting(c, SETTING_KEY, stored)
    return load_config(c)


def tokens_used_today(c: Connection) -> int:
    u = _get_setting(c, USAGE_KEY)
    return int(u.get("tokens", 0)) if u.get("date") == date.today().isoformat() else 0


def add_usage(c: Connection, tokens: int) -> None:
    _put_setting(c, USAGE_KEY, {"date": date.today().isoformat(), "tokens": tokens_used_today(c) + int(tokens)})


def view(c: Connection) -> LlmSettingsView:
    cfg = load_config(c)
    return LlmSettingsView(enabled=cfg.enabled, provider=cfg.provider, url=cfg.endpoint, model=cfg.model_name,  # type: ignore[arg-type]
                           timeout_s=cfg.timeout_s, max_tokens_day=cfg.max_tokens_day,
                           max_output_tokens=cfg.max_output_tokens, redaction=cfg.redaction,  # type: ignore[arg-type]
                           key_set=cfg.api_key() is not None, configured=cfg.configured,
                           tokens_used_today=tokens_used_today(c), source=cfg.source)


# ------------------------------------------------------------------ providers
@dataclass
class Completion:
    text: str
    model: str
    input_tokens: int
    output_tokens: int


class LlmProvider(Protocol):
    name: str

    def complete(self, system: str, user: str, max_tokens: int) -> Completion: ...


class _Base:
    name = "base"

    def __init__(self, cfg: LlmConfig, client: httpx.Client | None = None) -> None:
        self.cfg = cfg
        self.client = client or httpx.Client(timeout=cfg.timeout_s)

    def _post(self, url: str, headers: dict[str, str], body: dict[str, Any]) -> dict[str, Any]:
        try:
            r = self.client.post(url, headers=headers, json=body, timeout=self.cfg.timeout_s)
        except httpx.HTTPError as e:
            raise LlmError(f"{self.name}: {type(e).__name__}: {scrub(str(e))}") from None
        if r.status_code >= 400:
            raise LlmError(f"{self.name}: HTTP {r.status_code}: {scrub(r.text[:300])}")
        try:
            return dict(r.json())
        except ValueError:
            raise LlmError(f"{self.name}: non-JSON response") from None


class AnthropicProvider(_Base):
    """POST {url}/v1/messages (x-api-key, anthropic-version)."""

    name = "anthropic"

    def complete(self, system: str, user: str, max_tokens: int) -> Completion:
        d = self._post(f"{self.cfg.endpoint}/v1/messages",
                       {"x-api-key": self.cfg.api_key() or "", "anthropic-version": "2023-06-01",
                        "content-type": "application/json"},
                       {"model": self.cfg.model_name, "max_tokens": max_tokens, "system": system,
                        "messages": [{"role": "user", "content": user}]})
        if d.get("stop_reason") == "refusal":
            raise LlmError("anthropic: model declined the request (stop_reason=refusal)")
        text = "".join(b.get("text", "") for b in d.get("content", []) if b.get("type") == "text")
        u = d.get("usage") or {}
        return Completion(text, str(d.get("model") or self.cfg.model_name), int(u.get("input_tokens", 0)),
                          int(u.get("output_tokens", 0)))


class OpenAICompatibleProvider(_Base):
    """POST {url}/chat/completions (Bearer key). `url` is the API base, e.g. https://host/v1."""

    name = "openai_compatible"

    def complete(self, system: str, user: str, max_tokens: int) -> Completion:
        d = self._post(f"{self.cfg.endpoint}/chat/completions",
                       {"Authorization": f"Bearer {self.cfg.api_key() or ''}"},
                       {"model": self.cfg.model_name, "max_tokens": max_tokens,
                        "response_format": {"type": "json_object"},
                        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
        ch = (d.get("choices") or [{}])[0]
        u = d.get("usage") or {}
        return Completion(str((ch.get("message") or {}).get("content") or ""), str(d.get("model") or self.cfg.model_name),
                          int(u.get("prompt_tokens", 0)), int(u.get("completion_tokens", 0)))


class OllamaProvider(_Base):
    """POST {url}/api/chat (local, no key)."""

    name = "ollama"

    def complete(self, system: str, user: str, max_tokens: int) -> Completion:
        d = self._post(f"{self.cfg.endpoint}/api/chat", {},
                       {"model": self.cfg.model_name, "stream": False, "format": "json",
                        "options": {"num_predict": max_tokens},
                        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
        return Completion(str((d.get("message") or {}).get("content") or ""), str(d.get("model") or self.cfg.model_name),
                          int(d.get("prompt_eval_count", 0)), int(d.get("eval_count", 0)))


PROVIDERS: dict[str, type[_Base]] = {"anthropic": AnthropicProvider, "openai_compatible": OpenAICompatibleProvider,
                                     "ollama": OllamaProvider}


def provider_for(cfg: LlmConfig, client: httpx.Client | None = None) -> _Base:
    cls = PROVIDERS.get(cfg.provider)
    if cls is None:
        raise LlmError(f"unknown provider {cfg.provider}")
    return cls(cfg, client)


# ------------------------------------------------------------------ prompt + analysis
SYSTEM = """You are the incident analyst of ALELT, a log-based CDC replication platform (source agent reads the \
database log, hub decodes, assembles transactions and MERGEs into the destination with the checkpoint in the same \
transaction). You receive a redacted evidence pack (no row values) and the findings of deterministic rules.
Rules of engagement: you cannot act; propose steps for a human. Never suggest writing to the source database, \
deleting archive logs, or skipping data. Prefer replay from the hub raw archive and re-sync over manual fixes.
Answer with ONE JSON object only, no prose, with keys:
{"root_cause": str, "confidence": number 0..1, "reasoning_summary": str (max 5 sentences, cite evidence refs), \
"fix_steps": [str], "risk": "low"|"medium"|"high", "needs_human": bool, "classification": \
"PLATFORM_DEFECT"|"SOURCE_DATA"|"CONFIGURATION"|"INFRASTRUCTURE"|"DESTINATION"|"UNKNOWN"}"""


def build_prompt(pack: EvidencePack, findings: list[Finding], question: str | None = None) -> str:
    """User message: redacted pack + findings, size-capped (oldest logs and extra samples trimmed first)."""
    doc: dict[str, Any] = {"evidence": json.loads(pack.model_dump_json()),
                           "rule_findings": [f.model_dump(mode="json", exclude={"action"}) for f in findings[:5]]}
    if question:
        doc["operator_question"] = question
    text = json.dumps(doc, ensure_ascii=False, default=str)
    ev = doc["evidence"]
    for key in ("logs", "agent_events", "quarantine", "schema_changes", "config_revisions", "recon"):
        while len(text) > MAX_PROMPT_CHARS and ev.get(key):
            ev[key] = ev[key][len(ev[key]) // 2:] if len(ev[key]) > 1 else []
            text = json.dumps(doc, ensure_ascii=False, default=str)
    if len(text) > MAX_PROMPT_CHARS:
        ev["catalog"] = {"versions": ev.get("catalog", {}).get("versions", [])}
        text = json.dumps(doc, ensure_ascii=False, default=str)
    return scrub(text, pack.redaction)      # second pass: never let a secret through


def parse_answer(text: str) -> LlmAnswer:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise LlmError("model answer contains no JSON object")
    try:
        return LlmAnswer.model_validate_json(m.group(0))
    except ValidationError as e:
        raise LlmError(f"model answer does not match the schema: {e.errors()[:2]}") from None


@dataclass
class Analysis:
    answer: LlmAnswer
    provider: str
    model: str
    prompt_hash: str
    tokens: int
    duration_ms: int


def analyze(c: Connection, pack: EvidencePack, findings: list[Finding], cfg: LlmConfig | None = None,
            client: httpx.Client | None = None, question: str | None = None) -> Analysis:
    """One bounded call. Raises LlmError (not configured / budget / transport / schema)."""
    cfg = cfg or load_config(c)
    if not cfg.configured:
        raise LlmError("AI diagnosis not configured")
    prompt = build_prompt(pack, findings, question)
    est = len(prompt) // 4 + cfg.max_output_tokens
    if cfg.max_tokens_day and tokens_used_today(c) + est > cfg.max_tokens_day:
        raise LlmError(f"daily token budget exhausted ({cfg.max_tokens_day})")
    t0 = time.monotonic()
    comp = provider_for(cfg, client).complete(SYSTEM, prompt, cfg.max_output_tokens)
    used = (comp.input_tokens + comp.output_tokens) or est
    add_usage(c, used)
    return Analysis(parse_answer(comp.text), cfg.provider, comp.model,
                    hashlib.sha256((SYSTEM + prompt).encode()).hexdigest(), used, int((time.monotonic() - t0) * 1000))


def check_connection(cfg: LlmConfig, client: httpx.Client | None = None) -> dict[str, Any]:
    """Connectivity + tiny prompt. Returns {ok, latency_ms, model, error}."""
    t0 = time.monotonic()
    try:
        comp = provider_for(cfg, client).complete("Answer with JSON only.", 'Reply exactly {"ok": true}', 32)
        ok = '"ok"' in comp.text
        return {"ok": ok, "latency_ms": int((time.monotonic() - t0) * 1000), "model": comp.model,
                "error": None if ok else "unexpected answer"}
    except LlmError as e:
        return {"ok": False, "latency_ms": int((time.monotonic() - t0) * 1000), "model": cfg.model_name,
                "error": str(e)}

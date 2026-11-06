"""Stage B providers against a fake HTTP server: request shape, JSON validation, budget, secrets at rest."""
from __future__ import annotations

import json
from typing import Any

import pytest

from alelt_diagnose import llm
from alelt_diagnose.models import IncidentKind
from alelt_diagnose.rules import evaluate

from .conftest import Fake, pack

ANSWER = {"root_cause": "codepage 37 data in a 1208 column", "confidence": 0.8,
          "reasoning_summary": "quarantine:q1 shows BAD_CODEPAGE", "fix_steps": ["map codepage 37"], "risk": "low",
          "needs_human": True}


def _reply(path: str, body: dict[str, Any]) -> dict[str, Any]:
    text = "Here you go:\n```json\n" + json.dumps(ANSWER) + "\n```"
    if path == "/v1/messages":
        return {"model": body["model"], "stop_reason": "end_turn", "content": [{"type": "text", "text": text}],
                "usage": {"input_tokens": 900, "output_tokens": 100}}
    if path.endswith("/chat/completions"):
        return {"model": body["model"], "choices": [{"message": {"content": text}}],
                "usage": {"prompt_tokens": 900, "completion_tokens": 100}}
    return {"model": body["model"], "message": {"content": text}, "prompt_eval_count": 900, "eval_count": 100}


@pytest.mark.parametrize("provider,path", [("anthropic", "/v1/messages"), ("openai_compatible", "/v1/chat/completions"),
                                           ("ollama", "/api/chat")])
def test_providers(engine: Any, server: tuple[str, Fake], provider: str, path: str) -> None:
    url, fake = server
    fake.reply = _reply
    base = url + ("/v1" if provider == "openai_compatible" else "")
    cfg = llm.LlmConfig(enabled=True, provider=provider, url=base, model="m-1", api_key_plain="k-123")
    p = pack(IncidentKind.QUARANTINE, "row image: BAD_CODEPAGE: NAME codepage 37")
    with engine.begin() as c:
        a = llm.analyze(c, p, evaluate(p), cfg)
        assert llm.tokens_used_today(c) == 1000
    assert a.answer.root_cause == ANSWER["root_cause"] and a.model == "m-1" and len(a.prompt_hash) == 64
    req = fake.requests[-1]
    assert req["path"] == path and req["body"]["model"] == "m-1"
    if provider == "anthropic":
        assert req["headers"]["x-api-key"] == "k-123" and req["headers"]["anthropic-version"] == "2023-06-01"
        assert req["body"]["system"] and req["body"]["messages"][0]["role"] == "user"
    elif provider == "openai_compatible":
        assert req["headers"]["Authorization"] == "Bearer k-123"


def test_invalid_answer_budget_and_errors(engine: Any, server: tuple[str, Fake]) -> None:
    url, fake = server
    cfg = llm.LlmConfig(enabled=True, provider="ollama", url=url, model="m")
    p = pack(IncidentKind.JOB_FAILED, "boom")
    with engine.begin() as c:
        fake.reply = {"model": "m", "message": {"content": '{"root_cause": "x"}'}}      # missing fields
        with pytest.raises(llm.LlmError, match="schema"):
            llm.analyze(c, p, [], cfg)
        fake.status, fake.reply = 500, {"error": "down"}
        with pytest.raises(llm.LlmError, match="HTTP 500"):
            llm.analyze(c, p, [], cfg)
        cfg.max_tokens_day = 10
        with pytest.raises(llm.LlmError, match="budget"):
            llm.analyze(c, p, [], cfg)
    assert not llm.LlmConfig(enabled=False, provider="ollama", url=url, model="m").configured
    assert not llm.LlmConfig(enabled=True, provider="anthropic", model="m").configured        # no key


def test_check_connection(server: tuple[str, Fake]) -> None:
    url, fake = server
    fake.reply = {"model": "m", "stop_reason": "end_turn", "content": [{"type": "text", "text": '{"ok": true}'}],
                  "usage": {}}
    r = llm.check_connection(llm.LlmConfig(enabled=True, provider="anthropic", url=url, model="m", api_key_plain="k"))
    assert r["ok"] and r["model"] == "m" and r["latency_ms"] >= 0


def test_settings_env_console_and_encrypted_key(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_DIAG_LLM_PROVIDER", "ollama")
    monkeypatch.setenv("ALELT_DIAG_LLM_MODEL", "llama-x")
    monkeypatch.delenv("ALELT_DIAG_SECRET_KEY", raising=False)
    with engine.begin() as c:
        cfg = llm.load_config(c)
        assert (cfg.provider, cfg.model, cfg.source["model"], cfg.enabled) == ("ollama", "llama-x", "env", False)
        with pytest.raises(llm.LlmError):
            llm.save_config(c, {"api_key": "sk-secret-value-123456"})
        monkeypatch.setenv("ALELT_DIAG_SECRET_KEY", "test-only-secret")
        cfg = llm.save_config(c, {"provider": "anthropic", "enabled": True, "model": "claude-sonnet-5-5",
                                  "api_key": "sk-secret-value-123456"})
        v = llm.view(c)
        assert cfg.source["provider"] == "console" and v.key_set and v.configured
        assert "sk-secret" not in v.model_dump_json()
        from alelt_hub.db import hub_setting
        from sqlalchemy import select

        raw = json.dumps(c.execute(select(hub_setting.c.value).where(hub_setting.c.key == llm.SETTING_KEY)).scalar())
        assert "sk-secret" not in raw and "api_key_enc" in raw                        # encrypted at rest
        assert cfg.api_key() == "sk-secret-value-123456"
        assert not llm.save_config(c, {"api_key": ""}).api_key_enc

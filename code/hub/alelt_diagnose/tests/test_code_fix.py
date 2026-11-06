"""Code-fix proposals: patch validation (allowed paths, git apply --check), propose/review/export flow."""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from alelt_diagnose import code_fix, incidents as inc, llm
from alelt_diagnose.models import IncidentKind, Trigger

from .conftest import Fake

REL = "connectors/sources/db2/alelt_src_db2/rowimage.py"
ORIG = "def sign(n):\n    if n == 0xC:\n        return 1\n    return -1\n"
PATCH = f"""diff --git a/{REL} b/{REL}
--- a/{REL}
+++ b/{REL}
@@ -1,4 +1,4 @@
 def sign(n):
-    if n == 0xC:
+    if n in (0xA, 0xC, 0xE, 0xF):
         return 1
     return -1
diff --git a/tests/golden/db2/bcd_sign/case.json b/tests/golden/db2/bcd_sign/case.json
new file mode 100644
--- /dev/null
+++ b/tests/golden/db2/bcd_sign/case.json
@@ -0,0 +1 @@
+{{"expected": null}}
"""
needs_git = pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")


@pytest.fixture()
def src(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "src"
    (root / REL).parent.mkdir(parents=True)
    (root / REL).write_text(ORIG, encoding="utf-8", newline="\n")
    monkeypatch.setenv("ALELT_DIAG_SOURCE_DIR", str(root))
    return root


def test_paths_policy(src: Path) -> None:
    assert code_fix.patch_paths(PATCH) == [REL, "tests/golden/db2/bcd_sign/case.json"]
    for bad in ("deploy/hub/docker-compose.yml", ".github/workflows/ci.yml", "hub/alelt_hub/../../etc/passwd",
                "connectors/sources/db2/certs/agent.pem", "README.md"):
        v = code_fix.validate(f"--- a/{bad}\n+++ b/{bad}\n@@ -1 +1 @@\n-x\n+y\n", src)
        assert not v["paths_ok"] and v["disallowed"] == [bad], bad
    v = code_fix.validate("not a patch", src)
    assert not v["paths_ok"] and v["message"]


@needs_git
def test_git_apply_check(src: Path) -> None:
    v = code_fix.validate(PATCH, src)
    assert v["paths_ok"] and v["has_test"] and v["apply_check"] == "ok", v
    assert (src / REL).read_text(encoding="utf-8") == ORIG                    # --check never modifies the tree
    stale = PATCH.replace("-    if n == 0xC:", "-    if n == 0xD:")
    assert code_fix.validate(stale, src)["apply_check"] == "failed"


def _answer(patch: str) -> dict[str, Any]:
    text = json.dumps({"summary": "Accept all positive BCD sign nibbles", "explanation": "Db2 writes 0xF too",
                       "files": [REL], "patch": patch, "test_path": "tests/golden/db2/bcd_sign/case.json",
                       "confidence": 0.7})
    return {"model": "m", "stop_reason": "end_turn", "content": [{"type": "text", "text": text}],
            "usage": {"input_tokens": 100, "output_tokens": 50}}


def test_propose_review_export(engine: Any, src: Path, server: tuple[str, Fake], tmp_path: Path,
                               monkeypatch: pytest.MonkeyPatch) -> None:
    url, fake = server
    fake.reply = _answer(PATCH)
    monkeypatch.setenv("ALELT_DIAG_SECRET_KEY", "unit")
    with engine.begin() as c:
        llm.save_config(c, {"enabled": True, "provider": "anthropic", "url": url, "model": "m", "api_key": "sk-unit-0000000000"})
    iid, _ = inc.open_incident(engine, Trigger(kind=IncidentKind.QUARANTINE, connection_id="c1", table_fqn="S.T",
                                               position="1:0A", error="row image: BAD_BCD: AMT sign nibble F"))
    d = inc.get_incident(engine, iid)
    assert d["rule_id"] == "decoder_defect" and d["classification"] == "PLATFORM_DEFECT"

    fx = code_fix.propose(engine, iid, "op@x", "DataOperator", "please fix")
    assert fx["status"] == "PROPOSED" and fx["validation"]["paths_ok"] and fx["validation"]["has_test"]
    if shutil.which("git"):
        assert fx["validation"]["apply_check"] == "ok"
    sent = fake.requests[-1]["body"]["messages"][0]["content"]
    assert "def sign(n)" in sent and "sk-unit" not in sent                      # source file sent, never the key
    assert fx["regression_case"]["path"].startswith("tests/golden/db2/incident-")
    assert inc.get_incident(engine, iid)["code_fixes"][0]["id"] == fx["id"]
    assert (src / REL).read_text(encoding="utf-8") == ORIG                    # never applied

    assert code_fix.export(engine, iid, tmp_path / "out") == []                 # PROPOSED: not exportable
    r = code_fix.review(engine, fx["id"], "lead@x", "DataOperator", "looks right", approve=True)
    assert r["status"] == "REVIEWED"
    with pytest.raises(code_fix.CodeFixError):
        code_fix.review(engine, fx["id"], "lead@x", "DataOperator", "again", approve=True)
    acts = inc.get_incident(engine, iid)["actions"]
    assert any(a["kind"] == "REDECODE_RANGE" and a["status"] == "PROPOSED" for a in acts)   # after the release
    files = code_fix.export(engine, iid, tmp_path / "out")
    assert [f.suffix for f in files] == [".patch", ".json"] and PATCH in files[0].read_text(encoding="utf-8")
    assert code_fix.get(engine, fx["id"])["status"] == "EXPORTED"


def test_code_fix_refused_for_other_classes(engine: Any) -> None:
    iid, _ = inc.open_incident(engine, Trigger(kind=IncidentKind.DESTINATION_ERROR, connection_id="c1",
                                               error="OperationalError: connection refused"))
    with pytest.raises(code_fix.CodeFixError, match="PLATFORM_DEFECT"):
        code_fix.propose(engine, iid, "op", "DataOperator", "r")

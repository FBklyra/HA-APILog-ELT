"""F-26: the clean-clone publish check finds what must not leak and ignores what is excluded."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import publish_check  # noqa: E402


def test_publish_check(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / ".publicignore").write_text("NOTES.md\nmailbox/\nextensions/private/\n.publicignore\n", encoding="utf-8")
    (tmp_path / "NOTES.md").write_text("internal " + "five" + "tran", encoding="utf-8")
    (tmp_path / "mailbox").mkdir()
    (tmp_path / "mailbox" / "MAILBOX.md").write_text("five" + "tran", encoding="utf-8")
    (tmp_path / "extensions" / "private").mkdir(parents=True)
    (tmp_path / "extensions" / "private" / "x.py").write_text("FIVE" + "TRAN = 1", encoding="utf-8")
    (tmp_path / "ok.md").write_text("see the mailbox icon; nothing internal\n", encoding="utf-8")
    assert publish_check.main(["--root", str(tmp_path)]) == 0            # excluded paths never scanned
    (tmp_path / "doc.md").write_text("details in NOTES.md and mailbox/MAILBOX.md\n", encoding="utf-8")
    (tmp_path / "a.py").write_text("# Co-" + "Authored-By: " + "Cl" + "aude x\nname = 'Acme" + "Corp'\n", encoding="utf-8")
    (tmp_path / "vendor.dll").write_bytes(b"MZ")
    out = tmp_path / "f.json"
    assert publish_check.main(["--root", str(tmp_path), "--deny", "AcmeCorp", "--json", str(out)]) == 1
    import json

    f = json.loads(out.read_text(encoding="utf-8"))["findings"]
    assert {x["ref"] for x in f["internal"]} == {"NOTES.md", "mailbox"}
    assert {x["term"] for x in f["deny"]} == {"ai-attribution", "deny:AcmeCorp"}
    assert [x["file"] for x in f["binaries"]] == ["vendor.dll"]


def test_publish_check_blocks_the_internal_json_binary_decoder(tmp_path: Path) -> None:  # X-133
    (tmp_path / ".publicignore").write_text("extensions/mysql-json-" + "binary/\n.publicignore\n", encoding="utf-8")
    priv = tmp_path / "extensions" / ("mysql-json-" + "binary")
    priv.mkdir(parents=True)
    (priv / "x.py").write_text("def decode_" + "json(b): ...\n", encoding="utf-8")
    (tmp_path / "ok.py").write_text("from alelt_src_mysql.jsonhook import load_json_decoder\n", encoding="utf-8")
    assert publish_check.main(["--root", str(tmp_path)]) == 0           # excluded: never exported
    (tmp_path / "leak").mkdir()
    (tmp_path / "leak" / ("json" + "bin.py")).write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "leak2.py").write_text("v = decode_" + "json(raw)\nimport alelt_ext_mysql_" + "json" + "bin\n",
                                       encoding="utf-8")
    out = tmp_path / "f.json"
    assert publish_check.main(["--root", str(tmp_path), "--json", str(out)]) == 1
    import json

    f = json.loads(out.read_text(encoding="utf-8"))["findings"]["deny"]
    assert {x["file"] for x in f if x["term"] == "json-binary-decoder"} == {"leak/" + "json" + "bin.py", "leak2.py"}

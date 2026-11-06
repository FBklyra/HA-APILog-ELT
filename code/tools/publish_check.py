"""Clean-clone publish check (F-26): export the tree as it would be published and grep it for what must not leak.

Steps 3 and 7 of docs/legal/PUBLISHING.md on a *working tree* (no git needed): copy every file not excluded by
`.publicignore` (plus build/cache folders) into a fresh temporary directory, then scan the copy:

  deny        legacy-vendor term (assembled at runtime), AI-attribution trailers, extra words from --deny / a deny file
              (`re:<regex>` = case-sensitive regex); also file names and non-text files (bytes, ASCII + UTF-16LE)
  internal    references from published files to paths that are NOT published (internal notes listed in .publicignore)
  binaries    vendor binaries / headers (*.dll, *.lib, *.so, sqlenv.h, db2ApiDf.h)

  python tools/publish_check.py [--deny-file FILE] [--deny WORD ...] [--keep] [--json OUT]

Exit 0 = clean, 1 = findings. The temporary export is deleted unless --keep. Run it before step 9 (push) on the
same commit that went through filter-repo; it is a second, independent check, not a replacement for steps 3-8.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALWAYS_SKIP = {".git", "node_modules", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "dist", ".alelt",
               ".venv", "build", "out"}
TEXT_EXT = {".py", ".md", ".txt", ".yml", ".yaml", ".json", ".toml", ".ts", ".tsx", ".js", ".c", ".h", ".ps1", ".sh",
            ".sql", ".cfg", ".ini", ".cmake", ".html", ".css", ".env", ".example", ".csv", ".service", ".wxs", ""}
BINARY = re.compile(r"(\.(dll|lib|so|dylib|exe)$)|(^|/)(sqlenv\.h|db2ApiDf\.h|sqlcli1?\.h)$", re.I)


def read_ignore(root: Path) -> list[str]:
    f = root / ".publicignore"
    if not f.exists():
        return []
    out = []
    for line in f.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.append(line)
    return out


def excluded(rel: str, patterns: list[str]) -> bool:
    for p in patterns:
        if p.endswith("/"):
            if rel == p[:-1] or rel.startswith(p):
                return True
        elif rel == p or fnmatch.fnmatch(rel, p):
            return True
    return False


def is_local_secret(name: str) -> bool:
    """`.env` / `.env.<x>` hold real local secrets; only `*.example` templates are published."""
    return name == ".env" or (name.startswith(".env.") and not name.endswith(".example"))


def export(root: Path, dst: Path, patterns: list[str]) -> int:
    n = 0
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root).as_posix()
        rel_dir = "" if rel_dir == "." else rel_dir + "/"
        dirnames[:] = [d for d in dirnames if d not in ALWAYS_SKIP and not excluded(rel_dir + d + "/", patterns)]
        for fn in filenames:
            rel = rel_dir + fn
            if is_local_secret(fn):                  # machine-local .env files never leave the machine
                continue
            if excluded(rel, patterns) or fn.endswith((".pyc", ".tmp")) or fn.startswith("result.") and fn.endswith(".json.tmp"):
                continue
            target = dst / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(Path(dirpath) / fn, target)
            n += 1
    return n


_AI = "cl" + "aude"   # assembled at runtime so this file does not itself match


def deny_terms(extra: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    terms = [("legacy-vendor", re.compile("five" + "tran", re.I)),
             # X-133: the internal MySQL binary JSON decoder plugin (package / module / entry function) never ships
             ("json-binary-decoder", re.compile("alelt_ext_mysql_" + "jsonbin|" + r"\bjson" + r"bin\b|decode_" + r"json\(")),
             ("ai-attribution", re.compile(r"co-" + r"authored-by:\s*" + _AI + r"|generated with \[?" + _AI + r"|noreply@" + "anth" + "ropic", re.I))]
    for w in (w.strip() for w in extra):
        if w.startswith("re:"):                      # case-sensitive regex (generic words: exact identifiers only)
            terms.append((f"deny:{w[3:]}", re.compile(w[3:])))
        elif w:
            terms.append((f"deny:{w}", re.compile(re.escape(w), re.I)))
    return terms


def _binary_hits(f: Path, terms: list[tuple[str, re.Pattern[str]]]) -> set[str]:
    """Deny terms inside a non-text file: as single-byte text and as UTF-16LE (both alignments)."""
    data = f.read_bytes()
    views = [data.decode("latin-1"), data.decode("utf-16-le", errors="ignore"),
             data[1:].decode("utf-16-le", errors="ignore")]
    return {name for name, rx in terms if name.startswith("deny:") or name == "legacy-vendor"
            for v in views if rx.search(v)}


def read_noref(root: Path) -> set[str]:
    """`.publicignore` entries marked `# noref`: excluded files at a conventional location that published code may
    name (e.g. the default config path, where a generated regression case goes) - not reported as internal refs."""
    f = root / ".publicignore"
    if not f.exists():
        return set()
    return {line.split("#", 1)[0].strip() for line in f.read_text(encoding="utf-8").splitlines()
            if "#" in line and line.split("#", 1)[1].strip() == "noref" and line.split("#", 1)[0].strip()}


def internal_refs(patterns: list[str], noref: set[str] | None = None) -> re.Pattern[str]:
    # directories only as `name/...`, files by their exact name - a bare English word ("exchange") is not a reference
    names = [re.escape(p) if p.endswith("/") else re.escape(p) + r"(?![\w.-])" for p in patterns
             if p != ".publicignore" and p not in (noref or set())]
    return re.compile(r"(?<![\w.-])(" + "|".join(names) + ")") if names else re.compile(r"$^")


def scan(dst: Path, patterns: list[str], extra: list[str],
         noref: set[str] | None = None) -> dict[str, list[dict[str, object]]]:
    findings: dict[str, list[dict[str, object]]] = {"deny": [], "internal": [], "binaries": []}
    terms = deny_terms(extra)
    refs = internal_refs(patterns, noref)
    for f in sorted(dst.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(dst).as_posix()
        if "mysql-json-" + "binary" in rel or "json" + "bin" in Path(rel).name:   # X-133: internal plugin files
            findings["deny"].append({"file": rel, "line": 0, "term": "json-binary-decoder"})
            continue
        if BINARY.search(rel):
            findings["binaries"].append({"file": rel})
            continue
        for name, rx in terms:                       # file and directory names
            if name.startswith("deny:") and rx.search(rel):
                findings["deny"].append({"file": rel, "line": 0, "term": name + " (path)"})
        if f.suffix.lower() not in TEXT_EXT or f.stat().st_size > 5_000_000:
            findings["deny"] += [{"file": rel, "line": 0, "term": t} for t in sorted(_binary_hits(f, terms))]
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if rel == "tools/publish_check.py":          # the patterns themselves
            continue
        for i, line in enumerate(text.splitlines(), 1):
            for name, rx in terms:
                if rx.search(line):
                    findings["deny"].append({"file": rel, "line": i, "term": name})
            for m in refs.finditer(line):
                findings["internal"].append({"file": rel, "line": i, "ref": m.group(1).rstrip("/")})
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(ROOT))
    ap.add_argument("--deny", action="append", default=[], help="extra deny word (repeatable)")
    ap.add_argument("--deny-file", help="file with one deny word per line (keep it outside the published tree)")
    ap.add_argument("--keep", action="store_true", help="keep the temporary export")
    ap.add_argument("--json", help="write findings as JSON")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()
    extra = list(args.deny)
    if args.deny_file:
        extra += [w.strip() for w in Path(args.deny_file).read_text(encoding="utf-8").splitlines() if w.strip()]
    patterns = read_ignore(root)
    dst = Path(tempfile.mkdtemp(prefix="alelt-public-"))
    try:
        n = export(root, dst, patterns)
        findings = scan(dst, patterns, extra, read_noref(root))
    finally:
        if not args.keep:
            shutil.rmtree(dst, ignore_errors=True)
    total = sum(len(v) for v in findings.values())
    summary = {"exported_files": n, "export_dir": str(dst) if args.keep else None, "findings": total,
               "by_kind": {k: len(v) for k, v in findings.items()},
               "files": sorted({str(x["file"]) for v in findings.values() for x in v})}
    if args.json:
        Path(args.json).write_text(json.dumps({"summary": summary, "findings": findings}, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())

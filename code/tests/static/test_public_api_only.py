# SPDX-License-Identifier: Apache-2.0
"""Guard: capture code reads changes only through vendors' public log APIs (docs/PUBLIC-APIS.md).

Fails if production code under connectors/, hub/ or edge-agent/ opens, lists, maps or globs database
log / archive files directly (Db2 S*.LOG, Oracle redo/archive .log/.arc/.dbf, SQL Server .ldf/.mdf,
PostgreSQL pg_wal / pg_xlog, MySQL / MariaDB binlog files, InnoDB redo files).

Allowed: the public APIs themselves (db2ReadLog, logical replication / pgoutput, the replication protocol,
DBMS_LOGMNR, cdc.fn_cdc_get_all_changes_*), which the server reads on our behalf.
Run: python -m pytest tests/static -q   (stdlib only, no repo imports)
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCAN_ROOTS = ("connectors", "hub", "edge-agent")
SKIP_PARTS = {"tests", "test", "corpus", "golden", "node_modules", "__pycache__", "build", "out", ".venv", "dist"}

# File names / directories of engine-internal log storage.
LOG_PATTERNS = re.compile(
    r"""(
        \bS\d{7}\.LOG\b              # Db2 active / archive log file
      | \bSQLOGDIR\b | \bLOGSTREAM\d{4}\b
      | \bpg_wal\b | \bpg_xlog\b     # PostgreSQL WAL directory
      | \.ldf\b | \.mdf\b            # SQL Server log / data file
      | \bredo\w*\.log\b | \.arc\b | \.dbf\b | \bonlinelog\b | \barchivelog[\\/]   # Oracle redo / archive / datafiles
      | \b(mysql|mariadb)-bin\.\d+ | \bbinlog\.\d{3,} | \bib_logfile\d* | \#innodb_redo   # MySQL / MariaDB
    )""",
    re.IGNORECASE | re.VERBOSE,
)

PY_FILE_CALLS = {"open", "read_bytes", "read_text", "glob", "rglob", "iglob", "listdir", "scandir", "walk",
                 "mmap", "fdopen", "FileIO", "Path"}
C_FILE_CALL = re.compile(r"\b(fopen|_wfopen|fopen_s|_wfopen_s|open|_open|_wopen|CreateFile[AW]?|CreateFileMapping[AW]?|"
                         r"FindFirstFile[AW]?|opendir|mmap)\s*\(")


def _call_name(node: ast.Call) -> str:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _strings(node: ast.AST) -> list[str]:
    return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def python_violations(source: str, name: str = "<src>") -> list[str]:
    """Call sites that pass a log-file path/pattern to a file-system call."""
    try:
        tree = ast.parse(source, filename=name)
    except SyntaxError:
        return []
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) in PY_FILE_CALLS:
            for s in _strings(node):
                if LOG_PATTERNS.search(s):
                    out.append(f"{name}:{node.lineno}: {_call_name(node)}({s!r})")
    return out


def c_violations(source: str, name: str = "<src>") -> list[str]:
    out = []
    for i, line in enumerate(source.splitlines(), 1):
        if C_FILE_CALL.search(line) and LOG_PATTERNS.search(line):
            out.append(f"{name}:{i}: {line.strip()}")
    return out


def _files() -> list[Path]:
    files = []
    for root in SCAN_ROOTS:
        for p in (REPO / root).rglob("*"):
            if p.suffix not in (".py", ".c", ".h") or not p.is_file():
                continue
            if SKIP_PARTS & set(p.relative_to(REPO).parts) or p.name.startswith("test_"):
                continue
            files.append(p)
    return files


def test_guard_detects_direct_log_access() -> None:
    """The guard itself must catch the shapes it exists for (protects against a silently broken regex)."""
    bad_py = [
        "open('/var/lib/postgresql/data/pg_wal/000000010000000000000001', 'rb')",
        "Path('/var/lib/mysql').glob('mysql-bin.0*')",
        "os.listdir('C:/db2/NODE0000/SQL00001/LOGSTREAM0000')",
        "open(r'D:\\\\db2log\\\\S0000123.LOG', 'rb')",
        "open('/u01/oradata/FREE/redo01.log','rb')",
        "open('C:/mssql/data/shop_log.ldf','rb')",
        "glob.glob('/var/lib/mysql/binlog.000*')",
    ]
    for snippet in bad_py:
        assert python_violations(snippet), snippet
    assert c_violations('FILE *f = fopen("C:\\\\db2\\\\S0000001.LOG", "rb");')
    assert c_violations('h = CreateFileW(L"D:\\\\data\\\\shop_log.ldf", GENERIC_READ, 0, 0, OPEN_EXISTING, 0, 0);')


def test_guard_allows_public_apis() -> None:
    ok = [
        "cur.execute(\"SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_t(@f, @t, 'all update old')\")",
        "cur.execute(\"BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :f); END;\")",
        "conn.start_replication(slot_name='alelt', options={'proto_version': '1', 'publication_names': 'p'})",
        "open('/etc/alelt/config.yaml')",
        "open(spool_dir / 'batch.zst', 'rb')",
    ]
    for snippet in ok:
        assert not python_violations(snippet), snippet
    assert not c_violations("rc = db2ReadLog(DB2VERSION, &readLog, &sqlca);")


def test_no_connector_opens_database_log_files() -> None:
    files = _files()
    assert files, "nothing scanned: check SCAN_ROOTS"
    found: list[str] = []
    for p in files:
        text = p.read_text(encoding="utf-8", errors="replace")
        rel = str(p.relative_to(REPO))
        found += python_violations(text, rel) if p.suffix == ".py" else c_violations(text, rel)
    assert not found, ("Direct access to database log/archive files is forbidden; use the engine's public log API "
                       "(docs/PUBLIC-APIS.md):\n" + "\n".join(found))

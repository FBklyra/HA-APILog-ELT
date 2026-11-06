"""F-27: the daemon wheel carries aleltd + alelt_common + ONE connector only, and runs from that alone."""
from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))

import build_wheel  # noqa: E402


def test_postgres_wheel_is_minimal_and_importable(tmp_path: Path) -> None:
    assert build_wheel.main(["--engine", "postgres", "--out", str(tmp_path / "w")]) == 0
    whl = next((tmp_path / "w").glob("alelt_agentd_postgres-*.whl"))
    tops = {n.split("/")[0] for n in zipfile.ZipFile(whl).namelist() if not n.split("/")[0].endswith(".dist-info")}
    assert tops == {"aleltd", "alelt_common", "alelt_src_postgres"}
    target = tmp_path / "site"
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(target), str(whl)],
                   check=True, capture_output=True)
    code = ("import sys; sys.path[:] = [p for p in sys.path if 'ALELT' not in p and 'ALELT' not in p]; "
            f"sys.path.insert(0, {str(target)!r}); import aleltd.sources as s, aleltd.daemon, alelt_common.canonical_jsonl; "
            "import importlib.util as u; assert u.find_spec('alelt_hub') is None; print('ok')")
    r = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, cwd=str(tmp_path))
    assert r.returncode == 0 and r.stdout.strip() == "ok", r.stderr

"""DEP-12: `*_FILE` secrets are visible to the hub process but never exported to the OS environment (children,
/proc/<pid>/environ, crash dumps of the environment block)."""
from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

from alelt_hub.cli import load_secret_files


def test_secret_file_read_in_process_not_exported(tmp_path: Path) -> None:
    name = f"ALELT_T{uuid.uuid4().hex[:8].upper()}_PASSWORD"
    f = tmp_path / "pw"
    f.write_text("s3cr3t-value\n", encoding="utf-8")
    os.environ[name + "_FILE"] = str(f)
    try:
        assert name in load_secret_files()
        assert os.environ.get(name) == "s3cr3t-value" and os.getenv(name) == "s3cr3t-value"
        out = subprocess.run([sys.executable, "-c", f"import os;print(os.environ.get('{name}'))"],
                             capture_output=True, text=True, check=True).stdout.strip()
        assert out == "None"                                  # child processes never inherit the value
        assert load_secret_files() == []                      # already set: not re-read
        env = {name + "_FILE": str(f)}                        # plain mapping (tests / tools): written normally
        assert load_secret_files(env) == [name] and env[name] == "s3cr3t-value"
    finally:
        os.environ.pop(name + "_FILE", None)
        os.environ.pop(name, None)

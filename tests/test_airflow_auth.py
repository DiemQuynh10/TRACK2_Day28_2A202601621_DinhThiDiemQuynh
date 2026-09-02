from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "prepare_airflow_auth.py"
PASSWORD_FILE_MODE = 0o644


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _assert_host_readable(path: Path) -> None:
    assert os.access(path, os.R_OK)
    if os.name != "nt":
        assert _mode(path) == PASSWORD_FILE_MODE


def _prepare(path: Path) -> None:
    subprocess.run(
        [sys.executable, str(SCRIPT), str(path)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def test_generated_airflow_password_is_readable_by_the_host_harness(
    tmp_path: Path,
) -> None:
    password_file = tmp_path / "airflow" / "simple-auth-passwords.json"

    _prepare(password_file)

    assert json.loads(password_file.read_text(encoding="utf-8"))["airflow"]
    _assert_host_readable(password_file)


def test_existing_password_is_preserved_and_its_mode_is_repaired(tmp_path: Path) -> None:
    password_file = tmp_path / "simple-auth-passwords.json"
    password_file.write_text('{"airflow": "keep-this-value"}', encoding="utf-8")
    password_file.chmod(0o600)

    _prepare(password_file)

    assert json.loads(password_file.read_text(encoding="utf-8")) == {
        "airflow": "keep-this-value"
    }
    _assert_host_readable(password_file)

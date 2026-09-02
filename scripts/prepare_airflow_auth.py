"""Create one repository-local Airflow development credential at runtime."""

from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path


def ensure_password_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        return
    path.write_text(
        json.dumps({"airflow": secrets.token_urlsafe(24)}),
        encoding="utf-8",
    )
    path.chmod(0o600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    arguments = parser.parse_args()
    ensure_password_file(arguments.path)

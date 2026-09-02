"""Create one repository-local Airflow development credential at runtime."""

from __future__ import annotations

import argparse
import json
import secrets
from pathlib import Path

PASSWORD_FILE_MODE = 0o644


def ensure_password_file(path: Path) -> None:
    """Create a local-only credential that both container and host can read.

    Airflow writes the file from a container UID while the integration harness
    reads it from the host UID.  The runtime directory is gitignored and the
    generated password is only for the local lab stack, so read access must not
    depend on the two processes sharing an operating-system user.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.is_file():
        path.write_text(
            json.dumps({"airflow": secrets.token_urlsafe(24)}),
            encoding="utf-8",
        )
    path.chmod(PASSWORD_FILE_MODE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    arguments = parser.parse_args()
    ensure_password_file(arguments.path)

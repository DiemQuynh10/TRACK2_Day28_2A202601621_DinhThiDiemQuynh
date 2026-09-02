#!/usr/bin/env python3
"""Reject host-specific assumptions from the supported student workflow."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_PATH = REPO_ROOT / "compose.yaml"
DOC_PATHS = tuple(
    REPO_ROOT / name
    for name in ("README.md", "LAB28.md", "LAB28_GUIDE.md", "SUBMISSION.md")
)
HOST_PATH = re.compile(r"^(?:/Users/|/home/|[A-Za-z]:[\\/]|~[/\\]|\$\{?HOME)")
UNSUPPORTED_DOC_PATTERNS = {
    "${PWD}": "Compose mounts must be relative, not expanded by a particular shell",
    "$PWD": "use repository-relative paths instead of POSIX shell expansion",
    "source .venv/bin/activate": "use `uv run`; activation is POSIX-only",
    ".venv/bin/": "use `uv run`; this path does not exist on Windows",
}


def _volume_source(volume: Any) -> str | None:
    if isinstance(volume, dict):
        return str(volume.get("source", "")) or None
    if not isinstance(volume, str):
        return None
    # Short syntax with a relative source. A Windows drive path is rejected
    # before this helper is useful, so splitting once is unambiguous here.
    return volume.split(":", 1)[0]


def verify() -> list[str]:
    failures: list[str] = []
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    for service_name, service in compose.get("services", {}).items():
        for volume in service.get("volumes", []) or []:
            source = _volume_source(volume)
            if source and HOST_PATH.match(source):
                failures.append(
                    f"compose service {service_name!r} uses host-specific volume source {source!r}"
                )
            if source and ("${PWD}" in source or source.startswith("$PWD")):
                failures.append(
                    f"compose service {service_name!r} depends on shell expansion in {source!r}"
                )

    for path in DOC_PATHS:
        text = path.read_text(encoding="utf-8")
        for pattern, reason in UNSUPPORTED_DOC_PATTERNS.items():
            if pattern in text:
                failures.append(f"{path.name}: found {pattern!r}; {reason}")
        for line_number, line in enumerate(text.splitlines(), 1):
            for token in re.findall(
                r"(?:/Users/\S+|/home/\S+|(?<![A-Za-z])[A-Za-z]:[\\/](?!/)\S+)", line
            ):
                failures.append(f"{path.name}:{line_number}: host-specific path {token!r}")
    return failures


def main() -> int:
    failures = verify()
    if failures:
        print("portability check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("OK    supported workflow is host-path and shell independent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

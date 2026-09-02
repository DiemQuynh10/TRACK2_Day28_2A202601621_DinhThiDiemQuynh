"""Shared fixtures for the fast suite.

Nothing here opens a socket. The whole point of this suite is that it proves
the contracts, the pure logic and the HTTP surface on a laptop with no stack
running — so a failure here is always a defect in this repository, never an
environment problem. Live proof lives in ``integration-tests/``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
MATRIX_PATH = REPO_ROOT / "contracts" / "integration-matrix.yaml"


@pytest.fixture(scope="session", autouse=True)
def offline_environment() -> None:
    """Make the suite deterministic and silent about missing collectors.

    Telemetry is disabled rather than pointed somewhere harmless: an enabled
    OTLP exporter with no collector spends the whole run retrying and printing
    connection errors that look like test failures.
    """
    os.environ.setdefault("LAB28_OTEL_ENABLED", "false")
    os.environ.setdefault("LAB28_TRACE_CONSOLE", "false")


@pytest.fixture(scope="session")
def matrix() -> dict[str, Any]:
    """The integration matrix, loaded from the repository rather than the CWD."""
    import yaml

    return yaml.safe_load(MATRIX_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Settings pointed at a scratch directory, built after the env is patched."""
    from lab28_platform.settings import Settings

    monkeypatch.setenv("LAB28_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("LAB28_DELTA_ROOT", str(tmp_path / "delta"))
    return Settings.from_env()

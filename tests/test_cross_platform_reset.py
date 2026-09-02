"""Reset stays repository-scoped and uses Python filesystem APIs."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from lab28_platform.cli import _remove_generated, app


def test_remove_generated_keeps_model_cache(tmp_path: Path) -> None:
    runtime = tmp_path / ".lab28"
    (runtime / "fastembed").mkdir(parents=True)
    (runtime / "delta").mkdir()
    (runtime / "mlflow.db").write_text("state", encoding="utf-8")

    removed = _remove_generated(runtime, keep=frozenset({"fastembed"}))

    assert runtime / "fastembed" in list(runtime.iterdir())
    assert not (runtime / "delta").exists()
    assert not (runtime / "mlflow.db").exists()
    assert {Path(item).name for item in removed} == {"delta", "mlflow.db"}


def test_reset_requires_explicit_confirmation() -> None:
    result = CliRunner().invoke(app, ["reset", "--no-containers"])

    assert result.exit_code == 1
    assert "run again with --yes" in result.output


@pytest.mark.parametrize("flag", ["--help", "reset --help"])
def test_cli_help_is_available_without_a_posix_shell(flag: str) -> None:
    result = CliRunner().invoke(app, flag.split())

    assert result.exit_code == 0

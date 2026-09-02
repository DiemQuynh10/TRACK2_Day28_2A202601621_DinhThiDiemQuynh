"""Verify that the published learner scaffold starts with four clear tasks."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "starter-tests", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout + result.stderr
    failure_lines = [line for line in output.splitlines() if line.startswith("FAILED ")]
    tasks = ROOT / "src" / "lab28_platform" / "integration_tasks.py"
    task_source = tasks.read_text(encoding="utf-8")

    if (
        result.returncode != 1
        or len(failure_lines) != 4
        or "4 failed" not in output
        or task_source.count("raise NotImplementedError") != 4
    ):
        print(output)
        raise SystemExit(
            "expected the published scaffold to expose exactly four unfinished tasks"
        )

    print("published scaffold verified: exactly four guided tasks remain")


if __name__ == "__main__":
    main()

"""Windows, macOS and Linux share one supported command surface."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CHECKER = REPO_ROOT / "scripts" / "check_portability.py"


def test_student_workflow_has_no_host_specific_paths() -> None:
    spec = importlib.util.spec_from_file_location("check_portability", CHECKER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    assert module.verify() == []

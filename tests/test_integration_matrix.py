"""The matrix must keep describing this repository, checked on every run.

``scripts/verify_matrix.py`` exists so a person can run the check; this module
exists so nobody has to remember to. The contract in
``contracts/integration-matrix.yaml`` is read by the readiness report, the live
suite and the demo rubric, and it drifts silently — a renamed span or a moved
journey module breaks nothing until the demo.

The failure message is the verifier's own, so a red run here says exactly which
cross-reference broke rather than "the matrix is wrong".
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
VERIFIER_PATH = REPO_ROOT / "scripts" / "verify_matrix.py"


def load_verifier() -> ModuleType:
    """Import the script by path; ``scripts/`` is a directory, not a package.

    The module is registered before it executes because ``@dataclass`` resolves
    its annotations through ``sys.modules[cls.__module__]``, which does not exist
    yet during ``exec_module``.
    """
    spec = importlib.util.spec_from_file_location("verify_matrix", VERIFIER_PATH)
    assert spec and spec.loader, VERIFIER_PATH
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def verifier() -> ModuleType:
    return load_verifier()


def test_the_matrix_agrees_with_the_repository(verifier: ModuleType) -> None:
    checks, failures = verifier.verify()

    assert not failures, "\n".join(failures)
    assert checks > 0, "the verifier ran no checks, which proves nothing"


def test_the_verifier_notices_a_renamed_span(
    verifier: ModuleType, matrix: dict[str, Any], tmp_path: Path
) -> None:
    """The drift this whole file exists to catch, exercised once on purpose.

    A checker that always passes is indistinguishable from no checker, so the
    negative case is asserted rather than assumed.
    """
    import yaml

    drifted = dict(matrix)
    drifted["required_spans"] = ["lab28.gateway.renamed", *matrix["required_spans"][1:]]
    path = tmp_path / "drifted-matrix.yaml"
    path.write_text(yaml.safe_dump(drifted), encoding="utf-8")

    _, failures = verifier.verify(path)

    assert any("lab28.gateway.renamed" in failure for failure in failures), failures

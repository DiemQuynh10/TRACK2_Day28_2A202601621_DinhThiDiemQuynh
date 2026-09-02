from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pyarrow import parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "create_feature_snapshot.py"
SPEC = importlib.util.spec_from_file_location("create_feature_snapshot", SCRIPT)
assert SPEC and SPEC.loader
SNAPSHOT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SNAPSHOT
SPEC.loader.exec_module(SNAPSHOT)

FEATURE_SCHEMA = SNAPSHOT.FEATURE_SCHEMA
create_snapshot = SNAPSHOT.create_snapshot


def test_bootstrap_snapshot_has_the_serving_schema(tmp_path: Path) -> None:
    destination = tmp_path / "asker_activity"

    part = create_snapshot(destination)

    assert part.parent == destination
    assert pq.read_schema(part) == FEATURE_SCHEMA
    assert pq.read_table(destination).num_rows == 0


def test_bootstrap_does_not_overwrite_an_existing_export(tmp_path: Path) -> None:
    destination = tmp_path / "asker_activity"
    first = create_snapshot(destination)
    original = first.read_bytes()

    assert create_snapshot(destination) == first
    assert first.read_bytes() == original

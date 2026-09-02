"""Create the schema-only Feast source used before the first pipeline run.

Feast validates a ``FileSource`` while applying the feature repository.  On a
brand-new checkout the Spark export does not exist yet, so the feature server
would fail before Airflow had a chance to produce it.  This script creates one
empty Parquet part with the exact serving schema.  Spark later replaces the
directory atomically with real rows.

The script intentionally uses only ``pathlib`` and PyArrow.  It runs inside the
Feast container and behaves the same on Windows, macOS and Linux hosts.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet as pq

FEATURE_SCHEMA = pa.schema(
    [
        pa.field("asker_id", pa.string(), nullable=False),
        pa.field("feedback_count", pa.int64(), nullable=False),
        pa.field("avg_rating", pa.float64(), nullable=False),
        pa.field("negative_ratio", pa.float64(), nullable=False),
        pa.field("delta_version", pa.int64(), nullable=False),
        pa.field("event_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("created", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def create_snapshot(destination: Path) -> Path:
    """Create an empty Parquet dataset unless Spark has already exported one."""
    existing_parts = tuple(destination.glob("*.parquet")) if destination.exists() else ()
    if existing_parts:
        return existing_parts[0]

    destination.mkdir(parents=True, exist_ok=True)
    part = destination / "part-00000.parquet"
    table = pa.Table.from_arrays(
        [pa.array([], type=field.type) for field in FEATURE_SCHEMA],
        schema=FEATURE_SCHEMA,
    )
    pq.write_table(table, part)
    return part


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "destination",
        type=Path,
        nargs="?",
        default=Path(".lab28/delta/exports/asker_activity"),
    )
    args = parser.parse_args()
    print(create_snapshot(args.destination))


if __name__ == "__main__":
    main()

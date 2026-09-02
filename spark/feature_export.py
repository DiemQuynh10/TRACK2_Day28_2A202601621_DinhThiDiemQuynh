"""Lakehouse → feature store: the offline snapshot Feast materializes (IP04).

Feast does not read Delta here. It reads a parquet snapshot that this job
derives from the Delta feedback table, and the reason is worth stating because
it is the seam students most often get backwards: **the feature definition and
the feature computation are different artifacts**. ``feature-repo/`` declares
what ``avg_rating`` *is* and how long it stays fresh; this job decides what it
*currently equals*, from one named Delta version.

Stamping that version into every row is the whole point. When the serving
evidence says "answered with feature set at delta_version 7", the claim can be
checked by re-running this aggregation against version 7 and comparing — which
is not possible if the features were computed from "whatever was in the table
at the time".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

#: A rating at or below this counts as negative feedback. It is a product
#: decision, not a statistical one, so it lives in one named place rather than
#: inline in the SQL where a reviewer would have to reverse-engineer it.
NEGATIVE_RATING_THRESHOLD = 2

#: Feast's ``timestamp_field``. The name is part of the contract with
#: ``feature-repo/definitions.py`` — renaming it here makes every online lookup
#: return NOT_FOUND with no other symptom.
EVENT_TIMESTAMP_COLUMN = "event_timestamp"


@dataclass(frozen=True)
class ExportResult:
    """What the snapshot contains, for the DAG log and the evidence pack."""

    path: str
    entities: int
    delta_version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "entities": self.entities,
            "delta_version": self.delta_version,
        }


def aggregation_sql(source: str, delta_version: int) -> str:
    """The feature computation, as one auditable statement.

    Reading the source ``VERSION AS OF`` a pinned number rather than "now" is
    what makes the export reproducible: run this twice against the same version
    and the rows are identical, even if Kafka delivered more feedback in
    between.
    """
    return f"""
        SELECT
          asker_id,
          CAST(COUNT(*) AS BIGINT) AS feedback_count,
          CAST(AVG(rating) AS DOUBLE) AS avg_rating,
          CAST(
            SUM(CASE WHEN rating <= {NEGATIVE_RATING_THRESHOLD} THEN 1 ELSE 0 END)
              / COUNT(*) AS DOUBLE
          ) AS negative_ratio,
          CAST({delta_version} AS BIGINT) AS delta_version,
          MAX(occurred_at) AS {EVENT_TIMESTAMP_COLUMN},
          CURRENT_TIMESTAMP() AS created
        FROM delta.`{source}` VERSION AS OF {delta_version}
        GROUP BY asker_id
    """


def export_asker_features(
    spark: SparkSession, source: str, destination: str, delta_version: int
) -> ExportResult:
    """Write the per-asker feature snapshot Feast will materialize.

    The write is an overwrite, not an append: this is a *snapshot* of the whole
    entity population at one Delta version, and Feast picks the latest row per
    entity anyway. Appending would grow the file without changing an answer.
    """
    spark.sql(aggregation_sql(source, delta_version)).write.mode("overwrite").parquet(destination)

    result = ExportResult(
        path=destination,
        # Counted by reading the file back, not by re-running the aggregation:
        # it is one cheap metadata scan and it proves the write landed.
        entities=spark.read.parquet(destination).count(),
        delta_version=delta_version,
    )
    logger.info(
        "exported %s asker rows from delta version %s to %s",
        result.entities,
        delta_version,
        destination,
    )
    return result

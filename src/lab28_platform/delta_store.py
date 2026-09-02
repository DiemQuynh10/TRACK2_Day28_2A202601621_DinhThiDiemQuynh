"""Reading and proving the Delta lakehouse (IP03).

Writes belong to Spark — a MERGE with schema enforcement is a JVM job and lives
in ``spark/``. The statements it executes are declared *here*, next to the row
shapers they have to agree with. This module is the *read and evidence* side as
well: it opens the same
transaction log from Python with ``deltalake`` (no JVM), so the API, the
readiness report and the tests can answer three questions cheaply:

* what version is the table at right now,
* what did the table look like at an earlier version,
* and does the commit history show the operations we claim it does.

Binding the serving evidence to a concrete Delta version is the point. "The
model answered using data version 7" is checkable months later; "the model used
the latest data" is not.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from deltalake import DeltaTable
from deltalake.exceptions import DeltaError, TableNotFoundError

from lab28_platform import integration_tasks, metrics
from lab28_platform.contracts import IngestionEvent

#: The column the Spark MERGE matches on.
#:
#: Both sides of IP03 have to agree on this exact field: the consumer that
#: prepares a batch and the job that writes it. Deduplicating on anything else —
#: ``event_id``, an arrival timestamp, the row payload — makes a replay append a
#: second row that differs only by metadata, and the idempotency journey fails.
MERGE_KEY = "idempotency_key"


#: Canonical column definitions for the two Delta tables, in commit order.
#:
#: Spark owns the write, so the schema has to be written in something Spark can
#: execute — hence Spark SQL DDL fragments rather than a PyArrow or Delta-RS
#: schema object. It lives here, beside the row shapers, because a column added
#: to ``feedback_rows`` but not to the table is precisely the drift that Delta's
#: schema enforcement would otherwise surface as a failed Airflow task at 3am.
#: ``UT-delta-merge-idempotency`` asserts the two stay in step, without a JVM.
#:
#: ``NOT NULL`` is not decoration: it is the half of schema enforcement that a
#: demo can actually show failing. A null ``idempotency_key`` matches no target
#: row, so a MERGE would append it again on every replay — the one outcome IP03
#: exists to rule out. Every column marked nullable here is genuinely optional
#: in ``contracts.FeedbackPayload`` / ``contracts.DocumentPayload``.
FEEDBACK_SCHEMA: tuple[tuple[str, str], ...] = (
    (MERGE_KEY, "STRING NOT NULL"),
    ("event_id", "STRING NOT NULL"),
    ("asker_id", "STRING NOT NULL"),
    ("text", "STRING NOT NULL"),
    ("rating", "INT NOT NULL"),
    ("locale", "STRING NOT NULL"),
    ("label", "STRING"),
    ("occurred_at", "TIMESTAMP NOT NULL"),
    ("traceparent", "STRING"),
)

DOCUMENT_SCHEMA: tuple[tuple[str, str], ...] = (
    (MERGE_KEY, "STRING NOT NULL"),
    ("event_id", "STRING NOT NULL"),
    ("doc_id", "STRING NOT NULL"),
    ("title", "STRING NOT NULL"),
    ("text", "STRING NOT NULL"),
    ("locale", "STRING NOT NULL"),
    ("tags", "ARRAY<STRING> NOT NULL"),
    ("occurred_at", "TIMESTAMP NOT NULL"),
    ("traceparent", "STRING"),
)

#: The tables the pipeline writes and the serving path reads, by logical name.
TABLE_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "feedback": FEEDBACK_SCHEMA,
    "documents": DOCUMENT_SCHEMA,
}


def column_names(schema: tuple[tuple[str, str], ...]) -> list[str]:
    """The column names of a schema, in declared order."""
    return [name for name, _ in schema]


def _columns(table: str) -> list[str]:
    return [f"{name} {type_}" for name, type_ in TABLE_SCHEMAS[table]]


def schema_ddl(table: str) -> str:
    """The schema as a one-line DDL string, for ``spark.createDataFrame``.

    The batch is built with the *same* declaration the table was created from,
    so a column whose type drifted fails when the DataFrame is constructed —
    with the column named — instead of inside the MERGE as an analysis error
    about a plan the student never wrote.
    """
    return ", ".join(_columns(table))


def create_table_ddl(table: str, location: str) -> str:
    """The ``CREATE TABLE`` the Spark job runs before its first MERGE.

    The table is addressed by *path* (``delta.`/some/path```) rather than by a
    catalog name. That is deliberate: a name would live in the Spark container's
    metastore, so the table would vanish on the next ``compose down`` while its
    files stayed on disk, and nothing else in this lab addresses a table that
    way — ``deltalake``, the CLI, the readiness report and the evidence pack all
    open a URI. One addressing scheme, no hidden state.

    ``IF NOT EXISTS`` is what makes the job re-runnable: the first run lays down
    the schema, every later run adopts the existing transaction log instead of
    starting a new one and losing the version history the evidence pack cites.
    """
    columns = ",\n  ".join(_columns(table))
    return f"CREATE TABLE IF NOT EXISTS delta.`{location}` (\n  {columns}\n) USING DELTA"


def merge_sql(location: str, source_view: str) -> str:
    """The MERGE that makes a replayed batch a no-op instead of a duplicate.

    This single statement is IP03's idempotency guarantee: matching on
    ``idempotency_key`` means a redelivered event updates the row it wrote last
    time. An ``INSERT``-only writer would pass every unit test in this repo and
    still double the table on the first Kafka redelivery.

    ``UPDATE SET *`` / ``INSERT *`` resolve by column name, so a source missing a
    column fails here rather than silently writing nulls — which is the schema
    enforcement half of the same statement.
    """
    return (
        f"MERGE INTO delta.`{location}` AS target\n"
        f"USING {source_view} AS source\n"
        f"ON target.{MERGE_KEY} = source.{MERGE_KEY}\n"
        "WHEN MATCHED THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


class LakehouseUnavailable(RuntimeError):
    """The Delta table is missing or its log cannot be read."""


@dataclass(frozen=True)
class TableSnapshot:
    """A point-in-time view of one Delta table."""

    table: str
    uri: str
    version: int
    rows: int
    files: int
    schema: dict[str, str]
    last_operation: str | None
    last_commit_ts: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "uri": self.uri,
            "version": self.version,
            "rows": self.rows,
            "files": self.files,
            "columns": sorted(self.schema),
            "last_operation": self.last_operation,
            "last_commit_ts": (
                self.last_commit_ts.isoformat() if self.last_commit_ts else None
            ),
        }


def _commit_timestamp(entry: dict[str, Any]) -> datetime | None:
    raw = entry.get("timestamp")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    try:
        # The log stores epoch milliseconds.
        return datetime.fromtimestamp(int(raw) / 1000, tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


# --------------------------------------------------------------------------
# Merge input: what the writer is handed
# --------------------------------------------------------------------------


def dedupe_events(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Collapse a batch to one event per merge key, latest occurrence winning.

    A Delta MERGE raises when the source contains two rows matching the same
    target row, so the batch has to be unique on ``idempotency_key`` *before* it
    reaches the writer. Doing it here rather than inside the job keeps the rule
    testable without a JVM, and keeps both readers of this contract honest.

    Ties on ``occurred_at`` are broken by ``event_id`` so the result depends on
    the batch contents and not on the order Kafka happened to deliver them.
    """
    return integration_tasks.dedupe_latest(events)


def feedback_rows(events: Iterable[IngestionEvent]) -> list[dict[str, Any]]:
    """Shape deduplicated feedback events into Delta ``feedback`` rows."""
    return [
        {
            MERGE_KEY: event.idempotency_key,
            "event_id": event.event_id,
            "asker_id": event.payload.asker_id,
            "text": event.payload.text,
            "rating": event.payload.rating,
            "locale": event.payload.locale,
            "label": event.payload.label,
            "occurred_at": event.occurred_at,
            "traceparent": event.traceparent,
        }
        for event in dedupe_events(events)
        if event.kind == "feedback"
    ]


def document_rows(events: Iterable[IngestionEvent]) -> list[dict[str, Any]]:
    """Shape deduplicated document events into Delta ``documents`` rows."""
    return [
        {
            MERGE_KEY: event.idempotency_key,
            "event_id": event.event_id,
            "doc_id": event.payload.doc_id,
            "title": event.payload.title,
            "text": event.payload.text,
            "locale": event.payload.locale,
            "tags": list(event.payload.tags),
            "occurred_at": event.occurred_at,
            "traceparent": event.traceparent,
        }
        for event in dedupe_events(events)
        if event.kind == "document"
    ]


# --------------------------------------------------------------------------
# Reading and proving
# --------------------------------------------------------------------------


def open_table(uri: str, *, version: int | None = None) -> DeltaTable:
    """Open a Delta table, optionally pinned to an earlier version."""
    try:
        table = DeltaTable(uri)
    except TableNotFoundError as error:
        raise LakehouseUnavailable(f"no Delta table at {uri}") from error
    except DeltaError as error:
        raise LakehouseUnavailable(f"cannot read Delta log at {uri}: {error}") from error
    if version is not None:
        try:
            table.load_as_version(version)
        except DeltaError as error:
            raise LakehouseUnavailable(
                f"version {version} is not available at {uri}"
            ) from error
    return table


def snapshot(name: str, uri: str, *, version: int | None = None) -> TableSnapshot:
    """Describe a table at a version, and publish its version as a metric."""
    table = open_table(uri, version=version)
    history = table.history(1)
    latest = history[0] if history else {}
    resolved = table.version()
    snap = TableSnapshot(
        table=name,
        uri=uri,
        version=resolved,
        rows=table.to_pyarrow_dataset().count_rows(),
        files=len(table.file_uris()),
        schema={field.name: str(field.type) for field in table.schema().fields},
        last_operation=latest.get("operation"),
        last_commit_ts=_commit_timestamp(latest),
    )
    if version is None:
        metrics.DELTA_VERSION.labels(table=name).set(resolved)
    return snap


def current_version(uri: str) -> int:
    """The version the serving path should stamp into its evidence."""
    return open_table(uri).version()


def read_rows(
    uri: str,
    *,
    version: int | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Materialise rows as plain dicts, optionally from an earlier version."""
    table = open_table(uri, version=version)
    arrow = table.to_pyarrow_table(columns=columns)
    if limit is not None:
        arrow = arrow.slice(0, limit)
    return arrow.to_pylist()


def commit_history(uri: str, *, limit: int = 20) -> list[dict[str, Any]]:
    """The transaction log, trimmed to what the demo and the tests assert on."""
    entries = open_table(uri).history(limit)
    return [
        {
            "version": entry.get("version"),
            "operation": entry.get("operation"),
            "timestamp": (
                ts.isoformat() if (ts := _commit_timestamp(entry)) is not None else None
            ),
            "metrics": entry.get("operationMetrics") or {},
        }
        for entry in entries
    ]


def time_travel_evidence(name: str, uri: str) -> dict[str, Any]:
    """Compare the oldest retained version with the current one.

    This is what makes time travel demonstrable rather than asserted: two row
    counts read from the same URI, differing only by the version requested.
    """
    table = open_table(uri)
    history = table.history()
    if not history:
        raise LakehouseUnavailable(f"{uri} has no commit history")
    versions = sorted(entry["version"] for entry in history if "version" in entry)
    earliest, latest = versions[0], versions[-1]
    return {
        "table": name,
        "earliest_version": earliest,
        "latest_version": latest,
        "earliest_rows": snapshot(name, uri, version=earliest).rows,
        "latest_rows": snapshot(name, uri, version=latest).rows,
        "operations": [entry.get("operation") for entry in history],
    }


def health(tables: dict[str, str]) -> dict[str, Any]:
    """Readiness for every table the serving path depends on."""
    report: dict[str, Any] = {"reachable": True, "tables": {}}
    for name, uri in tables.items():
        try:
            report["tables"][name] = snapshot(name, uri).to_dict()
        except LakehouseUnavailable as error:
            report["reachable"] = False
            report["tables"][name] = {"uri": uri, "error": str(error)}
    return report

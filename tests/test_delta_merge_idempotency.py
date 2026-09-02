"""UT-delta-merge-idempotency — the batch handed to the Spark MERGE (IP03).

A Delta MERGE fails outright when two source rows match the same target row, so
"replaying a batch is safe" is decided here, before any JVM is involved. The
live proof that the table itself stays at one row per key is IT-J2.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from lab28_platform.contracts import DocumentPayload, FeedbackPayload, IngestionEvent
from lab28_platform.delta_store import (
    DOCUMENT_SCHEMA,
    FEEDBACK_SCHEMA,
    MERGE_KEY,
    TABLE_SCHEMAS,
    column_names,
    create_table_ddl,
    dedupe_events,
    document_rows,
    feedback_rows,
    merge_sql,
)

pytestmark = pytest.mark.matrix("UT-delta-merge-idempotency")

BASE = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def feedback(key: str, *, rating: int = 5, offset_seconds: int = 0) -> IngestionEvent:
    return IngestionEvent(
        idempotency_key=key,
        entity_id="asker-1",
        occurred_at=BASE + timedelta(seconds=offset_seconds),
        payload=FeedbackPayload(asker_id="asker-1", text="Dịch vụ rất tốt", rating=rating),
    )


def document(key: str, *, doc_id: str = "policy-1") -> IngestionEvent:
    return IngestionEvent(
        idempotency_key=key,
        entity_id=doc_id,
        occurred_at=BASE,
        payload=DocumentPayload(
            doc_id=doc_id,
            title="Chính sách hoàn tiền",
            text="Khách hàng có thể yêu cầu hoàn tiền trong vòng 14 ngày kể từ khi mua.",
            tags=["policy", "refund"],
        ),
    )


class TestDeduplication:
    def test_a_replayed_batch_collapses_to_one_row_per_key(self) -> None:
        batch = [feedback("fb:a"), feedback("fb:b")]

        assert len(dedupe_events(batch + batch)) == 2

    def test_distinct_keys_all_survive(self) -> None:
        merged = dedupe_events([feedback(f"fb:{n}") for n in range(5)])

        assert {event.idempotency_key for event in merged} == {f"fb:{n}" for n in range(5)}

    def test_the_latest_occurrence_wins(self) -> None:
        """A correction redelivered under the same key must replace, not append."""
        winner = dedupe_events(
            [
                feedback("fb:a", rating=1, offset_seconds=0),
                feedback("fb:a", rating=4, offset_seconds=30),
            ]
        )

        assert len(winner) == 1
        assert winner[0].payload.rating == 4

    def test_the_result_does_not_depend_on_delivery_order(self) -> None:
        """Kafka orders per partition, not per batch; the merge input must not care."""
        early = feedback("fb:a", rating=1, offset_seconds=0)
        late = feedback("fb:a", rating=4, offset_seconds=30)

        assert dedupe_events([early, late]) == dedupe_events([late, early])

    def test_events_sharing_a_timestamp_resolve_deterministically(self) -> None:
        first = feedback("fb:a", rating=2)
        second = feedback("fb:a", rating=3)

        assert dedupe_events([first, second]) == dedupe_events([second, first])

    def test_an_empty_batch_is_not_an_error(self) -> None:
        assert dedupe_events([]) == []


class TestRowShaping:
    def test_feedback_and_documents_are_routed_to_their_own_tables(self) -> None:
        batch = [feedback("fb:a"), document("doc:a")]

        assert [row["asker_id"] for row in feedback_rows(batch)] == ["asker-1"]
        assert [row["doc_id"] for row in document_rows(batch)] == ["policy-1"]

    def test_every_row_carries_the_merge_key(self) -> None:
        rows = feedback_rows([feedback("fb:a")]) + document_rows([document("doc:a")])

        assert all(row[MERGE_KEY] for row in rows)

    def test_rows_are_unique_on_the_merge_key_after_a_replay(self) -> None:
        batch = [feedback("fb:a"), document("doc:a")]
        rows = feedback_rows(batch * 3) + document_rows(batch * 3)
        keys = [row[MERGE_KEY] for row in rows]

        assert len(keys) == len(set(keys)) == 2

    def test_trace_context_survives_into_the_lakehouse(self) -> None:
        """IP10 needs the trace id to still be attached after the write."""
        traced = feedback("fb:a")
        traced = traced.model_copy(
            update={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
        )

        assert feedback_rows([traced])[0]["traceparent"] == traced.traceparent

    def test_row_timestamps_stay_timezone_aware(self) -> None:
        assert feedback_rows([feedback("fb:a")])[0]["occurred_at"].tzinfo is not None


class TestSchemaAgreement:
    """The declared table and the rows the writer builds must not drift apart.

    Delta enforces the schema on write, so a mismatch here is a failed Spark
    job in the Airflow log. Catching it in the fast suite is the difference
    between a one-line diff and a stack trace from a JVM the student cannot
    reach.
    """

    def test_feedback_rows_fill_exactly_the_declared_columns(self) -> None:
        assert list(feedback_rows([feedback("fb:a")])[0]) == column_names(FEEDBACK_SCHEMA)

    def test_document_rows_fill_exactly_the_declared_columns(self) -> None:
        assert list(document_rows([document("doc:a")])[0]) == column_names(DOCUMENT_SCHEMA)

    @pytest.mark.parametrize("table", sorted(TABLE_SCHEMAS))
    def test_every_table_merges_on_the_same_key(self, table: str) -> None:
        assert column_names(TABLE_SCHEMAS[table])[0] == MERGE_KEY

    @pytest.mark.parametrize("table", sorted(TABLE_SCHEMAS))
    def test_the_merge_key_cannot_be_null(self, table: str) -> None:
        """A null key matches no target row, so a MERGE would append forever."""
        assert dict(TABLE_SCHEMAS[table])[MERGE_KEY].endswith("NOT NULL")

    def test_a_well_formed_event_populates_every_non_null_column(self) -> None:
        row = feedback_rows([feedback("fb:a")])[0]
        required = [
            name for name, type_ in FEEDBACK_SCHEMA if type_.endswith("NOT NULL")
        ]

        assert [name for name in required if row[name] is None] == []

    def test_the_ddl_declares_the_table_at_its_path(self) -> None:
        ddl = create_table_ddl("feedback", ".lab28/delta/feedback")

        assert ddl.startswith("CREATE TABLE IF NOT EXISTS delta.`.lab28/delta/feedback` (")
        assert ddl.endswith("USING DELTA")
        assert f"{MERGE_KEY} STRING NOT NULL" in ddl


class TestMergeStatement:
    """The one statement that decides whether a replay duplicates the table."""

    def test_the_merge_matches_on_the_merge_key(self) -> None:
        sql = merge_sql(".lab28/delta/feedback", "batch")

        assert f"ON target.{MERGE_KEY} = source.{MERGE_KEY}" in sql

    def test_a_matched_row_is_updated_rather_than_appended(self) -> None:
        sql = merge_sql(".lab28/delta/feedback", "batch")

        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "INSERT INTO" not in sql

    def test_the_merge_targets_the_same_path_the_readers_open(self) -> None:
        """The writer and ``open_table`` must not disagree about the URI."""
        assert "delta.`.lab28/delta/feedback`" in merge_sql(".lab28/delta/feedback", "batch")

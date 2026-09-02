"""UT-contracts-ingestion — the HTTP and Kafka boundary of IP01.

These are the guarantees every other integration point inherits: an event that
survives this module is one the pipeline, the lakehouse and the vector store can
all trust to be uniquely identified and version-tagged.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from lab28_platform.contracts import (
    INGESTION_SCHEMA_VERSION,
    TOPIC_DATA_RAW,
    TOPICS,
    TOPICS_BY_NAME,
    DocumentPayload,
    DocumentSubmission,
    FeedbackPayload,
    FeedbackSubmission,
    IngestionEvent,
    content_hash,
)

pytestmark = pytest.mark.matrix("UT-contracts-ingestion")

VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def feedback_event(**overrides: object) -> IngestionEvent:
    fields: dict[str, object] = {
        "idempotency_key": "fb:asker-1:abc",
        "entity_id": "asker-1",
        "payload": FeedbackPayload(asker_id="asker-1", text="Dịch vụ rất tốt", rating=5),
    }
    fields.update(overrides)
    return IngestionEvent(**fields)  # type: ignore[arg-type]


class TestSchemaVersioning:
    """A consumer must be able to reject a payload it does not understand."""

    def test_event_carries_its_schema_version(self) -> None:
        assert feedback_event().schema_version == INGESTION_SCHEMA_VERSION

    def test_a_future_schema_version_is_rejected_not_coerced(self) -> None:
        body = json.loads(feedback_event().model_dump_json())
        body["schema_version"] = "2"

        with pytest.raises(ValidationError) as caught:
            IngestionEvent.model_validate(body)

        assert "schema_version" in str(caught.value)

    def test_an_unknown_field_is_rejected(self) -> None:
        """``extra="forbid"`` is what stops a silent producer/consumer drift."""
        body = json.loads(feedback_event().model_dump_json())
        body["priority"] = "high"

        with pytest.raises(ValidationError):
            IngestionEvent.model_validate(body)


class TestIdentity:
    def test_two_events_get_distinct_event_ids(self) -> None:
        assert feedback_event().event_id != feedback_event().event_id

    def test_the_idempotency_key_is_the_stable_identity(self) -> None:
        """Distinct deliveries of one fact share a key; that is the dedup unit."""
        first = feedback_event()
        second = feedback_event()

        assert first.event_id != second.event_id
        assert first.idempotency_key == second.idempotency_key

    def test_a_derived_key_is_content_addressed(self) -> None:
        from lab28_platform.api import _derive_key

        same = _derive_key("fb", "asker-1", "Dịch vụ rất tốt")
        again = _derive_key("fb", "asker-1", "Dịch vụ rất tốt")
        different_text = _derive_key("fb", "asker-1", "Dịch vụ chậm")
        different_asker = _derive_key("fb", "asker-2", "Dịch vụ rất tốt")

        assert same == again
        assert different_text != same
        assert different_asker != same

    def test_the_derived_key_never_embeds_the_user_text(self) -> None:
        from lab28_platform.api import _derive_key

        text = "Số điện thoại của tôi là 0912345678"
        key = _derive_key("fb", "asker-1", text)

        assert text not in key
        assert key.endswith(content_hash(text)[:32])

    def test_an_identifier_with_a_separator_is_rejected(self) -> None:
        """Keys travel in Kafka message keys and Delta predicates unquoted."""
        with pytest.raises(ValidationError):
            feedback_event(entity_id="asker 1/../etc")


class TestTraceContext:
    def test_a_well_formed_traceparent_is_accepted(self) -> None:
        assert feedback_event(traceparent=VALID_TRACEPARENT).traceparent == VALID_TRACEPARENT

    def test_trace_context_is_optional(self) -> None:
        assert feedback_event().traceparent is None

    @pytest.mark.parametrize(
        "bad",
        [
            "4bf92f3577b34da6a3ce929d0e0e4736",  # no version field
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",  # no flags
            "00-XXXX2f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",  # not hex
        ],
    )
    def test_a_malformed_traceparent_is_rejected(self, bad: str) -> None:
        """A broken traceparent breaks IP10; it must fail at the edge."""
        with pytest.raises(ValidationError):
            feedback_event(traceparent=bad)


class TestPayloadDiscrimination:
    def test_the_payload_kind_selects_the_model(self) -> None:
        document = IngestionEvent(
            idempotency_key="doc:policy-1:abc",
            entity_id="policy-1",
            payload=DocumentPayload(
                doc_id="policy-1",
                title="Chính sách hoàn tiền",
                text="Khách hàng có thể yêu cầu hoàn tiền trong vòng 14 ngày.",
            ),
        )

        assert document.kind == "document"
        assert feedback_event().kind == "feedback"

    def test_a_round_trip_preserves_the_payload_type(self) -> None:
        restored = IngestionEvent.model_validate_json(feedback_event().model_dump_json())

        assert isinstance(restored.payload, FeedbackPayload)

    def test_an_unknown_kind_is_rejected(self) -> None:
        body = json.loads(feedback_event().model_dump_json())
        body["payload"]["kind"] = "telemetry"

        with pytest.raises(ValidationError):
            IngestionEvent.model_validate(body)


class TestSubmissionBounds:
    def test_a_rating_outside_one_to_five_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeedbackSubmission(asker_id="asker-1", text="Quá ngắn nhưng đủ", rating=6)

    def test_text_shorter_than_the_minimum_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            FeedbackSubmission(asker_id="asker-1", text="ok", rating=5)

    def test_surrounding_whitespace_is_stripped_before_hashing(self) -> None:
        """Otherwise the same feedback pasted twice produces two dedup keys."""
        padded = FeedbackSubmission(asker_id="asker-1", text="  Dịch vụ rất tốt  ", rating=5)

        assert padded.text == "Dịch vụ rất tốt"

    def test_a_document_carries_at_most_ten_tags(self) -> None:
        with pytest.raises(ValidationError):
            DocumentSubmission(
                doc_id="policy-1",
                title="Chính sách hoàn tiền",
                text="Khách hàng có thể yêu cầu hoàn tiền trong vòng 14 ngày kể từ khi mua.",
                tags=[f"tag-{index}" for index in range(11)],
            )


class TestTopicRegistry:
    def test_every_topic_is_declared_once(self) -> None:
        names = [topic.name for topic in TOPICS]

        assert len(names) == len(set(names))
        assert set(TOPICS_BY_NAME) == set(names)

    def test_the_raw_topic_is_partitioned_and_retained(self) -> None:
        raw = TOPICS_BY_NAME[TOPIC_DATA_RAW]

        assert raw.partitions > 1, "ordering is per-key, so IP01 needs room to scale"
        assert raw.config["retention.ms"] == str(raw.retention_ms)

    def test_the_model_topic_is_compacted(self) -> None:
        """Release history is a changelog of one key; compaction keeps it bounded."""
        from lab28_platform.contracts import TOPIC_MODEL_EVENTS

        assert TOPICS_BY_NAME[TOPIC_MODEL_EVENTS].cleanup_policy == "compact"

    def test_every_topic_names_a_role_the_matrix_declares(
        self, matrix: dict[str, object]
    ) -> None:
        """A topic owned by a team the rubric never names has no one to fix it."""
        roles = set(matrix["roles"])  # type: ignore[arg-type]
        unowned = {topic.name: topic.owner for topic in TOPICS if topic.owner not in roles}

        assert unowned == {}


def test_occurred_at_is_timezone_aware() -> None:
    """Naive timestamps make Delta time travel and freshness meaningless."""
    occurred = feedback_event().occurred_at

    assert occurred.tzinfo is not None
    assert occurred <= datetime.now(UTC)

from datetime import UTC, datetime, timedelta

from lab28_platform.contracts import FeedbackPayload, IngestionEvent
from lab28_platform.integration_tasks import (
    dedupe_latest,
    event_headers,
    feast_online_request,
    readiness_status,
)

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def _feedback(key: str, *, seconds: int, rating: int) -> IngestionEvent:
    return IngestionEvent(
        event_id=f"event-{seconds:04d}-{rating}",
        idempotency_key=key,
        entity_id="student-7",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds),
        payload=FeedbackPayload(
            asker_id="student-7", text="Dịch vụ đủ dài để kiểm thử", rating=rating
        ),
    )


def test_event_headers_preserve_trace_and_idempotency() -> None:
    headers = dict(event_headers(TRACEPARENT, "feedback:42"))
    assert headers == {
        "traceparent": TRACEPARENT.encode(),
        "idempotency-key": b"feedback:42",
    }
    assert dict(event_headers(None, "feedback:42")) == {
        "idempotency-key": b"feedback:42"
    }


def test_delta_source_is_replay_safe_and_newest_wins() -> None:
    early = _feedback("a", seconds=1, rating=1)
    late = _feedback("a", seconds=2, rating=5)
    other = _feedback("b", seconds=1, rating=3)
    assert dedupe_latest([late, early, other, late]) == [late, other]
    assert dedupe_latest([other, late, early]) == [late, other]


def test_feast_request_matches_the_registry() -> None:
    request = feast_online_request("student-7")
    assert request["entities"] == {"asker_id": ["student-7"]}
    assert request["features"] == [
        "asker_activity_v1:feedback_count",
        "asker_activity_v1:avg_rating",
        "asker_activity_v1:negative_ratio",
        "asker_activity_v1:delta_version",
    ]
    assert request["full_feature_names"] is False


def test_readiness_distinguishes_failure_severity() -> None:
    assert readiness_status([{"ready": True, "mandatory": True}]) == "ready"
    assert readiness_status([{"ready": False, "mandatory": False}]) == "degraded"
    assert readiness_status([{"ready": False, "mandatory": True}]) == "not_ready"

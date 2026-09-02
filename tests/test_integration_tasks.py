from lab28_platform.integration_tasks import (
    dedupe_latest,
    event_headers,
    feast_online_request,
    readiness_status,
)


def test_event_headers_preserve_trace_and_idempotency() -> None:
    headers = dict(event_headers("00-abc-def-01", "feedback:42"))
    assert headers == {"traceparent": b"00-abc-def-01", "idempotency-key": b"feedback:42"}


def test_delta_source_is_replay_safe_and_newest_wins() -> None:
    early = {"idempotency_key": "a", "occurred_at": "2026-01-01T00:00:00Z", "value": 1}
    late = {"idempotency_key": "a", "occurred_at": "2026-01-02T00:00:00Z", "value": 2}
    other = {"idempotency_key": "b", "occurred_at": "2026-01-01T00:00:00Z", "value": 3}
    assert dedupe_latest([late, early, other, late]) == [late, other]


def test_feast_request_matches_the_registry() -> None:
    request = feast_online_request("student-7")
    assert request["entities"] == {"asker_id": ["student-7"]}
    assert request["features"] == [
        "asker_activity_v1:feedback_count",
        "asker_activity_v1:avg_rating",
        "asker_activity_v1:negative_ratio",
        "asker_activity_v1:delta_version",
    ]


def test_readiness_distinguishes_failure_severity() -> None:
    assert readiness_status([{"ready": True, "mandatory": True}]) == "ready"
    assert readiness_status([{"ready": False, "mandatory": False}]) == "degraded"
    assert readiness_status([{"ready": False, "mandatory": True}]) == "not_ready"

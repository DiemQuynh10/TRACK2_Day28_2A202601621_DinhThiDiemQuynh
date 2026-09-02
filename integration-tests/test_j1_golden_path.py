"""IT-J1 — one submission crossing all ten integration points.

The journey is a single act with many witnesses: one document and one piece of
feedback enter through the gateway carrying a trace context the test generated
itself, the pipeline runs, and then every boundary is asked whether it saw the
*same* record. That is what makes this a golden path rather than ten unit tests
in a trench coat — nothing is re-submitted per assertion, so a broken hand-off
shows up as one failing boundary instead of a green suite over a dead seam.

Preconditions: the stack is up and seeded (``lab28 seed``, ``lab28 index``,
``lab28 release``). The champion release matters — IP06 and IP07 answer with the
version that ``lab28 release`` promoted.

Evidence produced here: ``ip01-kafka-consume.json``, ``ip02-airflow-run.json``,
``ip04-feast-online.json``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import stack
from stack import Airflow, Prometheus, TraceBackend

from lab28_platform.contracts import stable_point_id
from lab28_platform.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.matrix("IT-J1-golden-path")]


@dataclass
class Journey:
    """Everything the one submission produced, gathered once."""

    suffix: str
    asker_id: str
    doc_id: str
    question: str
    trace_id: str
    traceparent: str
    document_response: httpx.Response
    feedback_response: httpx.Response
    delta_version_before: int | None
    dag_run: dict[str, Any]
    task_instances: list[dict[str, Any]]
    asset_events: list[dict[str, Any]]

    @property
    def idempotency_key(self) -> str:
        return str(self.feedback_response.json()["idempotency_key"])


@pytest.fixture(scope="module")
def journey(
    settings: Settings, gateway: httpx.Client, airflow: Airflow
) -> Journey:
    """Submit one document and one feedback, then run the pipeline once."""
    from lab28_platform import delta_store

    suffix = stack.run_id()
    asker_id = f"it-j1-{suffix}"
    doc_id = f"it-j1-doc-{suffix}"
    trace_id, traceparent = stack.new_trace()
    headers = {"traceparent": traceparent}

    try:
        delta_version_before: int | None = delta_store.current_version(settings.feedback_table)
    except Exception:
        # First run on a fresh volume: the table does not exist yet, which is a
        # legitimate starting state and not a failure of this journey.
        delta_version_before = None

    document = gateway.post(
        "/api/v1/documents",
        headers=headers,
        json={
            "doc_id": doc_id,
            "title": f"Tài liệu kiểm thử tích hợp {suffix}",
            "text": (
                f"Mã tham chiếu {suffix} được dùng cho bài kiểm thử tích hợp Day 28: "
                "nền tảng ghi phản hồi khách hàng vào Delta Lake, phục vụ đặc trưng "
                "qua Feast và truy hồi tài liệu bằng Qdrant."
            ),
            "tags": ["lab28", "integration"],
        },
    )
    feedback = gateway.post(
        "/api/v1/feedback",
        headers=headers,
        json={
            "asker_id": asker_id,
            "text": f"Nền tảng chạy ổn định trong bài kiểm thử {suffix}.",
            "rating": 5,
            "label": "positive",
        },
    )

    dag_run_id = airflow.trigger(
        conf={"traceparent": traceparent}, note=f"IT-J1 golden path {suffix}"
    )
    dag_run = airflow.wait_for_run(dag_run_id)

    return Journey(
        suffix=suffix,
        asker_id=asker_id,
        doc_id=doc_id,
        question=f"Mã tham chiếu {suffix} được dùng để làm gì?",
        trace_id=trace_id,
        traceparent=traceparent,
        document_response=document,
        feedback_response=feedback,
        delta_version_before=delta_version_before,
        dag_run=dag_run,
        task_instances=airflow.task_instances(dag_run_id),
        asset_events=airflow.asset_events(dag_run_id),
    )


@pytest.fixture(scope="module")
def answer(gateway: httpx.Client, journey: Journey) -> dict[str, Any]:
    """The serving half of the journey. Only gpu-gated tests may request it."""
    response = gateway.post(
        "/api/v1/ask",
        headers={"traceparent": journey.traceparent},
        json={"asker_id": journey.asker_id, "question": journey.question, "top_k": 3},
        timeout=90.0,
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


# -- IP08: the gateway --------------------------------------------------------


def test_the_gateway_accepted_both_submissions(journey: Journey) -> None:
    """202, not 200: the work downstream has not happened yet and saying so is the contract."""
    assert journey.document_response.status_code == 202, journey.document_response.text
    assert journey.feedback_response.status_code == 202, journey.feedback_response.text


def test_the_gateway_stamped_a_request_id(journey: Journey) -> None:
    """Without a request id there is nothing to correlate a client report against."""
    assert journey.feedback_response.headers.get("x-request-id")
    assert journey.document_response.headers.get("x-request-id")


# -- IP10: the caller's trace survives the edge -------------------------------


def test_the_response_carries_the_caller_s_trace_id(journey: Journey) -> None:
    assert journey.feedback_response.headers.get("x-lab28-trace-id") == journey.trace_id
    assert journey.feedback_response.json()["trace_id"] == journey.trace_id


# -- IP01: Kafka --------------------------------------------------------------


def test_the_event_reached_kafka_with_its_trace_context(
    settings: Settings, journey: Journey
) -> None:
    """The message must be findable, keyed for ordering, and still traceable.

    Keying by ``entity_id`` rather than the idempotency key is deliberate: it
    keeps every event about one asker on one partition and therefore in order.
    De-duplication is the lakehouse's job (IP03), not the broker's.
    """
    records = stack.read_topic(settings.kafka.bootstrap_servers, settings.kafka.topic_raw)
    mine = [
        record
        for record in records
        if record.value and record.value.get("idempotency_key") == journey.idempotency_key
    ]

    assert mine, f"no message on {settings.kafka.topic_raw} for {journey.idempotency_key}"
    record = mine[0]
    assert record.key == journey.asker_id
    assert record.value is not None
    assert record.value["schema_version"] == "1"
    assert record.value["payload"]["kind"] == "feedback"
    assert record.trace_id == journey.trace_id, "the trace context did not survive the produce"

    stack.write_evidence(
        "ip01-kafka-consume.json",
        {
            "topic": settings.kafka.topic_raw,
            "key": record.key,
            "partition": record.partition,
            "offset": record.offset,
            "headers": record.headers,
            "trace_id": record.trace_id,
            "value": record.value,
        },
    )


# -- IP02: the Airflow pipeline ----------------------------------------------


def test_the_pipeline_run_succeeded(journey: Journey) -> None:
    assert journey.dag_run["state"] == "success", journey.dag_run

    failed = [
        task["task_id"]
        for task in journey.task_instances
        if task["state"] not in {"success", "skipped"}
    ]
    assert failed == [], f"tasks that did not succeed: {failed}"


def test_the_run_published_the_lakehouse_asset_event(journey: Journey) -> None:
    """The asset event is what makes a downstream DAG schedulable on this data."""
    uris = {event["uri"] for event in journey.asset_events}

    assert "lab28://delta/feedback" in uris, f"asset events seen: {sorted(uris)}"

    stack.write_evidence(
        "ip02-airflow-run.json",
        {
            "dag_id": journey.dag_run["dag_id"],
            "dag_run_id": journey.dag_run["dag_run_id"],
            "state": journey.dag_run["state"],
            "conf": journey.dag_run.get("conf"),
            "task_instances": [
                {
                    "task_id": task["task_id"],
                    "state": task["state"],
                    "try_number": task.get("try_number"),
                }
                for task in journey.task_instances
            ],
            "asset_events": [
                {"uri": event["uri"], "timestamp": event.get("timestamp")}
                for event in journey.asset_events
            ],
        },
    )


# -- IP03: the lakehouse ------------------------------------------------------


def test_the_lakehouse_advanced_and_holds_the_row(
    settings: Settings, journey: Journey
) -> None:
    from lab28_platform import delta_store

    version = delta_store.current_version(settings.feedback_table)
    if journey.delta_version_before is not None:
        assert version > journey.delta_version_before, "the MERGE did not create a new version"

    rows = delta_store.read_rows(settings.feedback_table)
    mine = [row for row in rows if row.get("idempotency_key") == journey.idempotency_key]

    assert len(mine) == 1, f"expected exactly one row for {journey.idempotency_key}, got {len(mine)}"
    assert mine[0]["asker_id"] == journey.asker_id
    # Every hop creates its own span ID; continuity means the W3C trace ID
    # (field 2) stays the same, not that the entire traceparent is byte-equal.
    assert mine[0]["traceparent"].split("-")[1] == journey.traceparent.split("-")[1], (
        "IP10 needs the same trace id after the write"
    )


# -- IP04: the feature store --------------------------------------------------


def test_the_feature_store_serves_the_new_asker(settings: Settings, journey: Journey) -> None:
    """A cold entity is normal; an entity the pipeline just wrote is not.

    The features are read through the same client the serving path uses, so a
    shape change that would break serving breaks here first.
    """
    from lab28_platform.feature_store import FeatureClient

    client = FeatureClient(settings.feast)
    try:
        lookup = stack.wait_until(
            f"Feast to serve features for {journey.asker_id}",
            lambda: (
                found
                if (found := client.get_asker_features(journey.asker_id)).features.feedback_count
                else None
            ),
            timeout=120.0,
            interval=3.0,
        )
    finally:
        client.close()

    assert lookup.features.feedback_count >= 1
    assert lookup.features.delta_version is not None, "the feature row must carry its data version"
    assert lookup.freshness_seconds is not None

    stack.write_evidence(
        "ip04-feast-online.json",
        {
            "entity": {"asker_id": journey.asker_id},
            "feature_service": "asker_serving_v1",
            "features": lookup.features.model_dump(mode="json"),
            "statuses": lookup.statuses,
            "degraded": lookup.degraded,
            "freshness_seconds": lookup.freshness_seconds,
            "lookup_ms": lookup.latency_ms,
        },
    )


# -- IP05: the vector store ---------------------------------------------------


def test_the_document_is_retrievable_from_the_vector_store(
    settings: Settings, journey: Journey
) -> None:
    """Indexed at a deterministic point id, so a re-index updates instead of duplicating."""
    point_id = stable_point_id(journey.doc_id)

    point = stack.wait_until(
        f"Qdrant to hold the point for {journey.doc_id}",
        lambda: stack.qdrant_point(settings.qdrant.url, settings.qdrant.collection, point_id),
        timeout=120.0,
        interval=3.0,
    )

    assert point["payload"]["doc_id"] == journey.doc_id
    assert stack.qdrant_count(
        settings.qdrant.url, settings.qdrant.collection, doc_id=journey.doc_id
    ) == 1


# -- IP06: the model registry -------------------------------------------------


def test_a_champion_release_resolves(settings: Settings) -> None:
    """Serving reads the champion by alias; without one there is nothing to serve."""
    from lab28_platform.model_registry import ReleaseRegistry

    registry = ReleaseRegistry(settings.mlflow)
    release = registry.resolve()

    assert release.alias == settings.mlflow.alias
    assert release.version and release.run_id
    assert release.prompt_template, "the prompt is part of the release, not of the image"
    assert release.vllm_model_id


# -- IP07: real vLLM ----------------------------------------------------------


@pytest.mark.gpu
def test_the_answer_comes_from_the_champion_release_and_the_pinned_model(
    settings: Settings, journey: Journey, answer: dict[str, Any]
) -> None:
    from lab28_platform.model_registry import ReleaseRegistry

    evidence = answer["evidence"]
    champion = ReleaseRegistry(settings.mlflow).current_version()

    assert evidence["mlflow_release_version"] == champion
    assert evidence["vllm_model_id"] == settings.vllm.model_id
    assert evidence["embedding_model_id"] == settings.qdrant.embedding_model_id
    assert answer["answer"].strip(), "an empty answer is a failure, not a degradation"


@pytest.mark.gpu
def test_the_answer_is_grounded_in_the_document_this_journey_submitted(
    journey: Journey, answer: dict[str, Any]
) -> None:
    """Retrieval, not luck: the question quotes a token only this run's document contains."""
    assert answer["sources"], "an answer with no sources is ungrounded"
    assert journey.doc_id in {source["doc_id"] for source in answer["sources"]}


@pytest.mark.gpu
def test_the_answer_is_auditable_without_storing_user_text(answer: dict[str, Any]) -> None:
    audit = answer["audit"]

    assert audit["input_hash"] and audit["output_hash"]
    assert audit["output_length"] > 0
    assert audit["latency"]["total_ms"] > 0
    assert audit["latency"]["llm_ms"] > 0


# -- IP09: metrics ------------------------------------------------------------


def test_the_journey_is_visible_in_prometheus(prometheus: Prometheus, journey: Journey) -> None:
    """One request that no dashboard can see is one incident nobody can diagnose."""
    accepted = stack.wait_until(
        "Prometheus to scrape the ingestion counter",
        lambda: prometheus.value('sum(lab28_ingestion_events_total{outcome="accepted"})'),
        timeout=90.0,
        interval=5.0,
    )

    assert accepted >= 2, f"expected at least this journey's two events, saw {accepted}"


# -- IP10: the trace ----------------------------------------------------------


def test_the_journey_is_queryable_by_its_trace_id(
    traces: TraceBackend, journey: Journey
) -> None:
    """The whole point of propagating a trace id is being able to ask for it later.

    The asynchronous legs are included on purpose: the hop that usually loses
    context is Kafka, because nothing carries it there unless the producer wrote
    the header and the consumer read it back.
    """
    names = traces.wait_for_spans(
        journey.trace_id,
        [
            "lab28.gateway.request",
            "lab28.api.ingest",
            "lab28.kafka.produce",
            "lab28.kafka.consume",
            "lab28.airflow.dag",
            "lab28.spark.delta_merge",
        ],
        timeout=180.0,
    )

    assert len(names) >= 6

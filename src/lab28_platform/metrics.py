"""Prometheus metric definitions for every integration point.

Metric names are part of the platform contract: they are referenced by
``contracts/integration-matrix.yaml``, scraped by Prometheus, plotted by the
provisioned Grafana dashboards and evaluated by the SLO alert rules. Renaming
one silently breaks a dashboard panel and an alert, so the names live here and
nowhere else.

Naming follows the Prometheus convention — base unit in the name, ``_total`` on
counters, no unit suffix on gauges that carry an ``_info`` payload.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from prometheus_client import REGISTRY as DEFAULT_REGISTRY
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

logger = logging.getLogger(__name__)

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# Buckets chosen around the slide's latency budget so the SLO alert can be
# written against a real bucket boundary instead of an interpolated quantile.
_REQUEST_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_FAST_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
_LLM_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)


# --- IP01 ingestion -------------------------------------------------------

INGESTION_EVENTS = Counter(
    "lab28_ingestion_events_total",
    "Events accepted by the API and published to Kafka.",
    ["kind", "topic", "outcome"],
)
INGESTION_PUBLISH_SECONDS = Histogram(
    "lab28_ingestion_publish_seconds",
    "Time to durably publish one event to Kafka (acks=all).",
    ["topic"],
    buckets=_FAST_BUCKETS,
)

# --- IP02 pipeline --------------------------------------------------------

PIPELINE_BATCHES = Counter(
    "lab28_pipeline_batches_total",
    "Pipeline batches by outcome.",
    ["outcome"],
)
PIPELINE_EVENTS = Counter(
    "lab28_pipeline_events_total",
    "Events handled by the pipeline by outcome.",
    ["outcome"],
)
CONSUMER_LAG = Gauge(
    "lab28_consumer_lag",
    "Uncommitted messages behind the log end offset, per topic partition.",
    ["topic", "partition"],
)
DEAD_LETTERS = Counter(
    "lab28_dead_letters_total",
    "Messages routed to the dead-letter topic, by error category.",
    ["category"],
)
DEAD_LETTER_BACKLOG = Gauge(
    "lab28_dead_letter_backlog",
    "Messages currently parked on the dead-letter topic.",
)

# --- IP03 lakehouse -------------------------------------------------------

DELTA_VERSION = Gauge(
    "lab28_delta_version",
    "Current Delta table version. Monotonic; a drop means a rollback happened.",
    ["table"],
)
DELTA_ROWS_WRITTEN = Counter(
    "lab28_delta_rows_written_total",
    "Rows written by MERGE, split by whether they were inserted or updated.",
    ["table", "operation"],
)
DELTA_MERGE_SECONDS = Histogram(
    "lab28_delta_merge_seconds",
    "Duration of one Delta MERGE.",
    ["table"],
    buckets=_REQUEST_BUCKETS,
)

# --- IP04 feature store ---------------------------------------------------

FEATURE_LOOKUP_SECONDS = Histogram(
    "lab28_feature_lookup_seconds",
    "Online feature retrieval latency. Slide budget: 5 ms.",
    ["outcome"],
    buckets=_FAST_BUCKETS,
)
FEATURE_FRESHNESS_SECONDS = Gauge(
    "lab28_feature_freshness_seconds",
    "Age of the newest materialized feature row.",
    ["feature_view"],
)
FEATURE_ROWS_MATERIALIZED = Counter(
    "lab28_feature_rows_materialized_total",
    "Entity rows pushed into the online store.",
    ["feature_view"],
)

# --- IP05 vector store ----------------------------------------------------

RETRIEVAL_SECONDS = Histogram(
    "lab28_retrieval_seconds",
    "Vector retrieval latency. Slide budget: 50 ms.",
    ["mode", "outcome"],
    buckets=_FAST_BUCKETS,
)
VECTOR_POINTS = Gauge(
    "lab28_vector_points",
    "Points currently stored in the Qdrant collection.",
    ["collection"],
)
VECTOR_UPSERTS = Counter(
    "lab28_vector_upserts_total",
    "Points upserted into the vector store.",
    ["collection"],
)
EMBEDDING_SECONDS = Histogram(
    "lab28_embedding_seconds",
    "Time to embed one batch of text.",
    ["stage"],
    buckets=_FAST_BUCKETS,
)

# --- IP06 model registry --------------------------------------------------

RELEASE_INFO = Gauge(
    "lab28_release_version_info",
    "Champion release currently served. Value is always 1; the labels carry the payload.",
    ["model_name", "version", "run_id", "vllm_model_id", "delta_version"],
)
RELEASE_TRANSITIONS = Counter(
    "lab28_release_transitions_total",
    "Champion alias changes, by action.",
    ["action"],
)

# --- IP07 inference -------------------------------------------------------

LLM_SECONDS = Histogram(
    "lab28_llm_seconds",
    "vLLM chat completion latency. Slide budget: 500 ms.",
    ["model_id", "outcome"],
    buckets=_LLM_BUCKETS,
)
LLM_TOKENS = Counter(
    "lab28_llm_tokens_total",
    "Tokens consumed and produced by the inference endpoint.",
    ["model_id", "direction"],
)

# --- Serving and guardrails ----------------------------------------------

REQUEST_SECONDS = Histogram(
    "lab28_request_seconds",
    "End-to-end request latency measured inside the API. Slide budget: 1000 ms.",
    ["route", "status"],
    buckets=_REQUEST_BUCKETS,
)
REQUESTS = Counter(
    "lab28_requests_total",
    "Requests handled by the API.",
    ["route", "status"],
)
ERRORS = Counter(
    "lab28_errors_total",
    "Errors returned by the API, by stable error category.",
    ["route", "category"],
)
DEGRADED_RESPONSES = Counter(
    "lab28_degraded_responses_total",
    "Answers served on a degraded path, by the component that was unavailable.",
    ["reason"],
)
GUARDRAIL_ACTIONS = Counter(
    "lab28_guardrail_actions_total",
    "Guardrail outcomes, by direction and action.",
    ["direction", "action"],
)
BUDGET_EXCEEDED = Counter(
    "lab28_latency_budget_exceeded_total",
    "Components that exceeded their slide latency budget.",
    ["component"],
)

# --- Readiness ------------------------------------------------------------

COMPONENT_READY = Gauge(
    "lab28_component_ready",
    "1 when a dependency passes its readiness probe, 0 otherwise.",
    ["component", "owner"],
)
READINESS_SCORE = Gauge(
    "lab28_readiness_score",
    "Fraction of readiness checks passing, per pillar.",
    ["pillar", "profile"],
)


def render(registry: CollectorRegistry | None = None) -> bytes:
    """Render the Prometheus exposition format for the /metrics endpoint."""
    return generate_latest(registry or DEFAULT_REGISTRY)


def push_batch_metrics(
    gateway_url: str,
    job: str,
    *,
    grouping_key: dict[str, str] | None = None,
    registry: CollectorRegistry | None = None,
) -> bool:
    """Ship a short-lived job's metrics to the Pushgateway before it exits.

    Prometheus cannot scrape a process that has already finished, and an
    Airflow task lives for seconds. Without this, every IP02 and IP03 metric
    would be permanently absent from the dashboards IP09 requires — the classic
    "the pipeline has no metrics" failure, which looks like a scrape problem and
    is actually a lifetime problem.

    Pushing is used *only* for batch jobs. The API and the gateway are scraped
    normally; pushing long-lived services would hide their liveness behind the
    gateway's own.

    Returns False rather than raising: a metrics sink that is down must not fail
    a pipeline run that otherwise succeeded.
    """
    from prometheus_client import push_to_gateway

    try:
        push_to_gateway(
            gateway_url,
            job=job,
            registry=registry or DEFAULT_REGISTRY,
            grouping_key=grouping_key or {},
        )
    except Exception:  # pragma: no cover - needs a live gateway
        logger.warning("could not push metrics for job %s to %s", job, gateway_url)
        return False
    return True


def observe_budget(component: str, elapsed_ms: float, budget_ms: float) -> bool:
    """Record a latency budget breach. Returns True when the budget held."""
    if elapsed_ms > budget_ms:
        BUDGET_EXCEEDED.labels(component=component).inc()
        return False
    return True


def set_release(
    *,
    model_name: str,
    version: str,
    run_id: str,
    vllm_model_id: str,
    delta_version: int | None,
) -> None:
    """Publish the champion release as a single-valued info gauge.

    Old label sets are cleared first so exactly one series is exposed. Without
    the clear, a rollback would leave two releases reported as current and the
    dashboard would show both.
    """
    RELEASE_INFO.clear()
    RELEASE_INFO.labels(
        model_name=model_name,
        version=version,
        run_id=run_id,
        vllm_model_id=vllm_model_id,
        delta_version=str(delta_version if delta_version is not None else "unknown"),
    ).set(1)


def set_component_ready(components: Iterable[tuple[str, str, bool]]) -> None:
    """Publish per-dependency readiness as gauges for the SLO dashboard."""
    for name, owner, ready in components:
        COMPONENT_READY.labels(component=name, owner=owner).set(1 if ready else 0)

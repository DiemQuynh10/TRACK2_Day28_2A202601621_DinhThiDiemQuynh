"""Versioned public contracts for the Day 28 platform.

This module is the single source of truth for every boundary that crosses a
service: HTTP request/response bodies, Kafka payloads, the feature contract,
the retrieval contract and the serving evidence contract.

Every contract carries an explicit ``schema_version``. Producers must bump the
version when a change is not backward compatible; consumers must reject a
version they do not understand instead of silently coercing it.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

# --------------------------------------------------------------------------
# Schema versions
# --------------------------------------------------------------------------

INGESTION_SCHEMA_VERSION = "1"
PROCESSED_SCHEMA_VERSION = "1"
MODEL_EVENT_SCHEMA_VERSION = "1"
DEAD_LETTER_SCHEMA_VERSION = "1"
SERVING_SCHEMA_VERSION = "1"
FEATURE_SCHEMA_VERSION = "1"

# Stable UUID namespace so the same logical ID always maps to the same
# Qdrant point ID and the same Delta merge key across replays.
ID_NAMESPACE = uuid5(NAMESPACE_URL, "https://vinuni.edu.vn/aicb/day28/lab28-platform")


def stable_point_id(logical_id: str) -> str:
    """Derive a deterministic UUID from a logical string ID.

    Qdrant point IDs must be an unsigned integer or a UUID. Deriving the UUID
    from the logical ID is what makes re-indexing idempotent: replaying the same
    document overwrites one point instead of creating a duplicate.
    """
    return str(uuid5(ID_NAMESPACE, logical_id))


def content_hash(text: str) -> str:
    """Privacy-safe content fingerprint used in the request audit trail."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Kafka topic registry
# --------------------------------------------------------------------------


class TopicSpec(BaseModel):
    """Declarative Kafka topic definition applied by ``lab28 topics apply``."""

    model_config = ConfigDict(frozen=True)

    name: str
    partitions: int = Field(ge=1)
    replication_factor: int = Field(ge=1)
    retention_ms: int = Field(ge=1)
    cleanup_policy: Literal["delete", "compact"] = "delete"
    owner: str
    description: str

    @property
    def config(self) -> dict[str, str]:
        return {
            "retention.ms": str(self.retention_ms),
            "cleanup.policy": self.cleanup_policy,
        }


_SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000
_THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000

TOPIC_DATA_RAW = "data.raw"
TOPIC_DATA_PROCESSED = "data.processed"
TOPIC_MODEL_EVENTS = "model.events"
TOPIC_DATA_RAW_DLQ = "data.raw.dlq"

TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec(
        name=TOPIC_DATA_RAW,
        partitions=3,
        replication_factor=1,
        retention_ms=_SEVEN_DAYS_MS,
        owner="team-ingestion",
        description="Raw feedback and document submissions accepted by the API.",
    ),
    TopicSpec(
        name=TOPIC_DATA_PROCESSED,
        partitions=3,
        replication_factor=1,
        retention_ms=_SEVEN_DAYS_MS,
        owner="team-data",
        description="Batches durably committed to Delta by the Airflow pipeline.",
    ),
    TopicSpec(
        name=TOPIC_MODEL_EVENTS,
        partitions=1,
        replication_factor=1,
        retention_ms=_THIRTY_DAYS_MS,
        cleanup_policy="compact",
        owner="team-data",
        description="Model release lifecycle: registered, promoted, rolled back.",
    ),
    TopicSpec(
        name=TOPIC_DATA_RAW_DLQ,
        partitions=1,
        replication_factor=1,
        retention_ms=_THIRTY_DAYS_MS,
        owner="team-ingestion",
        description="Events the pipeline could not process, kept for replay.",
    ),
)

TOPICS_BY_NAME = {topic.name: topic for topic in TOPICS}

# --------------------------------------------------------------------------
# Service ownership and health semantics
# --------------------------------------------------------------------------

SERVICE_OWNERS: dict[str, str] = {
    "gateway": "team-platform",
    "lab28-api": "team-serving",
    "kafka": "team-ingestion",
    "airflow": "team-ingestion",
    "spark-delta": "team-data",
    "feast": "team-data",
    "qdrant": "team-serving",
    "mlflow": "team-data",
    "vllm": "team-serving",
    "otel-collector": "team-platform",
    "prometheus": "team-platform",
    "grafana": "team-platform",
}

HEALTH_SEMANTICS = {
    "/health": "Liveness. 200 whenever the process can serve HTTP. Never touches a dependency.",
    "/ready": (
        "Readiness. 200 only when every mandatory dependency for the serving path is usable. "
        "503 with a per-dependency breakdown otherwise. The gateway removes a 503 pod "
        "from rotation."
    ),
    "/startup": "Startup. 200 once configuration is loaded and clients are constructed.",
}


# --------------------------------------------------------------------------
# Error taxonomy
# --------------------------------------------------------------------------


class ErrorCategory(StrEnum):
    """Stable error categories. Clients branch on these, never on prose."""

    VALIDATION = "validation"
    CONTRACT_VERSION = "contract_version"
    GUARDRAIL_BLOCKED = "guardrail_blocked"
    RATE_LIMITED = "rate_limited"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    DEPENDENCY_TIMEOUT = "dependency_timeout"
    NOT_READY = "not_ready"
    INTERNAL = "internal"


#: HTTP status for each error category. Kept here so the API and the gateway
#: agree on what a category means on the wire.
ERROR_STATUS: dict[ErrorCategory, int] = {
    ErrorCategory.VALIDATION: 422,
    ErrorCategory.CONTRACT_VERSION: 409,
    ErrorCategory.GUARDRAIL_BLOCKED: 422,
    ErrorCategory.RATE_LIMITED: 429,
    ErrorCategory.DEPENDENCY_UNAVAILABLE: 503,
    ErrorCategory.DEPENDENCY_TIMEOUT: 504,
    ErrorCategory.NOT_READY: 503,
    ErrorCategory.INTERNAL: 500,
}


class ErrorResponse(BaseModel):
    """The only error body the platform returns."""

    schema_version: Literal["1"] = "1"
    category: ErrorCategory
    message: str
    trace_id: str
    service: str
    retryable: bool


# --------------------------------------------------------------------------
# Shared field types
# --------------------------------------------------------------------------

Locale = Literal["vi", "en"]
Label = Literal["positive", "negative", "neutral"]

TraceParent = Annotated[
    str,
    StringConstraints(pattern=r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$"),
]
FeedbackText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=5, max_length=2000)
]
QuestionText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=3, max_length=1000)
]
DocumentText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=20, max_length=20000)
]
Identifier = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=r"^[A-Za-z0-9._:-]{1,128}$")
]


def _utc_now() -> datetime:
    return datetime.now(UTC)


# --------------------------------------------------------------------------
# HTTP ingestion contract
# --------------------------------------------------------------------------


class FeedbackSubmission(BaseModel):
    """POST /api/v1/feedback request body."""

    model_config = ConfigDict(extra="forbid")

    asker_id: Identifier
    text: FeedbackText
    rating: int = Field(ge=1, le=5)
    locale: Locale = "vi"
    label: Label | None = None
    idempotency_key: Identifier | None = None


class DocumentSubmission(BaseModel):
    """POST /api/v1/documents request body."""

    model_config = ConfigDict(extra="forbid")

    doc_id: Identifier
    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=200)]
    text: DocumentText
    locale: Locale = "vi"
    tags: list[Identifier] = Field(default_factory=list, max_length=10)
    idempotency_key: Identifier | None = None


class IngestionAccepted(BaseModel):
    """202 response for both ingestion endpoints."""

    schema_version: Literal["1"] = INGESTION_SCHEMA_VERSION
    status: Literal["accepted"] = "accepted"
    event_id: str
    idempotency_key: str
    entity_id: str
    topic: str
    trace_id: str


# --------------------------------------------------------------------------
# Kafka payload contracts
# --------------------------------------------------------------------------


class FeedbackPayload(BaseModel):
    kind: Literal["feedback"] = "feedback"
    asker_id: Identifier
    text: FeedbackText
    rating: int = Field(ge=1, le=5)
    locale: Locale = "vi"
    label: Label | None = None


class DocumentPayload(BaseModel):
    kind: Literal["document"] = "document"
    doc_id: Identifier
    title: str
    text: DocumentText
    locale: Locale = "vi"
    tags: list[Identifier] = Field(default_factory=list)


class IngestionEvent(BaseModel):
    """Payload on ``data.raw``.

    ``idempotency_key`` is the logical de-duplication key. Two events with the
    same key are the same fact and must collapse to one row in Delta, one Feast
    entity update and one Qdrant point, no matter how many times they arrive.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = INGESTION_SCHEMA_VERSION
    event_id: str = Field(default_factory=lambda: uuid4().hex, min_length=8, max_length=64)
    idempotency_key: Identifier
    entity_id: Identifier
    occurred_at: datetime = Field(default_factory=_utc_now)
    producer: str = "lab28-api"
    traceparent: TraceParent | None = None
    payload: FeedbackPayload | DocumentPayload = Field(discriminator="kind")

    @property
    def kind(self) -> str:
        return self.payload.kind


class ProcessedBatchEvent(BaseModel):
    """Payload on ``data.processed``, emitted after a durable Delta commit."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = PROCESSED_SCHEMA_VERSION
    run_id: Identifier
    delta_version: int = Field(ge=0)
    feedback_rows: int = Field(ge=0)
    document_rows: int = Field(ge=0)
    idempotency_keys: list[str]
    entity_ids: list[str]
    occurred_at: datetime = Field(default_factory=_utc_now)
    traceparent: TraceParent | None = None


class ModelLifecycleEvent(BaseModel):
    """Payload on ``model.events``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = MODEL_EVENT_SCHEMA_VERSION
    action: Literal["registered", "promoted", "rolled_back"]
    model_name: str
    version: str
    alias: str = "champion"
    run_id: str
    delta_version: int | None = None
    previous_version: str | None = None
    occurred_at: datetime = Field(default_factory=_utc_now)
    traceparent: TraceParent | None = None


class DeadLetterEnvelope(BaseModel):
    """Payload on ``data.raw.dlq``.

    The raw bytes are preserved verbatim so a replay can re-submit the original
    message once the defect is fixed.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = DEAD_LETTER_SCHEMA_VERSION
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: str | None = None
    error_category: ErrorCategory
    error_detail: str
    attempts: int = Field(ge=1)
    failed_at: datetime = Field(default_factory=_utc_now)
    traceparent: TraceParent | None = None
    raw_payload_b64: str


# --------------------------------------------------------------------------
# Feature store contract
# --------------------------------------------------------------------------


class AskerFeatures(BaseModel):
    """The Feast online feature vector consumed by the serving path.

    Entity: ``asker_id``. Source: aggregation of the Delta feedback table.
    """

    schema_version: Literal["1"] = FEATURE_SCHEMA_VERSION
    asker_id: str
    feedback_count: int = 0
    avg_rating: float = 0.0
    negative_ratio: float = 0.0
    last_event_ts: datetime | None = None
    delta_version: int | None = None

    @property
    def freshness_seconds(self) -> float | None:
        if self.last_event_ts is None:
            return None
        return (_utc_now() - self.last_event_ts).total_seconds()


FEATURE_SERVICE_NAME = "asker_serving_v1"
FEATURE_VIEW_NAME = "asker_activity_v1"
FEATURE_REFS: tuple[str, ...] = (
    f"{FEATURE_VIEW_NAME}:feedback_count",
    f"{FEATURE_VIEW_NAME}:avg_rating",
    f"{FEATURE_VIEW_NAME}:negative_ratio",
    f"{FEATURE_VIEW_NAME}:delta_version",
)


# --------------------------------------------------------------------------
# Retrieval contract
# --------------------------------------------------------------------------

QDRANT_COLLECTION = "lab28_documents"
DENSE_VECTOR_NAME = "dense"
SPARSE_VECTOR_NAME = "sparse"


class RetrievedSource(BaseModel):
    """One retrieved document returned to the caller for grounding."""

    doc_id: str
    title: str
    snippet: Annotated[str, StringConstraints(max_length=600)]
    score: float
    retrieval_mode: Literal["hybrid", "dense", "sparse"]


# --------------------------------------------------------------------------
# Serving contract
# --------------------------------------------------------------------------


class AskRequest(BaseModel):
    """POST /api/v1/ask request body."""

    model_config = ConfigDict(extra="forbid")

    asker_id: Identifier
    question: QuestionText
    locale: Locale = "vi"
    top_k: int = Field(default=3, ge=1, le=10)

    @field_validator("question")
    @classmethod
    def reject_control_characters(cls, value: str) -> str:
        if any(ord(character) < 32 and character not in "\n\t" for character in value):
            raise ValueError("question contains control characters")
        return value


class LatencyBreakdown(BaseModel):
    """Per-component latency, mirroring the slide's request audit trail."""

    feature_ms: float = 0.0
    retrieval_ms: float = 0.0
    llm_ms: float = 0.0
    guardrail_ms: float = 0.0
    total_ms: float = 0.0


class AuditTrail(BaseModel):
    """Privacy-safe audit record: hashes and sizes, never raw user text."""

    input_hash: str
    output_hash: str
    output_length: int
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    latency: LatencyBreakdown


class ServingEvidence(BaseModel):
    """The identifiers that make one answer reproducible and auditable.

    This is the contract the live smoke tests assert on: the same trace ID must
    appear in the trace backend, the same MLflow version must be the champion,
    and the same vLLM model ID must be served by the inference endpoint.
    """

    trace_id: str
    mlflow_model_name: str
    mlflow_release_version: str
    mlflow_run_id: str
    vllm_model_id: str
    embedding_model_id: str
    delta_version: int | None = None
    feature_freshness_seconds: float | None = None
    degraded: bool = False
    degraded_reasons: list[str] = Field(default_factory=list)


class AskResponse(BaseModel):
    """200 response for /api/v1/ask."""

    schema_version: Literal["1"] = SERVING_SCHEMA_VERSION
    answer: str
    sources: list[RetrievedSource]
    evidence: ServingEvidence
    audit: AuditTrail


class ReadinessComponent(BaseModel):
    """One dependency in the /ready breakdown."""

    name: str
    ready: bool
    detail: str
    owner: str


class ReadinessResponse(BaseModel):
    schema_version: Literal["1"] = "1"
    status: Literal["ready", "degraded", "not_ready"]
    components: list[ReadinessComponent]
    trace_id: str

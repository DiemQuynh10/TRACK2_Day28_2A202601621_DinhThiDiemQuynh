"""Feast online feature retrieval during inference (IP04).

The serving process talks to the Feast *feature server* over HTTP and never
imports ``feast`` itself. That keeps the API image small, and it keeps Feast's
dependency constraints (it caps ``prometheus-client`` below the version this
project uses) out of the serving resolution entirely. The feature repository,
materialization and the server all live in the Feast image.

The response shape is the part worth reading twice: ``/get-online-features``
returns ``results`` as a *positional* array aligned with
``metadata.feature_names``, not a mapping. The entity join key comes back as an
extra column whose event timestamps are epoch zero, so it must not be mistaken
for feature freshness.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from lab28_platform import integration_tasks, metrics
from lab28_platform.contracts import (
    FEATURE_REFS,
    FEATURE_VIEW_NAME,
    AskerFeatures,
)
from lab28_platform.settings import FeastSettings
from lab28_platform.telemetry import SPAN_FEAST_LOOKUP, span

#: Feast reports one of these per requested feature, per entity row.
STATUS_PRESENT = "PRESENT"
STATUS_NOT_FOUND = "NOT_FOUND"
STATUS_STALE = "OUTSIDE_MAX_AGE"


class FeaturesUnavailable(RuntimeError):
    """The feature server is unreachable or returned an unusable response."""


@dataclass(frozen=True)
class FeatureLookup:
    """One online lookup, including why it may have been incomplete.

    ``degraded`` is what the serving path acts on: a cold entity is normal and
    must not fail the request, but it has to be visible in the response
    evidence rather than silently defaulted to zeros.
    """

    features: AskerFeatures
    degraded: bool
    statuses: dict[str, str]
    latency_ms: float
    detail: str

    @property
    def freshness_seconds(self) -> float | None:
        return self.features.freshness_seconds


def _strip_view(feature_name: str) -> str:
    return feature_name.split(":", 1)[-1]


def _parse_timestamp(raw: Any) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    # Feast returns epoch zero for the join-key column; that is a placeholder,
    # not a real event time, and treating it as one reports absurd freshness.
    return None if parsed.year <= 1970 else parsed


class FeatureClient:
    """Online feature reads against the Feast feature server."""

    def __init__(self, settings: FeastSettings) -> None:
        self._settings = settings
        self._client = httpx.Client(timeout=httpx.Timeout(settings.timeout_seconds))

    def get_asker_features(self, asker_id: str) -> FeatureLookup:
        """Fetch the serving feature vector for one asker.

        Never raises for a cold entity — it returns contract defaults marked
        ``degraded``. It does raise when the feature server itself is down,
        because that is a platform failure the readiness report must show.
        """
        started = time.perf_counter()
        with span(
            SPAN_FEAST_LOOKUP,
            attributes={
                "lab28.feature.view": FEATURE_VIEW_NAME,
                "lab28.feature.count": len(FEATURE_REFS),
            },
        ) as active:
            payload = integration_tasks.feast_online_request(asker_id)
            try:
                response = self._client.post(
                    f"{self._settings.server_url.rstrip('/')}/get-online-features",
                    json=payload,
                )
                response.raise_for_status()
                body = response.json()
            except httpx.HTTPError as error:
                metrics.FEATURE_LOOKUP_SECONDS.labels(outcome="unavailable").observe(
                    time.perf_counter() - started
                )
                raise FeaturesUnavailable(
                    f"Feast feature server unreachable: {type(error).__name__}"
                ) from error
            except ValueError as error:
                metrics.FEATURE_LOOKUP_SECONDS.labels(outcome="malformed").observe(
                    time.perf_counter() - started
                )
                raise FeaturesUnavailable("Feast returned a non-JSON body") from error

            lookup = self._to_lookup(asker_id, body, started)
            outcome = "degraded" if lookup.degraded else "ok"
            metrics.FEATURE_LOOKUP_SECONDS.labels(outcome=outcome).observe(
                lookup.latency_ms / 1000
            )
            if lookup.freshness_seconds is not None:
                metrics.FEATURE_FRESHNESS_SECONDS.labels(
                    feature_view=FEATURE_VIEW_NAME
                ).set(lookup.freshness_seconds)
            active.set_attribute("lab28.feature.degraded", lookup.degraded)
            return lookup

    def materialize_incremental(self) -> dict[str, Any]:
        """Materialize the declared feature view through Feast's HTTP API.

        The offline snapshot is a complete overwrite produced by Spark. Asking
        Feast to materialize all available rows is therefore both simpler and
        safer than manufacturing a host-local timestamp window (which is easy
        to get wrong across Windows, macOS and Linux time zones).
        """
        try:
            response = self._client.post(
                f"{self._settings.server_url.rstrip('/')}/materialize",
                json={
                    "feature_views": [FEATURE_VIEW_NAME],
                    "disable_event_timestamp": True,
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise FeaturesUnavailable(
                f"Feast materialization failed: {type(error).__name__}"
            ) from error

        metrics.FEATURE_ROWS_MATERIALIZED.labels(
            feature_view=FEATURE_VIEW_NAME
        ).inc()
        if not response.content:
            return {"status": "accepted", "feature_views": [FEATURE_VIEW_NAME]}
        try:
            body = response.json()
        except ValueError:
            body = {"status": response.text.strip() or "accepted"}
        return dict(body) if isinstance(body, dict) else {"result": body}

    def _to_lookup(
        self, asker_id: str, body: dict[str, Any], started: float
    ) -> FeatureLookup:
        names = (body.get("metadata") or {}).get("feature_names") or []
        results = body.get("results") or []
        if len(names) != len(results):
            raise FeaturesUnavailable(
                f"Feast returned {len(results)} result columns for {len(names)} names"
            )

        values: dict[str, Any] = {}
        statuses: dict[str, str] = {}
        event_times: list[datetime] = []
        for name, column in zip(names, results, strict=True):
            key = _strip_view(name)
            column_values = column.get("values") or [None]
            column_statuses = column.get("statuses") or [STATUS_NOT_FOUND]
            values[key] = column_values[0]
            statuses[key] = column_statuses[0]
            timestamp = _parse_timestamp((column.get("event_timestamps") or [None])[0])
            if timestamp is not None and key != "asker_id":
                event_times.append(timestamp)

        missing = [
            _strip_view(ref)
            for ref in FEATURE_REFS
            if statuses.get(_strip_view(ref), STATUS_NOT_FOUND) != STATUS_PRESENT
        ]
        stale = [key for key, status in statuses.items() if status == STATUS_STALE]

        features = AskerFeatures(
            asker_id=asker_id,
            feedback_count=int(values.get("feedback_count") or 0),
            avg_rating=float(values.get("avg_rating") or 0.0),
            negative_ratio=float(values.get("negative_ratio") or 0.0),
            last_event_ts=max(event_times) if event_times else None,
            delta_version=(
                int(values["delta_version"])
                if values.get("delta_version") is not None
                else None
            ),
        )

        if stale:
            detail = f"stale beyond ttl: {', '.join(sorted(stale))}"
        elif missing:
            detail = f"no online row for asker {asker_id}; defaulted {', '.join(missing)}"
        else:
            detail = "all features present"

        return FeatureLookup(
            features=features,
            degraded=bool(missing or stale),
            statuses=statuses,
            latency_ms=(time.perf_counter() - started) * 1000,
            detail=detail,
        )

    def health(self) -> dict[str, Any]:
        """Probe the feature server.

        ``/health`` answers 200 with an empty body, and 503 when the registry
        cannot be loaded — which is the failure a student is most likely to
        cause by forgetting ``feast apply``.
        """
        try:
            response = self._client.get(
                f"{self._settings.server_url.rstrip('/')}/health",
                timeout=self._settings.timeout_seconds,
            )
            return {
                "reachable": True,
                "status_code": response.status_code,
                "healthy": response.status_code == 200,
                "detail": (
                    "ok"
                    if response.status_code == 200
                    else f"feature server returned {response.status_code}"
                ),
            }
        except httpx.HTTPError as error:
            return {
                "reachable": False,
                "status_code": None,
                "healthy": False,
                "detail": f"unreachable: {type(error).__name__}",
            }

    def close(self) -> None:
        self._client.close()

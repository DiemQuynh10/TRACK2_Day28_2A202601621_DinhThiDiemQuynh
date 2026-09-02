"""Meaningful TODO boundaries; complete these before wiring the live services."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

FEATURE_VIEW = "asker_activity_v1"


def event_headers(traceparent: str, idempotency_key: str) -> list[tuple[str, bytes]]:
    """Return Kafka headers carrying both correlation contracts."""
    raise NotImplementedError("TODO IP01/IP10: propagate trace and idempotency headers")


def dedupe_latest(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one newest event per idempotency_key, in deterministic order."""
    raise NotImplementedError("TODO IP03: prepare a replay-safe Delta MERGE source")


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast /get-online-features request for asker_activity_v1."""
    raise NotImplementedError("TODO IP04: preserve the feature registry contract")


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ready, degraded or not_ready from ready/mandatory probe fields."""
    raise NotImplementedError("TODO IP07/IP08: implement explicit readiness semantics")

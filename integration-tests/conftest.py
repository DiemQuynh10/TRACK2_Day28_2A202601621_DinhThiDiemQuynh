"""Fixtures, preconditions and gates for the live suite.

This suite is opt-in (``pytest integration-tests``) and it never skips itself
into a false green: if the stack is not running the session stops with one
message naming the unreachable endpoints, and if a gated dependency is missing
only the tests that genuinely need it are skipped.

Two gates exist, both declared as markers so the matrix and the code agree:

``gpu``
    The endpoint must prove it is a real vLLM build. An OpenAI-compatible mock
    passes an HTTP check and fails this gate, which is the point.

``langsmith``
    Only the LangSmith export leg of IP10. The local trace backend carries the
    same spans, so everything else about tracing stays runnable offline.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import stack
import yaml
from stack import Airflow, Prometheus, TraceBackend

from lab28_platform.settings import Settings

#: Probing vLLM costs a round trip and every gpu-marked test needs the answer,
#: so the identity is resolved once per session.
_VLLM_IDENTITY: Any = None
_VLLM_PROBED = False


@pytest.fixture(scope="session")
def settings() -> Settings:
    """The same environment-driven settings the application itself reads."""
    return Settings.from_env()


@pytest.fixture(scope="session")
def matrix() -> dict[str, Any]:
    path = stack.REPO_ROOT / "contracts" / "integration-matrix.yaml"
    return dict(yaml.safe_load(path.read_text(encoding="utf-8")))


@pytest.fixture(scope="session")
def api(settings: Settings) -> Iterator[httpx.Client]:
    """Direct client to the API, bypassing the gateway.

    Some tests deliberately need the pod itself — for instance to show that an
    unready pod still answers when it is reached directly, while the gateway
    has already taken it out of rotation.
    """
    with httpx.Client(base_url=settings.api_url, timeout=stack.HTTP_TIMEOUT) as client:
        yield client


@pytest.fixture(scope="session")
def gateway(settings: Settings) -> Iterator[httpx.Client]:
    """Client to the public entry point — the path a real caller takes."""
    with httpx.Client(base_url=settings.gateway_url, timeout=stack.HTTP_TIMEOUT) as client:
        yield client


@pytest.fixture(scope="session")
def gateway_admin() -> str:
    return stack.env("LAB28_GATEWAY_ADMIN_URL", "http://localhost:9901")


@pytest.fixture(scope="session")
def airflow() -> Airflow:
    password = stack.env("LAB28_AIRFLOW_PASSWORD", "")
    password_file = (
        Path(__file__).resolve().parents[1]
        / ".lab28"
        / "airflow"
        / "simple-auth-passwords.json"
    )
    if not password and password_file.is_file():
        password = json.loads(password_file.read_text(encoding="utf-8"))["airflow"]
    if not password:
        pytest.skip("Airflow runtime credential is unavailable; start the full profile")
    return Airflow(
        base_url=stack.env("LAB28_AIRFLOW_URL", "http://localhost:8082"),
        username=stack.env("LAB28_AIRFLOW_USERNAME", "airflow"),
        password=password,
        dag_id=stack.env("LAB28_AIRFLOW_DAG", "lab28_ingestion_pipeline"),
    )


@pytest.fixture(scope="session")
def prometheus() -> Prometheus:
    return Prometheus(stack.env("LAB28_PROMETHEUS_URL", "http://localhost:9090"))


@pytest.fixture(scope="session")
def traces() -> TraceBackend:
    return TraceBackend(stack.env("LAB28_TRACE_BACKEND_URL", "http://localhost:16686"))


@pytest.fixture(scope="session")
def grafana() -> tuple[str, tuple[str, str]]:
    return (
        stack.env("LAB28_GRAFANA_URL", "http://localhost:3000"),
        (stack.env("LAB28_GRAFANA_USER", "admin"), stack.env("LAB28_GRAFANA_PASSWORD", "admin")),
    )


@pytest.fixture(scope="session")
def stack_is_up(settings: Settings, prometheus: Prometheus) -> None:
    """Fail the whole session, once, if the platform is not up.

    Without this every test fails separately with its own connection error and
    the report buries the actual cause. Only *reachability* is checked here —
    whether a component is correct is what the tests are for.
    """
    from lab28_platform.event_bus import broker_metadata

    airflow_url = stack.env("LAB28_AIRFLOW_URL", "http://localhost:8082")
    checks: dict[str, Any] = {
        f"API {settings.api_url}/health": lambda: httpx.get(
            f"{settings.api_url}/health", timeout=5.0
        ).raise_for_status(),
        f"gateway {settings.gateway_url}": lambda: httpx.get(
            f"{settings.gateway_url}/healthz", timeout=5.0
        ).raise_for_status(),
        f"Kafka {settings.kafka.bootstrap_servers}": lambda: broker_metadata(
            settings.kafka, timeout=5.0
        ),
        f"Airflow {airflow_url}": lambda: httpx.get(
            f"{airflow_url.rstrip('/')}/api/v2/monitor/health", timeout=5.0
        ).raise_for_status(),
        f"Prometheus {prometheus.base_url}": prometheus.targets,
    }

    down = {}
    for name, probe in checks.items():
        try:
            probe()
        except Exception as error:
            down[name] = f"{type(error).__name__}: {error}"

    if down:
        listed = "\n".join(f"  - {name}: {detail}" for name, detail in down.items())
        pytest.fail(
            "the live suite needs the whole stack running; unreachable:\n"
            f"{listed}\n"
            "start it with `docker compose up -d` and seed it with `lab28 seed`, "
            "`lab28 index` and `lab28 release` before running this suite.",
            pytrace=False,
        )


@pytest.fixture(scope="session", autouse=True)
def running_stack(request: pytest.FixtureRequest) -> None:
    """Run one preflight before any live fixture can perform network I/O.

    Module-scoped journey fixtures are set up before function-scoped autouse
    fixtures. Keeping this guard at session scope is therefore essential: a
    function-scoped guard lets the module fixture fail first with a raw
    ``ConnectError``. Tests marked ``offline`` are static contract checks and
    remain runnable without Docker.
    """
    selected = list(request.session.items)
    if selected and all(item.get_closest_marker("offline") for item in selected):
        return
    request.getfixturevalue("stack_is_up")


def vllm_identity(settings: Settings) -> Any:
    """Resolve the vLLM identity once and reuse it for every gpu-gated test."""
    global _VLLM_IDENTITY, _VLLM_PROBED
    if not _VLLM_PROBED:
        from lab28_platform.llm_client import probe_identity

        try:
            _VLLM_IDENTITY = probe_identity(settings.vllm)
        except Exception:
            _VLLM_IDENTITY = None
        _VLLM_PROBED = True
    return _VLLM_IDENTITY


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Apply the two declared gates before a marked test runs."""
    if item.get_closest_marker("gpu"):
        identity = vllm_identity(Settings.from_env())
        if identity is None:
            pytest.skip("gpu gate: no vLLM endpoint answered")
        if not identity.is_real_vllm:
            pytest.skip(f"gpu gate: endpoint is not a verifiable vLLM build — {identity.detail}")

    if item.get_closest_marker("langsmith") and not os.getenv("LANGSMITH_API_KEY"):
        pytest.skip("langsmith gate: LANGSMITH_API_KEY is not set")

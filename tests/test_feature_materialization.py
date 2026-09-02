from __future__ import annotations

import httpx

from lab28_platform.feature_store import FeatureClient
from lab28_platform.settings import FeastSettings


def _settings(tmp_path):
    return FeastSettings(
        repo_path=tmp_path,
        server_url="http://feast.test",
        metrics_url="http://feast.test:8000",
        timeout_seconds=1.0,
    )


def test_materialization_uses_the_cross_platform_all_rows_contract(tmp_path) -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request=request, payload=request.read().decode())
        return httpx.Response(200, json={"status": "success"})

    client = FeatureClient(_settings(tmp_path))
    client._client.close()
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        result = client.materialize_incremental()
    finally:
        client.close()

    assert result == {"status": "success"}
    assert isinstance(seen["request"], httpx.Request)
    assert seen["request"].url.path == "/materialize"
    assert '"disable_event_timestamp":true' in str(seen["payload"]).replace(" ", "")

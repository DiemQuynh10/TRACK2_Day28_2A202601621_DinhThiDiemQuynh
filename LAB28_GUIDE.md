# Hướng dẫn thực hành — Day 28 Track 2 v2

## 1. Setup giống nhau trên Windows/macOS/Linux

```text
uv sync --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 preflight
docker compose up -d --build
docker compose ps
uv run lab28 topics
```

`--no-editable` tránh lỗi filesystem sync/permission ở một số thư mục đồng bộ;
source vẫn được mount vào container cho phần lab. Nếu trùng cổng, facilitator phát
cấu hình Compose override thống nhất cho cả lớp.

## 2. Core checkpoint

```text
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed
uv run lab28 inspect
uv run lab28 ready
```

Gateway `:8080`, Grafana `:3000`, Prometheus `:9090`, Jaeger `:16686`, MLflow
`:5000`, Qdrant `:6333/dashboard`.

## 3. Full data/ML checkpoint

```text
docker compose --profile full up -d --build
uv run pytest integration-tests/test_j1_golden_path.py -q
```

Airflow ở `:8082`. Golden path gửi document/feedback, chạy DAG, MERGE Delta,
materialize Feast, index Qdrant và phát processed event.

## 4. GPU checkpoint

Local NVIDIA dùng `compose.gpu.yaml`. Máy không GPU dùng endpoint vLLM thật từ
Kaggle/cluster. Gate kiểm tra `/version`, model list và metric `vllm:`; server giả
OpenAI-compatible không pass.

## 5. Evidence và demo

```text
uv run lab28 evidence
uv run python load-tests/run_profile.py --requests 200 --workers 8
uv run python scripts/validate_manifests.py
```

Dùng [demo runbook](docs/demo-runbook.md) và [rubric](docs/rubric.md). Evidence có
timestamp, run/trace/data/model IDs và nguồn live; không commit runtime DB/cache.

## Troubleshooting theo boundary

- Kafka: topics, broker health, topic name.
- Airflow: health, DAG import errors, task retry/log.
- Delta: Spark Connect và path container `/workspace/.lab28/delta`.
- Feast: `asker_activity_v1`, snapshot và materialize HTTP 200.
- Qdrant: collection/points và embedding model ID.
- MLflow: tracking URI host/container và champion alias.
- Gateway/API: phân biệt `/health` liveness và `/ready` dependencies.
- Trace: so trace ID, không so span ID trong toàn traceparent.

## Dọn môi trường

```text
docker compose --profile full down --remove-orphans
```

Chỉ dùng `uv run lab28 reset --yes` khi muốn xóa state/volumes; không dùng trong
demo recovery.

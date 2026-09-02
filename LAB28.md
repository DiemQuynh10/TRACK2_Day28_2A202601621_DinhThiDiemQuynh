# Lab 28 Track 2 — Platform Integration & Production Readiness

## Thử thách

Nhóm bạn tiếp quản một RAG platform chưa nối hoàn chỉnh. Mục tiêu không chỉ là
“chạy chatbot”, mà là chứng minh request đi qua đủ 10 boundary của slide, có
version, trace, fallback và rollback. Không giới hạn thời lượng cứng; facilitator
kiểm tra checkpoint và hỗ trợ chẩn đoán, sau đó mỗi nhóm demo trước lớp.

## Learning outcomes

1. Thiết kế contract giữa HTTP, Kafka, orchestration và lakehouse.
2. Chứng minh idempotency bằng Kafka replay và Delta MERGE/time travel.
3. Tách offline/online feature path trong Feast và retrieval path trong Qdrant.
4. Đóng gói release bằng MLflow alias để promotion/rollback tái lập được.
5. Gọi vLLM thật qua OpenAI-compatible contract.
6. Thiết kế health/readiness, degraded mode và gateway rate limit.
7. Theo W3C trace xuyên HTTP → Kafka → Airflow → data/ML → response.
8. Đọc golden signals, alert và readiness evidence.
9. Giải thích Kubernetes/Gateway API/Argo CD và GitOps rollback.

## Definition of Done — 10 integration points

| ID | Boundary | Evidence |
|---|---|---|
| IP01 | HTTP ingestion → Kafka | event + key + traceparent |
| IP02 | Kafka → Airflow 3 | DAG run + asset event |
| IP03 | Airflow/Spark → Delta | MERGE history + time travel |
| IP04 | Delta → Feast | online entity + freshness |
| IP05 | Delta documents → Qdrant | deterministic IDs + scores |
| IP06 | Evaluation → MLflow Registry | artifact/signature/tags + champion |
| IP07 | RAG prompt → real vLLM | identity + model + metrics |
| IP08 | Client → Envoy gateway | route + request ID + 429 |
| IP09 | Components → Prometheus/Grafana | targets + dashboard + alert |
| IP10 | Components → OTLP trace | one trace carrying required spans |

Nguồn machine-readable: `contracts/integration-matrix.yaml`.

## Team roles và checkpoints

- Ingestion & Orchestration: IP01–IP02, retry, DLQ/replay.
- Data & ML: IP03–IP04–IP06, schema, version, materialization, rollback.
- Serving & Retrieval: IP05–IP07, grounding, budget, degraded behavior.
- Platform & Observability: IP08–IP10, Kubernetes/GitOps, readiness/security.
- Presenter / Incident Commander: evidence, incident narrative và Q&A.

Checkpoints: contracts → data plane → ML plane → serving → operations → delivery.
Explorer dùng progressive hints; Builder triển khai adapter/tests; Operator chạy
failure injection, profiling và GitOps. Level quyết định độ tự chủ, không giảm DoD nhóm.

Demo gồm architecture, happy path, một error/recovery, dashboard/trace, rollback,
readiness conclusion và Q&A. Recording fallback phải có timestamp + run/trace/version IDs.

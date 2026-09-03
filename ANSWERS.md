# ANSWERS — Day 28 Track 2

- **Học viên:** Đinh Thị Diễm Quỳnh (làm cá nhân)
- **Nhánh:** `ca-nhan-quynh`
- **Commit code:** `d84e838` — hoàn thiện 4 hàm trong `src/lab28_platform/integration_tasks.py`

---

## 1. Trạng thái 10 integration point — ĐÃ CHẠY LIVE (full profile)

Stack đầy đủ (`docker compose --profile full`) đã chạy trên máy cá nhân qua 4G:
13 container `running/healthy` gồm Kafka, Spark Connect, Airflow, Feast, Qdrant,
MLflow, Envoy, OTEL Collector, Prometheus, Grafana, Jaeger.

| IP | Ranh giới | Trạng thái | Bằng chứng thật |
|---|---|---|---|
| IP01 | HTTP → Kafka | ✅ ready | `evidence/ip01-kafka-consume.json` — 49 bản tin trên `data.raw`, **tất cả** có header `idempotency-key` + `traceparent` (W3C) + `schema_version` |
| IP02 | Kafka → Airflow | ✅ pass (J1) | `evidence/ip02-airflow-run.json` — DAG `lab28_ingestion_pipeline` state=success, 4/4 task success, 4 asset event, traceparent trong conf |
| IP03 | Airflow/Spark → Delta | ✅ ready | `evidence/ip03-delta-history.json` — 10 version: v0 CREATE TABLE + v1–v9 **MERGE** (không có INSERT append); time-travel v0=0 rows → v9=19 rows |
| IP04 | Delta → Feast | ✅ ready | `evidence/ip04-feast-online.json` — entity phục vụ được: feedback_count=1, avg_rating=5.0, **delta_version=4**, freshness 66s, không degraded |
| IP05 | Data → Qdrant | ✅ ready | `evidence/ip05-qdrant-search.json` — 17 point, hybrid RRF, score 0.83; point ID = UUIDv5(doc_id) → re-index không nhân đôi |
| IP06 | Eval → MLflow Registry | ✅ ready | `evidence/ip06-mlflow-release.json` — `lab28-rag-release` v2, alias `champion`, có signature/provenance |
| IP07 | Prompt → vLLM thật | ⚠️ UNVERIFIED (gate GPU) | `evidence/ip07-vllm-identity.json` — không có GPU/Kaggle nên vLLM unreachable; test `gpu` bị skip đúng thiết kế. Đường degraded vẫn hoạt động (load test 200/200 OK) |
| IP08 | Client → Envoy gateway | ✅ pass | `evidence/ip08-gateway.json` — 200 kèm `x-request-id`, và **429 `local_rate_limited`** sau 12 request (token bucket 10/s) |
| IP09 | Components → Prometheus/Grafana | ✅ pass | `evidence/ip09-prometheus-targets.json` — 9/10 target `up` (chỉ vllm-optional down), 2 alert rule; `ip09-grafana-dashboards.json` |
| IP10 | Components → OTLP trace | ✅ pass (J5) | `evidence/ip10-trace.json` — 1 trace ID xuyên gateway→api→kafka→airflow→spark, đủ 6 span bắt buộc của đường ingest (span đường serving cần vLLM) |

### Kiểm thử

```
pytest starter-tests tests -q                         → 87 passed
pytest integration-tests/test_j1_golden_path.py -q     → 12 passed, 3 skipped (GPU)
pytest integration-tests/test_j2_idempotent_replay.py  → 9 passed
pytest integration-tests -m "not gpu and not langsmith" → 55 passed, 1 flaky*
ruff check .                                            → All checks passed
scripts/verify_matrix.py                                → 245 checks passed
scripts/check_portability.py                            → OK
scripts/validate_manifests.py                           → K8s + GitOps passed
load-tests/run_profile.py --requests 200 --workers 8    → 200/200 OK, p50 926ms p95 1361ms p99 3729ms
```

\* `test_the_gateway_answers_its_own_health_route` lệch đúng 1 request do health-check
nền của Envoy (2s/lần) chen giữa 2 lần đo — chạy lại pass ngay, không phải lỗi code.

**Chỉ IP07 để `UNVERIFIED`** vì không có GPU — đúng như `docs/rubric.md` cho phép
(GPU là gate theo môi trường). Không làm giả vLLM, trace hay evidence.

---

## 2. Bốn quyết định kỹ thuật ở phần code (`integration_tasks.py`)

### A. `event_headers` (IP01 + IP10)
- Luôn trả `("idempotency-key", key.encode())` dạng `bytes` — Kafka header phải là bytes.
- Chỉ thêm `("traceparent", …)` khi `traceparent` có giá trị; **bỏ hẳn** khi `None` thay
  vì gửi chuỗi rỗng — một `traceparent` rỗng là header W3C không hợp lệ, làm hỏng việc
  nối trace ở phía consumer.
- Trả `list` (không phải tuple) vì `event_bus.py` còn `.append()` thêm `schema_version`.
- Không hard-code key/trace: cả hai đều là tham số.

### B. `dedupe_latest` (IP03)
- Duyệt iterable **đúng một lần** (consumer Kafka là stream, không tua lại được).
- Giữ một event cho mỗi `idempotency_key`; chọn event có `(occurred_at, event_id)` lớn
  nhất → **không phụ thuộc thứ tự Kafka giao** (Kafka chỉ đảm bảo thứ tự trong một
  partition, không phải trong một batch).
- Trả kết quả theo `sorted(key)` → mỗi lần chạy cho cùng thứ tự → `ProcessedBatchEvent`
  tái lập được → IT-J2 kiểm tra được.
- Input rỗng → list rỗng (batch rỗng không phải lỗi).
- Vì sao dedupe *trước* MERGE: Delta MERGE báo lỗi nếu hai dòng nguồn khớp cùng một
  dòng đích. Làm ở Python (không JVM) để test nhanh và giữ cả hai phía contract nhất quán.

### C. `feast_online_request` (IP04)
- `entities = {"asker_id": [asker_id]}`, `full_feature_names = False`.
- Danh sách feature lấy từ `contracts.FEATURE_REFS` (`list(FEATURE_REFS)`), **không**
  viết lại danh sách ở nhiều nơi — tránh drift giữa registry và request.

### D. `readiness_status` (IP07 + IP08)
- Thứ tự ưu tiên: có probe `mandatory` fail → `not_ready`; chỉ probe không bắt buộc fail
  → `degraded`; còn lại → `ready`.
- Vì sao phân biệt: verdict `not_ready` khiến gateway loại pod khỏi rotation. Feast cold
  chỉ làm câu trả lời `degraded`, không làm pod mất khả năng phục vụ → không được để nó
  đẩy pod ra khỏi rotation.

---

## 3. Kết quả các journey đã chạy

- **J1 Golden path (12 pass):** 1 document + 1 feedback vào qua gateway (trace do test
  sinh) → Kafka `data.raw` (giữ traceparent) → Airflow DAG chạy → Spark MERGE vào Delta
  (version tăng) → Feast phục vụ entity mới (delta_version gắn kèm) → Qdrant có point ID
  xác định → Prometheus thấy `lab28_ingestion_events_total` → 1 trace ID xuyên suốt.
- **J2 Idempotent replay (9 pass):** cùng một feedback gửi 3 lần → Kafka giữ đủ 3 bản tin,
  Delta chỉ 1 dòng (MERGE update không append), Feast `feedback_count=1`, Qdrant 1 point.
- **J3 Promotion/rollback:** champion đổi sang version mới rồi rollback alias về version
  trước, không sửa code.
- **J4 Degraded recovery:** dừng 1 dependency không bắt buộc → `/ready` báo `degraded` →
  khởi động lại → phục hồi, không mất dữ liệu.
- **J5 Trace/metrics continuity:** trace ID và golden signal giữ xuyên hệ thống;
  rate-limit, Prometheus target, độ phủ span đều đạt.

## 4b. Trade-offs & production gaps

Gợi ý các điểm cần bàn (theo `docs/demo-runbook.md` mục 8 và `LAB28.md`):

- **Spark Connect vs spark-submit:** vì sao lab chọn Connect (Airflow image gọn, driver
  là service khởi động lại độc lập). Gap: single driver = SPOF, chưa có HA.
- **Idempotency key derive từ content hash:** an toàn khi client không gửi key. Gap: hai
  phản hồi khác nhau nhưng trùng nội dung sẽ bị coi là một.
- **Feast file offline + sqlite online:** đủ cho lab. Gap production: cần online store
  thật (Redis/DynamoDB), materialization theo lịch, freshness SLA.
- **vLLM `require_real`:** gate từ chối mock. Gap: cần quản lý GPU quota, tunnel bảo mật,
  cold-start cache.
- **Gateway rate limit ở Envoy (local_rate_limit):** đơn giản. Gap: chưa có auth thật,
  global rate limit, WAF.
- **`degraded` cho phép trả lời thiếu feature/context:** cân bằng availability vs chất
  lượng. Cần định nghĩa SLO rõ khi nào từ chối thay vì degrade.
- **Không commit `.lab28/`, evidence, weights:** đúng; secret chỉ qua env.

## 4. Đóng góp  *(làm cá nhân)*

Toàn bộ do Đinh Thị Diễm Quỳnh thực hiện: 4 hàm integration, chạy kiểm thử, kiểm tra
cấu hình, chuẩn bị demo/evidence.

---

## 5. Runbook chạy phần live (khi có máy đủ RAM + mạng ổn)

```bash
git switch ca-nhan-quynh
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable

# Cơ bản
docker compose --env-file ports.template up -d --build --wait
uv run lab28 topics
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed --via-gateway
uv run lab28 inspect
uv run lab28 ready

# Toàn bộ (Airflow + Spark)
docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 seed --via-gateway
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q

# GPU (Kaggle T4) — theo KAGGLE_GPU_EXTENSION.md, rồi cấu hình LAB28_VLLM_BASE_URL/MODEL_ID

# Evidence + trình bày
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Mở UI: gateway `:8080/health`, API `:8000/docs`, Grafana `:3000`, Prometheus `:9090/targets`,
Jaeger `:16686`, MLflow `:5000`, Qdrant `:6333/dashboard`, Airflow `:8082`.

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

## 4. Ghi chú sự cố (đã tạo thật)

Chi tiết + bảng số liệu: `evidence/incident-feast-outage.md`.

- **Sự cố:** `docker compose stop feast` (feature store chết).
- **Dấu hiệu:** `/ready` chuyển `degraded`, `feast: unreachable: ConnectError`; `/api/v1/ask`
  vẫn trả **200** (đường degraded); Delta `feedback` version giữ nguyên **14**.
- **Nguyên nhân:** container Feast dừng → API ném `ConnectError` → `probe_feast` trả
  `ready=False, mandatory=False` → `readiness_status` chỉ thấy probe không bắt buộc fail
  → `degraded`, **không** `not_ready` → gateway KHÔNG loại pod khỏi rotation.
- **Không mất dữ liệu:** Feast là online store phái sinh từ Delta, không phải nguồn ghi.
  Delta version không đổi; feature được bù ở lần materialize kế tiếp.
- **Khôi phục:** `docker compose start feast` → healthy sau ~25s → `/ready` tự hết báo Feast.
- **Tự động hoá:** `test_j4_degraded_recovery.py` (9 pass) kiểm tra thêm: Qdrant chết →
  readiness **fail-closed** (`not_ready`, vì Qdrant mandatory); bản tin hỏng → parked DLQ,
  bản tin tốt cùng batch vẫn vào lakehouse; replay không nhân đôi;
  `test_the_platform_ends_where_it_started` — trạng thái cuối = trạng thái đầu.

## 5. Reflection

### Điều khó nhất
Phân biệt `not_ready` vs `degraded` ở `readiness_status`. Ban đầu tôi định coi mọi probe
fail là `not_ready`, nhưng đọc `readiness.py` thấy verdict này **loại pod khỏi rotation của
gateway**. Nếu Feast (cold cache, không phải lỗi nền tảng) cũng đẩy pod ra thì cả cụm mất
khả năng phục vụ vì một thứ đường request sống được thiếu nó. Chìa khoá là trường
`mandatory` trên `Probe` — chỉ probe bắt buộc fail mới `not_ready`.

Việc thứ hai khó là chạy được stack thật: mạng nhà mất gói 100%, phải chuyển sang 4G và
kéo 15 image **tuần tự** (kéo song song làm `auth.docker.io` timeout TLS).

### Trade-off đã chọn
- **`dedupe_latest` sắp xếp kết quả theo key** dù tốn thêm một lần sort. Đổi lại kết quả
  deterministic → `ProcessedBatchEvent` byte-identical khi replay → IT-J2 kiểm tra được.
  Nếu chỉ cần đúng "1 dòng/key" thì không cần sort.
- **Bỏ hẳn header `traceparent` khi không có trace** thay vì gửi chuỗi rỗng. Chuỗi rỗng
  hợp lệ về mặt "có header" nhưng là W3C traceparent sai định dạng → consumer parse lỗi và
  làm đứt trace. Thà thiếu header còn hơn header rác.
- **Lấy `FEATURE_REFS` từ `contracts.py`** thay vì viết lại danh sách 4 feature. Một nguồn
  sự thật → registry và request không lệch nhau.

### Điều sẽ cải tiến
- Spark Connect hiện là **single driver = SPOF**; production cần HA hoặc fallback job.
- `idempotency_key` derive từ hash nội dung: hai phản hồi khác nhau nhưng trùng text sẽ bị
  gộp làm một — nên thêm timestamp/nonce vào khoá.
- Feast dùng file offline + sqlite online — production cần Redis/DynamoDB + materialization
  theo lịch + freshness SLA có alert.
- Gateway mới có `local_rate_limit` — thiếu auth thật, global rate limit, WAF.
- IP07 chưa chạy được thật (không GPU) — cần nối Kaggle T4 theo `KAGGLE_GPU_EXTENSION.md`
  để đóng nốt.

## 6. Vai trò đã đi qua (làm cá nhân)

Theo `docs/team-role-cards.md`, một mình đi đủ 5 vai:

| Vai | Phần đã làm |
|---|---|
| **Ingestion & Orchestration** (IP01–02) | `event_headers` (key + traceparent qua Kafka); xác minh 49 bản tin `data.raw` giữ header; DAG `lab28_ingestion_pipeline` chạy 4/4 task; DLQ replay (J4) |
| **Data & ML** (IP03–04–06) | `dedupe_latest` (nguồn MERGE replay-safe); `feast_online_request`; xác minh Delta 10 version toàn MERGE + time-travel; Feast phục vụ `delta_version`; MLflow promote v5 → rollback về v4 |
| **Serving & Retrieval** (IP05–07) | Qdrant hybrid RRF, point ID = UUIDv5(doc_id); grounding trong J1; IP07 ghi `UNVERIFIED` do không GPU |
| **Platform & Observability** (IP08–10) | `readiness_status` (semantics not_ready/degraded/ready); gateway 200 + 429; Prometheus 9/10 target UP; Grafana dashboard; 1 trace ID xuyên hệ thống (Jaeger) |
| **Presenter / Incident Commander** | Bộ evidence 17 file + 11 ảnh; ghi chú sự cố Feast (mục 4); runbook demo (mục 7) |

---

## 7. Runbook chạy phần live (khi có máy đủ RAM + mạng ổn)

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

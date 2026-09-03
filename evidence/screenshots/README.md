# Ảnh màn hình cho demo

Cách chụp trên Windows: **Win + Shift + S** (chọn vùng) → dán vào Paint → lưu `.png`
vào chính thư mục này. Đặt tên theo IP để đối chiếu.

Mỗi ảnh nên thấy rõ **thanh URL** và **một ID cụ thể** (trace ID / run ID / version).

| Tên file | Trang | URL | Cần thấy gì |
|---|---|---|---|
| `ip08-gateway-health.png` | Gateway health | http://localhost:8080/health | `{"status":"alive","service":"lab28-api"}` — (ảnh bạn vừa chụp) |
| `ip08-gateway-429.png` | Rate limit | (dùng ảnh terminal của `evidence/ip08-gateway.json`) | dãy `200...429...` |
| `ip02-airflow-dag.png` | Airflow | http://localhost:8082 → DAG `lab28_ingestion_pipeline` | 1 run **success**, 4 task xanh |
| `ip03-delta` | (không có UI) | dùng `evidence/ip03-delta-history.json` | 10 version, toàn MERGE |
| `ip05-qdrant.png` | Qdrant | http://localhost:6333/dashboard → collection `lab28_documents` | số point > 0 |
| `ip06-mlflow.png` | MLflow | http://localhost:5000 → Models → `lab28-rag-release` | version có alias **champion** |
| `ip07-vllm` | — | dùng `evidence/ip07-vllm-identity.json` | unreachable = UNVERIFIED (không GPU) |
| `ip09-grafana.png` | Grafana | http://localhost:3000 (admin/admin) → dashboard **Platform Overview** | các panel có số liệu |
| `ip09-prometheus.png` | Prometheus | http://localhost:9090/targets | 9/10 target **UP** |
| `ip10-jaeger.png` | Jaeger | http://localhost:16686 → service `lab28-api` → mở 1 trace | trace có nhiều span: gateway → api → kafka → airflow → spark |
| `api-docs.png` | API | http://localhost:8000/docs | danh sách endpoint |

## Ảnh quan trọng nhất (theo rubric — "happy path thật")

`ip10-jaeger.png` — mở Jaeger, tìm trace ID trong `evidence/ip10-trace.json`
(vd `b28821ba54d44d0091dd51930fc6f7ed`), chụp cả cây span. Đây là bằng chứng
1 yêu cầu đi xuyên toàn hệ thống.

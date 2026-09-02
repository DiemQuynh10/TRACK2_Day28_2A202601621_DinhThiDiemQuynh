# Facilitator guide

Facilitator không live-code lời giải. Vai trò là giữ 10 boundary thống nhất, hỏi
học viên chỉ ra evidence và giúp phân loại lỗi theo owner.

## Trước buổi học

1. Chạy fast suite và full non-GPU integration suite trên instructor repo.
2. Pull image trước; kiểm tra Core trên Windows/macOS/Linux đại diện.
3. Chuẩn bị full stack dùng chung và vLLM endpoint thật (Kaggle T4x2 được).
4. Chia role, gửi Student repo private, không gửi instructor repo.
5. Mở Grafana, Jaeger, Airflow, MLflow; lưu fallback có timestamp.

## Facilitation loop

Mỗi checkpoint hỏi: input/output contract, state owner, health signal, failure có
mất dữ liệu không, evidence có tái lập không. Chỉ mở hint tiếp theo sau khi nhóm
đưa ra giả thuyết và signal muốn kiểm tra.

| Triệu chứng | Kiểm tra đầu tiên | Owner |
|---|---|---|
| API 503 | `/ready` component list | Serving/Platform |
| DAG không chạy | Airflow health, topics, import errors | Ingestion |
| Delta không tăng version | Spark Connect, table path, task log | Data |
| Feast NOT_FOUND | registry view, snapshot, materialization | Data |
| Retrieval rỗng | collection count, embedding model ID | Serving |
| Trace đứt | event/Delta traceparent, collector metrics | Platform |

Không chấp nhận screenshot xanh thiếu ID/version. Không bắt buộc mọi laptop chạy
full stack; bắt buộc nhóm chứng minh toàn luồng trên hạ tầng chung.

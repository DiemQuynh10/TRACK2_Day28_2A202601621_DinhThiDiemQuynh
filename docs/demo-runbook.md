# Demo runbook

1. Architecture: 5 layer, owner, 10 boundary.
2. Happy path: gửi data qua gateway; trigger Airflow; chỉ Delta/Feast/Qdrant/MLflow; ask.
3. Trace: mở Jaeger bằng trace ID và đối chiếu required spans.
4. Golden signals: rate, errors, duration, saturation và Kafka lag.
5. Incident: dự đoán signal, inject failure, quan sát, recover, chứng minh no data loss.
6. Promotion/rollback: đổi champion, kiểm tra behavior, rollback alias.
7. GitOps: show diff, drift/self-heal và desired-state rollback.
8. Q&A: production gaps, SLO, security, chi phí/GPU.

Fallback recording phải có clock, command và run/trace/version IDs.

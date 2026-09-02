# Builder hints

1. Kafka chỉ commit sau Delta MERGE; invalid payload mới đi DLQ.
2. Spark Connect phân giải path ở server: Compose dùng `/workspace/...`.
3. FeatureView/FeatureService name phải khớp registry/request refs.
4. Qdrant point ID lấy từ `doc_id`, không lấy thứ tự batch.
5. Readiness fail dependency bắt buộc; optional failure phải visible degraded.

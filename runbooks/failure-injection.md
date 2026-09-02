# Failure injection & recovery

Chỉ thao tác service thuộc project `lab28-platform`; ghi timestamp và state trước/sau.

| Scenario | Inject | Expected | Recovery proof |
|---|---|---|---|
| Feast down | `docker compose stop feast` | degraded reason visible | start; lookup present |
| Qdrant down | `docker compose stop qdrant` | not_ready/protected request | start; same count |
| Kafka down | `docker compose stop kafka` | ingestion 503 | start; consume once |
| vLLM down | stop endpoint | degraded/503 per policy | restore; identity passes |

Không dùng `down -v` vì sẽ xóa state. Chỉ replay DLQ sau khi sửa nguyên nhân.

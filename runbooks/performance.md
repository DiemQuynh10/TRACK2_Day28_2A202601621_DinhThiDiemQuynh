# Performance profile

Chạy `uv run python load-tests/run_profile.py --requests 200 --workers 8`, rồi
lặp với 16 workers. Ghi P50/P95/P99, API CPU/RAM, vLLM queue/tokens, Kafka lag và
error rate. `/ready` là baseline; nhóm phải đo thêm `/api/v1/ask` với corpus đại diện.

Không suy ra production capacity từ laptop. Luôn ghi hardware, model, dataset,
concurrency, warm-up và degraded policy.

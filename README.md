# Day 28 Track 2 — Student Lab

Repo này là scaffold riêng cho học viên. Mục tiêu nhóm là hoàn thiện và chứng minh
10 integration points trong `contracts/integration-matrix.yaml`, không phải copy
instructor solution.

## Bắt đầu

```text
uv sync --extra dev --no-editable
uv run pytest -q
```

Baseline có các test fail được ghi rõ vì bốn boundary còn TODO. Chọn role trong
`docs/team-role-cards.md`, đọc `LAB28.md`, rồi sửa implementation thay vì sửa test.

Máy Windows/macOS/Linux đều dùng cùng Python/Compose commands. Máy yếu chạy Core;
nhóm dùng full stack/GPU endpoint chung cho evidence kết nối cuối cùng.

## Student tasks

1. Event headers: giữ W3C trace context và deterministic idempotency key.
2. Delta replay: newest event wins, một row mỗi key.
3. Feast request: feature refs và entity payload đúng registry contract.
4. Readiness: mandatory failure → not_ready; optional failure → degraded.
5. Nối bốn boundary vào Kafka/Airflow/Spark/Feast/Qdrant/MLflow/vLLM path.
6. Thu đủ 10 evidence files và demo theo runbook/rubric.

Không commit credential, runtime state, model weights hoặc evidence chứa dữ liệu nhạy cảm.

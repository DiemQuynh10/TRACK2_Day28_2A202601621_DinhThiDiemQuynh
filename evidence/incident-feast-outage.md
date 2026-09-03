# Ghi chú sự cố — Feast outage (2026-09-03)

## Sự cố đã tạo
`docker compose --env-file ports.template stop feast` — mô phỏng feature store chết.

## Dấu hiệu quan sát
| Nơi | Trước | Trong sự cố | Sau khôi phục |
|---|---|---|---|
| `GET /ready` (qua gateway) | `degraded` (chỉ do vllm) | `degraded` — `feast: unreachable: ConnectError` | `degraded` (lại chỉ do vllm), `feast: ready` |
| `POST /api/v1/ask` | 200 | **200** (vẫn trả lời trên đường degraded) | 200 |
| Delta `feedback` version | 14 | 14 | **14 — không đổi** |
| Verdict xoay pod | không đổi | **không `not_ready`** → gateway KHÔNG loại pod | không đổi |

## Nguyên nhân
Feast container bị dừng → API `httpx` tới `feast:6566` ném `ConnectError`.
Probe `probe_feast` trong `readiness.py` bắt exception, trả `Probe(ready=False, mandatory=False)`.
`readiness_status` thấy chỉ có probe **không bắt buộc** fail → trả `degraded` (không phải `not_ready`).

## Vì sao không mất dữ liệu
- Feast là **online store phái sinh** từ Delta, không phải nguồn ghi. Delta version giữ nguyên 14.
- Đường serving có nhánh degraded: thiếu feature → trả lời với giá trị mặc định + đánh dấu, không 500.
- Khi Feast sống lại, `materialize` ở lần chạy DAG kế tiếp bù lại feature — không cần thao tác tay.

## Cách khôi phục
`docker compose --env-file ports.template start feast` → healthy sau ~25s → `/ready` tự hết báo feast.
Không dùng `lab28 reset` (sẽ xoá state trước sự cố).

## Đối chiếu với J4 (tự động)
`integration-tests/test_j4_degraded_recovery.py` — 9 passed, gồm:
- feature store outage KHÔNG đổi verdict xoay pod
- vector store outage → readiness **fail-closed** (`not_ready`, vì Qdrant là mandatory)
- một bản tin hỏng → parked ở DLQ, bản tin tốt cùng batch vẫn vào lakehouse
- replay không nhân đôi dòng
- `test_the_platform_ends_where_it_started` — trạng thái cuối = trạng thái đầu

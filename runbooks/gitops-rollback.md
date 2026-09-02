# GitOps drift and rollback

1. Chạy `uv run python scripts/validate_manifests.py`.
2. Build immutable image, đổi tag trong Git, review diff.
3. Argo CD sync; kiểm tra health/smoke.
4. Tạo drift ở field dùng cho demo, quan sát self-heal.
5. Revert desired Git revision/image; kiểm tra replicas, gateway và trace.

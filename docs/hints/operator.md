# Operator hints

1. Viết hypothesis/signal trước khi inject failure; chỉ dừng service của lab.
2. So sánh offset, Delta row/version và Qdrant count trước/sau recovery.
3. Load probe: baseline rồi tăng workers; báo P50/P95/P99 + saturation.
4. GitOps rollback đổi desired revision/image, không để live edit undocumented.
5. Secret chỉ qua env/secret manager, không vào ConfigMap/evidence/notebook.

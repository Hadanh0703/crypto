# Open Questions

1. ConfigMaps `*-env` được quản lý ở đâu và giá trị production là gì?
2. Cluster/manifests đã từng deploy thành công chưa? Không có runtime evidence.
3. Bucket MinIO được tạo bằng cách nào, và batch path nào là canonical?
4. Đường collector→Kafka→Spark hay backend direct worker là nguồn chính của `future_kline_5m`?
5. Backend worker có deployment riêng ngoài repository không?
6. Kafka retention/acks/min.insync.replicas thực tế là gì?
7. `CacheTTL` đang được hiểu milliseconds hay seconds với package versions hiện tại?
8. Có data-quality reconciliation giữa 1m, 5m và Binance không?
9. `collect_list` RSI có thứ tự deterministic trong cluster thực tế không?
10. Có backup/restore cho ClickHouse, MinIO, Redis và Kafka không?
11. Tên database production và user permissions là gì?
12. Có dashboards/alerts được tạo thủ công ngoài Git không?
13. OpenAI compatible base URL/model thực tế nào được dùng và có data-retention policy không?

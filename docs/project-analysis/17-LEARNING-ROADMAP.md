# Learning Roadmap

1. **Architecture:** đọc `02`, rồi `infra-k8s` và entry points. Vẽ lại hai ingestion paths. Kiểm tra: vì sao không được coi direct worker là consumer Kafka?
2. **Ingestion:** đọc `WorkerService`, Binance/CoinDesk adapters, RedisKey/lock. Bài tập: mô phỏng checkpoint khi emit thành công nhưng Redis fail.
3. **Kafka:** đọc schema/serializer/producer rồi Spark `read_kafka_stream`. Bài tập: xác định duplicate scenario. Kiểm tra: thiếu key ảnh hưởng ordering thế nào?
4. **Spark Streaming:** trace `prepare_raw_records` → hai writers. Bài tập: từ sample event suy ra Parquet columns và ClickHouse row.
5. **Storage:** đọc DDL và K8s PVC. Bài tập: giải thích ReplacingMergeTree/FINAL và eventual dedup.
6. **Batch:** đọc `kline_batch_job.py` theo thứ tự schema → aggregate → indicators → sink. Tự tính 5 candles và RSI seed.
7. **Signals:** đọc `signal_batch_job.py`; tạo bộ 6 candles thỏa Pump. Phân biệt rule-based với ML.
8. **Backend:** controller → service → repository. Bài tập: trace `/api/indicators?interval=900000` và chỉ ra RSI bị thiếu.
9. **Frontend:** App routes → page → service → component. Bài tập: trace lỗi API thành error state.
10. **Deployment/monitoring:** đọc app→Kafka→Spark→monitor manifests. Bài tập: liệt kê ConfigMap/Secret cần cung cấp và thiết kế freshness alert.

Kết quả mong đợi: có thể giải thích schema/timing/failure ở mỗi cạnh, không chỉ tên công nghệ; có thể phân biệt điều đã code với điều đã vận hành.

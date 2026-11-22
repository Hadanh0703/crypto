# Spark Structured Streaming

Entry `kafka_raw_to_minio.main()` tạo SparkSession UTC, S3A path-style MinIO, graceful shutdown (`kafka_raw_to_minio.py:45-63,224-251`). Source giữ topic/partition/offset/Kafka timestamp/key/raw JSON và parse metadata `type,ticker,interval,fromTime,toTime,nextCheckpoint,syncedAt` (`:32-42,66-105`). Không có watermark, stateful aggregation hoặc data validation sâu.

Raw sink append Parquet mặc định, trigger 30 giây, checkpoint `s3a://<bucket>/checkpoints/kafka-raw`, partition `topic/event_date/event_hour`; Spark tự đặt part filenames (`:108-119`). Structured sink chỉ bật khi `CLICKHOUSE_URL` có giá trị. Nó explode news/klines, cast numeric strings sang Decimal(38,18), thay null bằng 0/empty string và dùng HTTP JSONEachRow theo chunk 5 MiB (`kafka_to_clickhouse.py:126-198,235-310`). Điều này tránh reject nhưng có thể biến dữ liệu xấu thành số 0.

Không có explicit retry/DLQ. `failOnDataLoss=false` ưu tiên tiếp tục khi Kafka mất offsets, có thể mất dữ liệu âm thầm. Hai query có checkpoint riêng nên raw và structured có thể lệch tạm thời hoặc lâu dài khi một sink lỗi.

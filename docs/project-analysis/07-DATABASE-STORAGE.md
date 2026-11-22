# Database & Storage

| Dataset | Producer | Transform | Storage | Consumer |
|---|---|---|---|---|
| Kafka raw | Spark stream | metadata + raw JSON | MinIO Parquet, topic/date/hour | batch/replay |
| `future_kline_1m` | stream | explode/cast | ClickHouse ReplacingMergeTree | chưa thấy API trực tiếp |
| `news` | stream hoặc direct worker | normalize CoinDesk | ClickHouse ReplacingMergeTree | API/news impact |
| `future_kline_5m` | batch hoặc direct worker | OHLCV 5m | ClickHouse ReplacingMergeTree | API/batch signals |
| `kline_indicators` | batch | indicators | ClickHouse ReplacingMergeTree | indicators API |
| `signals` | signal batch | rule detection | ClickHouse ReplacingMergeTree | signals API |

DDL nguồn nằm trong `processing-stream/kafka_to_clickhouse.py:27-70`, `processing-batch/kline_batch_job.py:260-279`, `signal_batch_job.py:40-53`. Không có TTL, secondary index, materialized view hoặc migration framework (`NOT_IMPLEMENTED`). Database mặc định `default`. MinIO bucket mặc định `development`; repo không có code tạo bucket. Redis collector lưu tickers, kline checkpoint/ticker, news checkpoint và locks; backend worker dùng locks, không làm cache API. API cache là in-memory cache-manager.

BTCUSDT lineage chuẩn: Binance Futures 1m → Kafka event → MinIO raw + `future_kline_1m`; batch dự kiến đọc MinIO → `future_kline_5m`/`kline_indicators` → signal job → `signals` → API/UI. Do path mismatch và fallback, đoạn MinIO→batch chưa thể coi là chạy thành công (`CONFIGURED_NOT_EXECUTED`).

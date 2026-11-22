# Apache Kafka

Topic prefix mặc định `local`: `*-ingestion-collector-events`, `*-binance-tickers`, `*-klines-1m`, `*-news` (`schema.ts:3-8`). Admin tạo cả bốn topic, 3 partitions, replication 1 (`worker-kafka.service.ts:20-48`); chỉ kline/news được emit trong WorkerService. Message không có key nên phân phối partition không đảm bảo cùng ticker vào cùng partition. Serializer dùng JSON-bigint cho object (`serializer.ts:6-47`).

Ví dụ dựa trên schema (giá trị minh họa):

```json
{"type":"Klines1mSynced","ticker":"BTCUSDT","interval":"1m","fromTime":1710000000000,"toTime":1710000060000,"nextCheckpoint":1710000120000,"klines":[{"ticker":"BTCUSDT","openTime":1710000000000,"open":"60000","high":"60100","low":"59900","close":"60050","volume":"12.5","closeTime":1710000059999,"quoteAssetVolume":"750000","numOfTrades":120,"takerBuyBaseAssetVolume":"6","takerBuyQuoteAssetVolume":"360000","ignore":"0"}],"syncedAt":"2026-01-01T00:00:00.000Z"}
```

Spark subscribes kline/news, `startingOffsets=latest`, `failOnDataLoss=false`, optional max offsets, checkpoint per sink. Khi consumer dừng, Kafka giữ backlog theo broker retention (retention không cấu hình trong repo: `UNKNOWN`). Khi Kafka lỗi, collector catch ở cron; checkpoint không tăng, lần sau thử lại. Không có retry/backoff producer rõ ràng ngoài KafkaJS defaults. Sink ClickHouse là foreachBatch + HTTP insert; crash giữa insert/checkpoint có thể duplicate. ReplacingMergeTree chỉ dedup sau merge và key của news là `(publishedOn,id)`, kline `(ticker,openTime)`.

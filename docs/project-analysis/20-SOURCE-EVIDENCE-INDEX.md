# Source Evidence Index

| Claim | Status | Evidence |
|---|---|---|
| Binance Futures 1m ingestion mỗi phút | VERIFIED_IN_CODE | `ingestion-collector/src/modules/worker/worker.service.ts:168-224`; `libs/binance/binance.service.ts:11-31` |
| CoinDesk sentiment là dữ liệu nguồn | VERIFIED_IN_CODE | `coindesk.service.ts:37-55` |
| Kafka topics 3 partitions/RF1 | VERIFIED_IN_CODE | `worker-kafka.service.ts:27-45` |
| Raw Parquet topic/date/hour | VERIFIED_IN_CODE | `processing-stream/.../kafka_raw_to_minio.py:108-119` |
| ClickHouse 1m/news schemas | VERIFIED_IN_CODE | `processing-stream/.../kafka_to_clickhouse.py:27-70` |
| 5m OHLCV aggregation | VERIFIED_IN_CODE | `processing-batch/kline_batch_job.py:116-133` |
| Wilder RSI, SMA, sample Bollinger, volume spike | VERIFIED_IN_CODE | `kline_batch_job.py:41-71,141-167` |
| Pump/Dump thresholds | VERIFIED_IN_CODE | `signal_batch_job.py:88-127` |
| News impact bucket math | VERIFIED_IN_CODE | `backend-api/src/news-impact/repositories/clickhouse-news-impact.repository.ts:28-36,177-209` |
| API inventory | VERIFIED_IN_CODE | controllers dưới `backend-api/src/**` |
| React pages/routes | VERIFIED_IN_CODE | `frontend-dashboard/src/app/App.tsx:11-21` |
| LLM function calling, no RAG | VERIFIED_IN_CODE / NOT_IMPLEMENTED | `backend-api/src/chatbot/chatbot.service.ts:23-127,187-319`; repo search |
| K8s deployment topology | CONFIGURED_NOT_EXECUTED | `infra-k8s/**` |
| Monitoring stack | CONFIGURED_NOT_EXECUTED | `infra-k8s/monitor/**` |
| End-to-end production success | UNKNOWN | no runtime/test evidence |

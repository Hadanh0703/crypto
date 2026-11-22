# Data Ingestion

## Collector chính

`WorkerService.syncBinanceTickers()` và `syncKlines1m()` chạy mỗi phút (`worker.service.ts:40-60,168-247`). Binance Futures endpoints là `/fapi/v2/ticker/price` và `/fapi/v1/klines` (`binance.service.ts:4-31`). Tất cả ticker trả về được thu thập; không lọc contract status. Kline 1m, tối đa 95 bản ghi/request, concurrency theo lô 10. `toTime` được floor xuống phút hiện tại, nên request có thể gồm candle đang mở; code không kiểm tra closeTime/closed flag (`VERIFIED_IN_CODE`, rủi ro High).

Checkpoint Redis `binance_klines_1m_checkpoint:<ticker>` không TTL; mặc định lùi 60 phút. Checkpoint chỉ cập nhật sau khi `emit` hoàn tất, tạo at-least-once khi publish thành công nhưng Redis write lỗi. Lock `flag:is_locking_sync_new_kline` TTL 60 giây. Release bằng `DEL` không token ownership, nên instance chậm có thể xóa lock mới của instance khác.

News chạy mỗi phút (`worker.service.ts:249-306`), đọc CoinDesk `/news/v1/article/list`, `lang=EN`, limit 100, `to_ts`, API key round-robin. Sentiment đến trực tiếp từ CoinDesk. Window 1 giờ; filter `(from,to]`, sort tăng dần, checkpoint bằng max published timestamp hoặc `toTime`. Không deduplicate GUID/ID trước Kafka.

## Đường direct worker

`backend-api/src/worker/binance-sync.service.ts` và `coindesk-sync.service.ts` là ingestion độc lập ghi ClickHouse qua `WorkerKlineService`/`WorkerNewsService`. Nó backfill theo `BACKFILL_MONTHS`; Binance dùng nến 5m. Không có manifest chạy `start:worker:prod`, nên đây là code tồn tại nhưng chưa được K8s hiện tại kích hoạt (`CONFIGURED_NOT_EXECUTED`).

# Spark Batch

## 1m → 5m

`kline_batch_job.process_klines()` đọc hard-coded `s3a://development/raw/kafka/ingestion-collector/topic=local-.../*/*` (`:73-82`). Stream thực tế ghi `raw/kafka/topic=.../event_date=.../event_hour=...`; có thêm segment `ingestion-collector` trong batch path nên khả năng không khớp (`VERIFIED_IN_CODE`, High).

Bucket = `floor(openTime/300000)*300000`; open=first theo openTime, high=max, low=min, close=last, volume=sum (`:116-133`). Không bắt buộc đủ 5 candles, không kiểm tra gap/duplicate trước sum và không tạo closeTime/các volume phụ dù table yêu cầu. Ví dụ: closes 100,101,99,103,102; highs 101,102,100,104,103; lows 99,100,98,102,101; volumes 1..5 → output open=100, high=104, low=98, close=102, volume=15.

## Chỉ báo

- SMA20/SMA50: `avg(close)` trên 20/50 dòng gồm dòng hiện tại; Spark cho kết quả ngay cả khi window chưa đủ.
- Bollinger: SMA20 ± 2 × `stddev(close)`; PySpark `stddev` là sample standard deviation, khác API higher-timeframe dùng `stddevPop`.
- RSI14: UDF cần ≥15 giá, seed 14 delta rồi Wilder smoothing; collect_list cửa sổ tối đa 101 candles. Không có đảm bảo rõ ràng về thứ tự collect_list sau window (`rủi ro`).
- Volume spike: current volume > 3 × trung bình 7 candles trước, loại current.

Chỉ ba candle mới nhất/ticker được insert. `ReplacingMergeTree(version)` giúp eventual dedup cho kline/indicator. Nếu MinIO lỗi, job gọi Binance **Spot** `/api/v3/klines` cho BTCUSDT rồi static mock; đây có thể tạo dữ liệu giả trong bảng production (`Critical`).

## Pump/Dump

Signal job đọc 2 giờ gần nhất, change = `(close-prev_close)/prev_close*100`; avg volume của 5 dòng trước. Pump nếu change ≥0.2% AND volume ≥1.2×avg; Dump nếu ≤-0.2% AND volume ≥1.2×avg (`signal_batch_job.py:56-127`). Dòng đầu có avg null nên không signal. Mỗi run insert UUID mới; ORDER BY `(timestamp,ticker)` nhưng UUID không tham gia key, ReplacingMergeTree có thể collapse Pump/Dump trùng timestamp nhưng rerun tạo duplicates trước merge. Đây là rule-based, không phải ML.

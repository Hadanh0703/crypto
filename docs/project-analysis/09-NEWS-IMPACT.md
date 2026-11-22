# News Impact

News timestamp là epoch seconds, đổi sang ms rồi floor vào bucket 5 phút chứa thời điểm publish (`clickhouse-news-impact.repository.ts:28-36`). `priceAtPublish` là **close của bucket chứa tin**, có thể ở sau thời điểm tin tới gần 5 phút; không phải giá tức thời. Các giá sau 5m/15m/1h/4h là close tại bucket +1/+3/+12/+48. Change = `(price_after-price_publish)/price_publish*100`, 4 decimals (`:151-210`). Missing candle trả `null`.

Markers query news trước rồi lookup các openTime qua `IN`; impact table làm tương tự cho +15m/+1h. Sentiment correlation giới hạn 100 tin mới nhất, nhóm theo sentiment và average các changes khả dụng. Sentiment là trường nguồn CoinDesk, không được model nội bộ tính.

Ví dụ minh họa: tin 10:02 nằm bucket 10:00, close 60,000; bucket 10:15 close 60,300 → impact 15m theo code = `(60300-60000)/60000×100=0.5000%`. Đây chỉ là tương quan thời gian; thị trường có nhiều yếu tố đồng thời nên không chứng minh nhân quả.

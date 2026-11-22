# Backend API

Global prefix `/api`, CORS mở, ValidationPipe whitelist/transform, exception filter, response interceptor và Swagger `/api/docs` (`main.ts:10-37`). Không có authentication/authorization.

| Method | Endpoint | Data source | Behavior |
|---|---|---|---|
| GET | `/api` | static | health text |
| GET | `/api/kline/klines` | `future_kline_5m FINAL` | aggregate timeframe; yêu cầu multiple 5m và đủ count |
| GET | `/api/kline/time-now` | clock | epoch ms |
| GET | `/api/indicators` | kline + indicators | ma/ema/rsi/bb |
| GET | `/api/news` | `news FINAL` | pagination/filter/search |
| GET | `/api/news/limit` | `news FINAL` | latest range |
| GET | `/api/signals/active` | `signals` | latest optional ticker |
| GET | `/api/overview/market-summary` | kline/news | BTC/ETH + sentiment |
| GET | `/api/overview/top-movers` | kline | 5 gainers/losers |
| GET | `/api/overview/volume-spike` | kline | 24h vs daily avg prior 6 days |
| GET | `/api/news-impact/*` | news+kline | markers/impact/correlation |
| POST | `/api/chatbot` | OpenAI + internal API | one message, tool loop |

`USE_MOCK` mặc định `true` trong env validation; module repository chọn mock theo env. Input thời gian/limit chủ yếu là string rồi `Number()`; thiếu validation min/max ở nhiều DTO. Cache TTL decorators ghi 60000 với cache-manager v7; cần runtime verify đơn vị (`UNKNOWN`). Signals repository nuốt lỗi và trả `[]`, có thể che outage.

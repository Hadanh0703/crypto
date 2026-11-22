# Ingestion Collector

## Commands

- `yarn dev` starts the main NestJS app in watch mode.
- `yarn dev:worker` starts the worker app in watch mode.
- `yarn build` builds the app.

## Runtime

The service only requires Redis. The worker syncs Binance futures ticker symbols every minute and stores them in Redis under `binance_tickers`.

Kafka can be enabled with `KAFKA_RUNNING_FLAG=true`. The setup matches `viral-bot-be`: Kafka clients use `KAFKA_BROKER`, topics are prefixed with `KAFKA_TOPIC_PREFIX`, and the worker creates missing topics on startup.

The worker also syncs Binance `klines_1m` incrementally. Checkpoints are stored per ticker in Redis under `binance_klines_1m_checkpoint:<ticker>`; if a ticker has no checkpoint, the worker starts from one hour before the current time. Synced rows are emitted to the Kafka topic `<KAFKA_TOPIC_PREFIX>-ingestion-collector-klines-1m`.

The worker also syncs CoinDesk news incrementally. The checkpoint is stored in Redis under `coindesk_news_checkpoint`; if it is missing, the worker starts from one hour before the current time. Synced rows are emitted to `<KAFKA_TOPIC_PREFIX>-ingestion-collector-news`. Set `COINDESK_API_KEYS` to a comma-separated list of API keys when CoinDesk requires authentication.

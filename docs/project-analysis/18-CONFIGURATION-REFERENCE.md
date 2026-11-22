# Configuration Reference

| Area | Variables/defaults | Role |
|---|---|---|
| Collector | `NODE_ENV` required; `PORT=3000`; `WORKER_PORT=3001`; `REDIS_URL` required; `KAFKA_BROKER=localhost:39092`; `KAFKA_TOPIC_PREFIX=local`; `KAFKA_RUNNING_FLAG=false`; `COINDESK_API_KEYS` | ingestion |
| Stream | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS`, `KAFKA_STARTING_OFFSETS=latest`, `KAFKA_MAX_OFFSETS_PER_TRIGGER`, `MINIO_HOST/PORT/SECURE`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `BUCKET_NAME=development`, `RAW_OUTPUT_PATH`, `CHECKPOINT_PATH`, `CLICKHOUSE_URL`, `CLICKHOUSE_CHECKPOINT_PATH`, `OUTPUT_FORMAT=parquet`, `TRIGGER_PROCESSING_TIME=30 seconds` | streaming |
| Batch | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `CLICKHOUSE_URL` hoặc host/http-port/user/password | analytics |
| API | `PORT=3000`, `WORKER_PORT=3002`, `CLICKHOUSE_*`, `REDIS_URL`, `BACKFILL_MONTHS=6`, `COINDESK_API_KEYS`, `USE_MOCK=true`, `OPENAI_API_KEY`, `OPENAI_MODEL=gpt-4o-mini`, `INTERNAL_API_BASE` | REST/worker/LLM |
| UI | `VITE_API_BASE_URL=/api`, `VITE_USE_MOCK=false` in K8s | browser |

Ports: Redis 6379; MinIO 9000/console 9001; ClickHouse HTTP 8123/native 9000/metrics 9363; Kafka 9092; ZooKeeper 2181; API 3000; collector K8s worker 5002; frontend 80; Spark UI 4040. Không ghi secret values. ConfigMap definitions cho runtime app variables không đầy đủ trong repo.

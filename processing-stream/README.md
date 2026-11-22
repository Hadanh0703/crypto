# Processing Stream

PySpark Structured Streaming job for the Crypto Intelligence pipeline.

```text
ingestion-collector -> Kafka -> PySpark Structured Streaming -> MinIO raw data lake
                                                     \-> ClickHouse analytics tables
```

The job consumes Kafka events emitted by `ingestion-collector`, stores replayable raw data in MinIO using the S3A connector, and can also store parsed news/kline records in ClickHouse when `CLICKHOUSE_URL` is set.

## What It Reads

Local defaults follow the Kafka config from `/ingestion-collector`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:39092
KAFKA_TOPIC_PREFIX=local
KAFKA_TOPICS=local-ingestion-collector-events,local-ingestion-collector-binance-tickers,local-ingestion-collector-klines-1m,local-ingestion-collector-news
```

The active producer currently emits useful data mainly to:

- `local-ingestion-collector-klines-1m`
- `local-ingestion-collector-news`

The other topics are included so Spark can consume them when the producer starts emitting those events later.

## What It Writes

Default raw output:

```text
s3a://development/raw/kafka/ingestion-collector
```

Default checkpoint path:

```text
s3a://development/checkpoints/processing-stream/kafka-raw
```

Records are written as Parquet and partitioned by:

- `topic`
- `event_date`
- `event_hour`

Each row includes:

- Kafka metadata: `topic`, `partition`, `offset`, `kafka_timestamp`, `kafka_key`
- original Kafka payload: `raw_value`
- parsed metadata: `event_type`, `ticker`, `synced_at`
- processing timestamp: `ingested_at`

When `CLICKHOUSE_URL` is configured, a second streaming sink writes:

- news events to `news`
- 1-minute futures kline events to `future_kline_1m`

## Prerequisites

Install Java before running Spark. Java 17 is a good local default:

```bash
brew install openjdk@17
```

If `java -version` still fails, export `JAVA_HOME` for your shell. On Apple Silicon Homebrew this is usually:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

On Intel Homebrew this is usually:

```bash
export JAVA_HOME=/usr/local/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

Verify:

```bash
java -version
```

## Setup Python

Create and install the local virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Verify PySpark is installed:

```bash
.venv/bin/python -c "import pyspark; print(pyspark.__version__)"
```

## Configure Environment

This repo already includes a local `.env` for the current development setup:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:39092
KAFKA_BROKER=localhost:39092
KAFKA_TOPIC_PREFIX=local
KAFKA_TOPICS=local-ingestion-collector-events,local-ingestion-collector-binance-tickers,local-ingestion-collector-klines-1m,local-ingestion-collector-news
KAFKA_STARTING_OFFSETS=earliest
KAFKA_MAX_OFFSETS_PER_TRIGGER=50000

MINIO_ACCESS_KEY=development
MINIO_SECRET_KEY=123456789
BUCKET_NAME=development
MINIO_HOST=localhost
MINIO_PORT=32126
MINIO_SECURE=false

RAW_OUTPUT_PATH=s3a://development/raw/kafka/ingestion-collector
CHECKPOINT_PATH=s3a://development/checkpoints/processing-stream/kafka-raw
OUTPUT_FORMAT=parquet
TRIGGER_PROCESSING_TIME=30 seconds
PROGRESS_LOG_INTERVAL_SECONDS=10

CLICKHOUSE_URL=http://default:1@localhost:8123/default
CLICKHOUSE_CHECKPOINT_PATH=s3a://development/checkpoints/processing-stream/clickhouse
```

Use `.env.example` as the template if `.env` is missing.

## Start Kafka

Kafka is provided by `ingestion-collector`:

```bash
cd /ingestion-collector
docker compose up -d zookeeper kafka
```

Verify Kafka is reachable:

```bash
nc -z localhost 39092
```

To make `ingestion-collector` emit Kafka events, set this in `/ingestion-collector/.env`:

```env
KAFKA_RUNNING_FLAG=true
```

Then run the worker app from `ingestion-collector` using that repo's normal command.

## Start MinIO

Spark expects MinIO at:

```text
http://localhost:32126
```

with:

```env
MINIO_ACCESS_KEY=development
MINIO_SECRET_KEY=123456789
BUCKET_NAME=development
```

Verify the port is reachable:

```bash
nc -z localhost 32126
```

Create the bucket named `development` before starting the Spark stream. You can create it from the MinIO console or with your local MinIO client.

## Start ClickHouse

ClickHouse is optional. When enabled, the job expects an HTTP URL:

```env
CLICKHOUSE_URL=http://default:1@localhost:8123/default
```
curl -u default:1 'http://localhost:8123/?query=SELECT%201'
The job creates the `news` and `future_kline_1m` tables on startup with `CREATE TABLE IF NOT EXISTS`. The ClickHouse sink has its own checkpoint at `CLICKHOUSE_CHECKPOINT_PATH`, so it can progress independently from the raw MinIO sink.

## Run The Stream

From this repo:

```bash
cd /processing-stream
. .venv/bin/activate
```

Run Spark with the Kafka and S3A packages:

```bash
PYSPARK_PYTHON="$(pwd)/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python" \
PYTHONPATH=src .venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  src/processing_stream/kafka_raw_to_minio.py
```

The first run downloads Spark connector jars from Maven, so it needs network access.

The `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` variables are intentional. PySpark's `spark-submit` wrapper otherwise calls `python3` from your shell PATH while resolving `SPARK_HOME`; if that Python cannot import `pyspark`, Spark may fail with `/bin/spark-class: No such file or directory`.

The app logs progress every `PROGRESS_LOG_INTERVAL_SECONDS`. If Kafka has no messages yet, you should still see logs saying the query is active and waiting for the first micro-batch. When data arrives, logs include `batchId`, `numInputRows`, and Kafka offset ranges.

## Run As JSON Instead Of Parquet

Parquet is the default. For easier manual inspection during local testing:

```bash
OUTPUT_FORMAT=json \
PYSPARK_PYTHON="$(pwd)/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python" \
PYTHONPATH=src .venv/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  src/processing_stream/kafka_raw_to_minio.py
```

## Local Health Checklist

Before running the stream, these should pass:

```bash
java -version
.venv/bin/python -c "import pyspark; print(pyspark.__version__)"
nc -z localhost 39092
nc -z localhost 32126
nc -z localhost 8123
```

Also check:

- bucket `development` exists in MinIO
- ClickHouse is reachable if `CLICKHOUSE_URL` is set
- `ingestion-collector/.env` has `KAFKA_RUNNING_FLAG=true`
- `ingestion-collector` worker is running
- Kafka topics use the same `KAFKA_TOPIC_PREFIX=local`

## Notes For Kubernetes

Do not use `localhost` in Kubernetes. For K8s deployment, replace Kafka and MinIO hosts with service DNS names, for example:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka.default.svc.cluster.local:9092
MINIO_HOST=minio.default.svc.cluster.local
MINIO_PORT=9000
```

For production, package this job into a Docker image and run it with Spark Operator or Spark native Kubernetes submit.

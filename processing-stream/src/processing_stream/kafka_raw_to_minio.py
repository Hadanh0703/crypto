from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    from_json,
    to_date,
)
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing_stream.config import StreamConfig
from processing_stream.kafka_to_clickhouse import (
    ensure_clickhouse_tables,
    write_clickhouse_stream,
)


LOGGER = logging.getLogger("processing-stream")


EVENT_METADATA_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("fromTime", LongType(), True),
        StructField("toTime", LongType(), True),
        StructField("nextCheckpoint", LongType(), True),
        StructField("syncedAt", StringType(), True),
    ]
)


def build_spark(config: StreamConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("processing-stream-kafka-raw-to-minio")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.minio_secure).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, config: StreamConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", ",".join(config.kafka_topics))
        .option("startingOffsets", config.kafka_starting_offsets)
        .option("failOnDataLoss", "false")
    )

    if config.kafka_max_offsets_per_trigger:
        reader = reader.option(
            "maxOffsetsPerTrigger", config.kafka_max_offsets_per_trigger
        )

    return reader.load()


def prepare_raw_records(kafka_df: DataFrame) -> DataFrame:
    payload = col("value").cast("string")
    parsed_payload = from_json(payload, EVENT_METADATA_SCHEMA)

    return (
        kafka_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestamp_type"),
            col("key").cast("string").alias("kafka_key"),
            payload.alias("raw_value"),
            parsed_payload.alias("event"),
        )
        .withColumn("event_type", col("event.type"))
        .withColumn("ticker", col("event.ticker"))
        .withColumn("synced_at", col("event.syncedAt"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("event_date", to_date(col("kafka_timestamp")))
        .withColumn("event_hour", date_format(col("kafka_timestamp"), "HH"))
        .drop("event")
    )


def write_raw_stream(raw_df: DataFrame, config: StreamConfig):
    writer = (
        raw_df.writeStream.format(config.output_format)
        .queryName("kafka_raw_to_minio")
        .option("checkpointLocation", config.checkpoint_path)
        .option("path", config.raw_output_path)
        .outputMode("append")
        .partitionBy("topic", "event_date", "event_hour")
        .trigger(processingTime=config.trigger_processing_time)
    )

    return writer.start()


def _source_summary(progress: dict[str, Any]) -> str:
    summaries = []
    for source in progress.get("sources", []):
        description = source.get("description", "unknown-source")
        start_offset = source.get("startOffset")
        end_offset = source.get("endOffset")
        summaries.append(
            f"{description} startOffset={start_offset} endOffset={end_offset}"
        )
    return " | ".join(summaries) if summaries else "no source progress"


def monitor_query(query: StreamingQuery, config: StreamConfig) -> None:
    LOGGER.info("Started Spark query id=%s runId=%s", query.id, query.runId)

    last_logged_batch_id = None
    while query.isActive:
        query.awaitTermination(config.progress_log_interval_seconds)
        progress = query.lastProgress

        if not progress:
            LOGGER.info(
                "Spark query is active; waiting for first micro-batch. status=%s",
                query.status,
            )
            continue

        batch_id = progress.get("batchId")
        if batch_id == last_logged_batch_id:
            LOGGER.info(
                "Spark query is active; no new batch since batchId=%s. status=%s",
                batch_id,
                query.status,
            )
            continue

        last_logged_batch_id = batch_id
        LOGGER.info(
            "Spark batch completed: batchId=%s rows=%s inputRowsPerSecond=%s processedRowsPerSecond=%s durationMs=%s",
            batch_id,
            progress.get("numInputRows"),
            progress.get("inputRowsPerSecond"),
            progress.get("processedRowsPerSecond"),
            progress.get("durationMs"),
        )
        LOGGER.info("Spark batch sources: %s", _source_summary(progress))

    query.awaitTermination()


def monitor_queries(queries: list[StreamingQuery], config: StreamConfig) -> None:
    for query in queries:
        LOGGER.info(
            "Started Spark query name=%s id=%s runId=%s",
            query.name,
            query.id,
            query.runId,
        )

    last_logged_batch_ids: dict[str, int | None] = {
        str(query.id): None for query in queries
    }
    while any(query.isActive for query in queries):
        for query in queries:
            query.awaitTermination(config.progress_log_interval_seconds)
            progress = query.lastProgress

            if not progress:
                LOGGER.info(
                    "Spark query name=%s is active; waiting for first micro-batch. status=%s",
                    query.name,
                    query.status,
                )
                continue

            batch_id = progress.get("batchId")
            query_id = str(query.id)
            if batch_id == last_logged_batch_ids[query_id]:
                LOGGER.info(
                    "Spark query name=%s is active; no new batch since batchId=%s. status=%s",
                    query.name,
                    batch_id,
                    query.status,
                )
                continue

            last_logged_batch_ids[query_id] = batch_id
            LOGGER.info(
                "Spark batch completed: queryName=%s batchId=%s rows=%s inputRowsPerSecond=%s processedRowsPerSecond=%s durationMs=%s",
                query.name,
                batch_id,
                progress.get("numInputRows"),
                progress.get("inputRowsPerSecond"),
                progress.get("processedRowsPerSecond"),
                progress.get("durationMs"),
            )
            LOGGER.info("Spark batch sources: %s", _source_summary(progress))

    for query in queries:
        query.awaitTermination()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    config = StreamConfig.from_env()
    LOGGER.info("Reading Kafka topics: %s", ", ".join(config.kafka_topics))
    LOGGER.info("Kafka bootstrap servers: %s", config.kafka_bootstrap_servers)
    LOGGER.info("Kafka starting offsets: %s", config.kafka_starting_offsets)
    LOGGER.info("MinIO endpoint: %s", config.minio_endpoint)
    LOGGER.info("Writing raw stream to: %s", config.raw_output_path)
    LOGGER.info("Writing checkpoint to: %s", config.checkpoint_path)
    if config.clickhouse_url:
        LOGGER.info(
            "Writing ClickHouse checkpoint to: %s", config.clickhouse_checkpoint_path
        )
    else:
        LOGGER.info("CLICKHOUSE_URL is not set; ClickHouse sink is disabled")

    spark = build_spark(config)
    ensure_clickhouse_tables(config)
    kafka_df = read_kafka_stream(spark, config)
    raw_df = prepare_raw_records(kafka_df)
    queries = [write_raw_stream(raw_df, config)]
    if config.clickhouse_url:
        queries.append(write_clickhouse_stream(raw_df, config))
    monitor_queries(queries, config)


if __name__ == "__main__":
    main()

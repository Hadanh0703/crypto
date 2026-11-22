from __future__ import annotations

import base64
import logging
import urllib.parse
import urllib.request
from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, explode, from_json, lit, to_json
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from processing_stream.config import StreamConfig


LOGGER = logging.getLogger("processing-stream")
CLICKHOUSE_INSERT_CHUNK_BYTES = 5 * 1024 * 1024

CLICKHOUSE_NEWS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS news (
    id UInt64,
    type String,
    guid String,
    url String,
    publishedOn UInt64,
    imageUrl String,
    title String,
    subtitle String,
    authors String,
    sourceId String,
    rawBody String,
    keywords String,
    sentiment String,
    sourceName String,
    categories String,
    htmlBody String,
    version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime(publishedOn))
ORDER BY (publishedOn, id)
"""

CLICKHOUSE_FUTURE_KLINE_1M_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS future_kline_1m (
    ticker String,
    openTime UInt64,
    open Decimal(38, 18),
    high Decimal(38, 18),
    low Decimal(38, 18),
    close Decimal(38, 18),
    volume Decimal(38, 18),
    closeTime UInt64,
    quoteAssetVolume Decimal(38, 18),
    numOfTrades UInt32,
    takerBuyBaseAssetVolume Decimal(38, 18),
    takerBuyQuoteAssetVolume Decimal(38, 18),
    ignore Decimal(38, 18),
    version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(toDateTime(openTime / 1000))
ORDER BY (ticker, openTime)
"""

NEWS_ITEM_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("guid", StringType(), True),
        StructField("url", StringType(), True),
        StructField("publishedOn", LongType(), True),
        StructField("imageUrl", StringType(), True),
        StructField("title", StringType(), True),
        StructField("subtitle", StringType(), True),
        StructField("authors", StringType(), True),
        StructField("sourceId", StringType(), True),
        StructField("rawBody", StringType(), True),
        StructField("keywords", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("sourceName", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("htmlBody", StringType(), True),
    ]
)

NEWS_EVENT_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("news", ArrayType(NEWS_ITEM_SCHEMA), True),
    ]
)

KLINE_ITEM_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), True),
        StructField("openTime", LongType(), True),
        StructField("open", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("closeTime", LongType(), True),
        StructField("quoteAssetVolume", StringType(), True),
        StructField("numOfTrades", IntegerType(), True),
        StructField("takerBuyBaseAssetVolume", StringType(), True),
        StructField("takerBuyQuoteAssetVolume", StringType(), True),
        StructField("ignore", StringType(), True),
    ]
)

KLINES_1M_EVENT_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("klines", ArrayType(KLINE_ITEM_SCHEMA), True),
    ]
)


def _string_value(column_name: str):
    return coalesce(col(column_name), lit(""))


def _long_value(column_name: str):
    return coalesce(col(column_name), lit(0))


def _decimal_value(column_name: str):
    return coalesce(col(column_name).cast(DecimalType(38, 18)), lit("0").cast(DecimalType(38, 18)))


def prepare_clickhouse_news(raw_df: DataFrame) -> DataFrame:
    parsed_df = raw_df.withColumn("clickhouse_event", from_json("raw_value", NEWS_EVENT_SCHEMA))
    return (
        parsed_df.filter(
            col("topic").contains("news")
            | (col("event_type") == "NewsSynced")
            | (col("clickhouse_event.type") == "NewsSynced")
        )
        .select(explode(col("clickhouse_event.news")).alias("item"))
        .select(
            _long_value("item.id").alias("id"),
            _string_value("item.type").alias("type"),
            _string_value("item.guid").alias("guid"),
            _string_value("item.url").alias("url"),
            _long_value("item.publishedOn").alias("publishedOn"),
            _string_value("item.imageUrl").alias("imageUrl"),
            _string_value("item.title").alias("title"),
            _string_value("item.subtitle").alias("subtitle"),
            _string_value("item.authors").alias("authors"),
            _string_value("item.sourceId").alias("sourceId"),
            _string_value("item.rawBody").alias("rawBody"),
            _string_value("item.keywords").alias("keywords"),
            _string_value("item.sentiment").alias("sentiment"),
            _string_value("item.sourceName").alias("sourceName"),
            coalesce(to_json(col("item.categories")), lit("[]")).alias("categories"),
            _string_value("item.htmlBody").alias("htmlBody"),
        )
    )


def prepare_clickhouse_future_kline_1m(raw_df: DataFrame) -> DataFrame:
    parsed_df = raw_df.withColumn(
        "clickhouse_event", from_json("raw_value", KLINES_1M_EVENT_SCHEMA)
    )
    return (
        parsed_df.filter(
            col("topic").contains("klines-1m")
            | (col("event_type") == "Klines1mSynced")
            | (col("clickhouse_event.type") == "Klines1mSynced")
        )
        .select(explode(col("clickhouse_event.klines")).alias("item"))
        .select(
            _string_value("item.ticker").alias("ticker"),
            _long_value("item.openTime").alias("openTime"),
            _decimal_value("item.open").alias("open"),
            _decimal_value("item.high").alias("high"),
            _decimal_value("item.low").alias("low"),
            _decimal_value("item.close").alias("close"),
            _decimal_value("item.volume").alias("volume"),
            _long_value("item.closeTime").alias("closeTime"),
            _decimal_value("item.quoteAssetVolume").alias("quoteAssetVolume"),
            coalesce(col("item.numOfTrades"), lit(0)).alias("numOfTrades"),
            _decimal_value("item.takerBuyBaseAssetVolume").alias(
                "takerBuyBaseAssetVolume"
            ),
            _decimal_value("item.takerBuyQuoteAssetVolume").alias(
                "takerBuyQuoteAssetVolume"
            ),
            _decimal_value("item.ignore").alias("ignore"),
        )
    )


def _clickhouse_request_parts(
    clickhouse_url: str, query: str
) -> tuple[str, dict[str, str]]:
    parsed_url = urllib.parse.urlsplit(clickhouse_url)
    query_params = urllib.parse.parse_qsl(parsed_url.query, keep_blank_values=True)
    headers = {"Content-Type": "application/x-ndjson"}

    username = urllib.parse.unquote(parsed_url.username or "")
    password = urllib.parse.unquote(parsed_url.password or "")
    if username:
        credentials = f"{username}:{password}".encode("utf-8")
        headers["Authorization"] = f"Basic {base64.b64encode(credentials).decode('ascii')}"

    host = parsed_url.hostname or ""
    if parsed_url.port:
        host = f"{host}:{parsed_url.port}"

    database = parsed_url.path.strip("/")
    if database and not any(name == "database" for name, _ in query_params):
        query_params.append(("database", database))
    query_params.append(("query", query))

    request_url = urllib.parse.urlunsplit(
        (
            parsed_url.scheme,
            host,
            "/",
            urllib.parse.urlencode(query_params),
            "",
        )
    )
    return request_url, headers


def _post_clickhouse_json_each_row(
    clickhouse_url: str, table: str, rows: Iterable[str]
) -> None:
    insert_url, headers = _clickhouse_request_parts(
        clickhouse_url, f"INSERT INTO {table} FORMAT JSONEachRow"
    )
    buffer = bytearray()

    def flush() -> None:
        if not buffer:
            return
        request = urllib.request.Request(
            insert_url,
            data=bytes(buffer),
            method="POST",
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
        buffer.clear()

    for row in rows:
        encoded_row = row.encode("utf-8") + b"\n"
        if buffer and len(buffer) + len(encoded_row) > CLICKHOUSE_INSERT_CHUNK_BYTES:
            flush()
        buffer.extend(encoded_row)

    flush()


def execute_clickhouse_query(clickhouse_url: str, query: str) -> None:
    request_url, headers = _clickhouse_request_parts(clickhouse_url, query)
    request = urllib.request.Request(request_url, method="POST", headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        response.read()


def ensure_clickhouse_tables(config: StreamConfig) -> None:
    if not config.clickhouse_url:
        return

    LOGGER.info("Ensuring ClickHouse tables exist")
    execute_clickhouse_query(config.clickhouse_url, CLICKHOUSE_NEWS_TABLE_DDL)
    execute_clickhouse_query(config.clickhouse_url, CLICKHOUSE_FUTURE_KLINE_1M_TABLE_DDL)


def write_clickhouse_table(df: DataFrame, config: StreamConfig, table: str) -> None:
    if not config.clickhouse_url:
        return
    df.toJSON().foreachPartition(
        lambda rows: _post_clickhouse_json_each_row(config.clickhouse_url, table, rows)
    )


def write_clickhouse_batch(batch_df: DataFrame, batch_id: int, config: StreamConfig) -> None:
    if not config.clickhouse_url:
        return

    LOGGER.info("Writing micro-batch batchId=%s to ClickHouse", batch_id)
    write_clickhouse_table(prepare_clickhouse_news(batch_df), config, "news")
    write_clickhouse_table(
        prepare_clickhouse_future_kline_1m(batch_df), config, "future_kline_1m"
    )


def write_clickhouse_stream(raw_df: DataFrame, config: StreamConfig):
    writer = (
        raw_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_clickhouse_batch(batch_df, batch_id, config)
        )
        .queryName("kafka_to_clickhouse")
        .option("checkpointLocation", config.clickhouse_checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=config.trigger_processing_time)
    )

    return writer.start()

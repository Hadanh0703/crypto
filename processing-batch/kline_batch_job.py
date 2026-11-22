import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.window import Window
import clickhouse_connect

aws_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_PASS = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "development")
RAW_OUTPUT_PATH = os.getenv("RAW_OUTPUT_PATH", f"s3a://{BUCKET_NAME}/raw/kafka").rstrip("/")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "local")
KLINES_TOPIC = os.getenv("KLINES_TOPIC", f"{KAFKA_TOPIC_PREFIX}-ingestion-collector-klines-1m")

spark = SparkSession.builder \
    .appName("Crypto-Kline-Batch-Processing") \
    .config("spark.jars.packages", ",".join(aws_packages)) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MINIO_ENDPOINT.startswith("https://")).lower()) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    ) \
    .getOrCreate()

kline_raw_schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("openTime", LongType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("closeTime", LongType(), True)
])

# -----------------------------------------------------------------------------
# UDF: Calculate RSI (Wilder's Smoothing) from a historical array of close prices
# -----------------------------------------------------------------------------
@F.udf(DoubleType())
def calculate_rsi_udf(prices):
    if not prices or len(prices) < 15:
        return None
    # Ensure types are standard floats to avoid serialization issues
    prices = [float(p) for p in prices]
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    seed = deltas[:14]
    up = sum([d for d in seed if d >= 0]) / 14.0
    down = -sum([d for d in seed if d < 0]) / 14.0
    
    if down == 0:
        return 100.0
    
    rs = up / down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    # Apply Wilder's smoothed moving average for remaining periods
    for delta in deltas[14:]:
        upval = delta if delta > 0 else 0.
        downval = -delta if delta < 0 else 0.
        up = (up * 13 + upval) / 14.0
        down = (down * 13 + downval) / 14.0
        if down == 0:
            rsi = 100.0
        else:
            rs = up / down
            rsi = 100.0 - (100.0 / (1.0 + rs))
            
    return float(rsi)

def process_klines():
    print("Reading Parquet data from MinIO...")
    event_schema = StructType([
        StructField("klines", F.ArrayType(kline_raw_schema), True)
    ])
    try:
        minio_read_path = f"{RAW_OUTPUT_PATH}/*/*/*"
        print(f"Reading Parquet from: {minio_read_path} (basePath={RAW_OUTPUT_PATH})")
        raw_df = spark.read.option("basePath", RAW_OUTPUT_PATH).parquet(minio_read_path)
        if "topic" in raw_df.columns and "event_type" in raw_df.columns:
            raw_df = raw_df.filter(
                (F.col("topic") == KLINES_TOPIC) | (F.col("event_type") == "Klines1mSynced")
            )
        elif "event_type" in raw_df.columns:
            raw_df = raw_df.filter(F.col("event_type") == "Klines1mSynced")
        parsed_df = raw_df.withColumn("event", F.from_json(F.col("raw_value"), event_schema)) \
                          .select(F.explode("event.klines").alias("data")) \
                          .select("data.*")
    except Exception as e:
        print("Data not found on MinIO. Fetching REAL DATA from Binance API for Demo...")
        import urllib.request
        import json
        try:
            req = urllib.request.Request("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000", headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                
            real_data = [
                ("BTCUSDT", int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), int(row[6]))
                for row in data
            ]
            parsed_df = spark.createDataFrame(real_data, kline_raw_schema)
        except Exception as fetch_err:
            print(f"Failed to fetch from Binance: {fetch_err}. Falling back to static mock data...")
            mock_data = [
                ("BTCUSDT", 1710000000000 + (i * 60000), 
                 str(60000 + (i % 10) * 50), str(60500 + (i % 10) * 50), 
                 str(59900 + (i % 10) * 50), str(60000 + (i % 10) * 100), 
                 str(10.0 + (50.0 if i == 45 else 0.0)), 1710000060000 + (i * 60000)) 
                for i in range(100)
            ]
            parsed_df = spark.createDataFrame(mock_data, kline_raw_schema)

    # Cast data types
    parsed_df = parsed_df.withColumn("openTime", F.col("openTime").cast(LongType())) \
                         .withColumn("open", F.col("open").cast("double")) \
                         .withColumn("high", F.col("high").cast("double")) \
                         .withColumn("low", F.col("low").cast("double")) \
                         .withColumn("close", F.col("close").cast("double")) \
                         .withColumn("volume", F.col("volume").cast("double"))

    # Calculate 5-minute bucket boundaries (Epoch ms)
    parsed_df = parsed_df.withColumn("openTime_5m", (F.col("openTime") / 300000).cast(LongType()) * 300000)
    
    # -------------------------------------------------------------------------
    # Aggregate 1m data into 5m candles
    # -------------------------------------------------------------------------
    window_5m = Window.partitionBy("ticker", "openTime_5m").orderBy("openTime") \
                      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    final_5m_df = parsed_df \
        .withColumn("open", F.first("open").over(window_5m)) \
        .withColumn("high", F.max("high").over(window_5m)) \
        .withColumn("low", F.min("low").over(window_5m)) \
        .withColumn("close", F.last("close").over(window_5m)) \
        .withColumn("volume", F.sum("volume").over(window_5m)) \
        .withColumn("closeTime", F.col("openTime_5m") + F.lit(299999)) \
        .select("ticker", F.col("openTime_5m").alias("openTime"), "open", "high", "low", "close", "volume", "closeTime") \
        .dropDuplicates(["ticker", "openTime"]) \
        .orderBy("ticker", "openTime")
        
    # Cache intermediate DataFrame used for multiple downstream calculations
    final_5m_df.cache()

    # -------------------------------------------------------------------------
    # Calculate Technical Indicators
    # -------------------------------------------------------------------------
    ticker_time_window = Window.partitionBy("ticker").orderBy("openTime")
    
    # Simple Moving Averages
    ma20_formula = F.avg("close").over(ticker_time_window.rowsBetween(-19, 0))
    ma50_formula = F.avg("close").over(ticker_time_window.rowsBetween(-49, 0))
    
    # Bollinger Bands (20 periods, 2 stddev)
    stddev_20 = F.stddev("close").over(ticker_time_window.rowsBetween(-19, 0))
    bb_upper_formula = ma20_formula + (stddev_20 * 2)
    bb_lower_formula = ma20_formula - (stddev_20 * 2)
    
    # Volume Spike Detection (> 3x average of past 7 periods)
    avg_vol_prev_7 = F.avg("volume").over(ticker_time_window.rowsBetween(-7, -1))
    volume_spike_condition = F.when((F.col("volume") > (avg_vol_prev_7 * 3)) & (avg_vol_prev_7 > 0), 1).otherwise(0)

    # RSI 14 (Look back 100 candles to get an accurate smoothed result)
    window_rsi_lookback = Window.partitionBy("ticker").orderBy("openTime").rowsBetween(-100, 0)
    
    indicators_df = final_5m_df \
        .withColumn("ma20", ma20_formula) \
        .withColumn("ma50", ma50_formula) \
        .withColumn("bb_upper", bb_upper_formula) \
        .withColumn("bb_lower", bb_lower_formula) \
        .withColumn("is_volume_spike", volume_spike_condition) \
        .withColumn("close_history", F.collect_list("close").over(window_rsi_lookback)) \
        .withColumn("rsi", calculate_rsi_udf(F.col("close_history"))) \
        .drop("close_history")

    # -------------------------------------------------------------------------
    # Distributed Data Sink
    # -------------------------------------------------------------------------
    print("Executing distributed write to ClickHouse...")
    
    def write_partition(partition_data, table_name, columns):
        import datetime
        from decimal import Decimal
        import math
        import os
        import urllib.parse
        # pyrefly: ignore [missing-import]
        import clickhouse_connect

        records = [list(row) for row in partition_data]
        if not records:
            return

        try:
            url = os.getenv("CLICKHOUSE_URL")
            if url:
                parsed = urllib.parse.urlparse(url)
                host = parsed.hostname
                port = parsed.port or (443 if parsed.scheme == 'https' else 8123)
                user = parsed.username or "default"
                password = parsed.password or ""
                secure = parsed.scheme == 'https'
            else:
                host = os.getenv("CLICKHOUSE_HOST", "localhost")
                port = int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123))
                user = os.getenv("CLICKHOUSE_USER", "default")
                password = os.getenv("CLICKHOUSE_PASSWORD", "")
                secure = (port == 443)

            client = clickhouse_connect.get_client(
                host=host, port=port, username=user, password=password, secure=secure
            )

            # Adapt values to match target ClickHouse column types (supports both
            # UInt64/Decimal(38,18) and DateTime64/String schemas safely)
            insert_ctx = client.create_insert_context(table_name, column_names=columns)
            col_types = [t.name for t in insert_ctx.column_types]

            def coerce_value(val, ch_type):
                if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
                    if "String" in ch_type:
                        return ""
                    if "Decimal" in ch_type:
                        return Decimal("0")
                    if "DateTime" in ch_type:
                        return datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
                    return 0
                if "String" in ch_type:
                    return str(val)
                if "DateTime" in ch_type:
                    if isinstance(val, (int, float)):
                        return datetime.datetime.fromtimestamp(val / 1000.0, tz=datetime.timezone.utc)
                    return val
                if "Decimal" in ch_type:
                    return Decimal(str(val))
                if "Int" in ch_type:
                    return int(val)
                return val

            coerced_records = [
                [coerce_value(val, ch_type) for val, ch_type in zip(row, col_types)]
                for row in records
            ]

            client.insert(table_name, coerced_records, column_names=columns)
            client.close()
        except Exception as e:
            print(f"Error writing partition to {table_name}: {e}")
            raise e

    try:
        
        # Only insert the 3 most recent candles to prevent overwriting perfectly calculated historical MAs with edge-case sliding window errors.
        window_latest = Window.partitionBy("ticker").orderBy(F.col("openTime").desc())
        
        filtered_5m_df = final_5m_df.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") <= 3).drop("rn")
        filtered_indicators_df = indicators_df.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") <= 3).drop("rn")

        # Core Fact Table: future_kline_5m
        kline_cols = ['ticker', 'openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime']
        filtered_5m_df.select(*kline_cols).rdd.foreachPartition(
            lambda partition: write_partition(partition, 'future_kline_5m', kline_cols)
        )
        print("Successfully written to 'future_kline_5m'.")
        
        # Computed Fact Table: kline_indicators
        ind_cols = ['ticker', 'openTime', 'ma20', 'ma50', 'rsi', 'bb_upper', 'bb_lower', 'is_volume_spike']
        filtered_indicators_df.select(*ind_cols).rdd.foreachPartition(
            lambda partition: write_partition(partition, 'kline_indicators', ind_cols)
        )
        print("Successfully written to 'kline_indicators'.")
        
        print("Batch job completed successfully. Data synchronized to ClickHouse.")
    except Exception as ex:
        print(f"Execution error writing data: {ex}")
        raise ex

def ensure_tables():
    import os
    import urllib.parse
    import clickhouse_connect
    
    url = os.getenv("CLICKHOUSE_URL")
    if url:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == 'https' else 8123)
        user = parsed.username or "default"
        password = parsed.password or ""
        secure = parsed.scheme == 'https'
    else:
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_HTTP_PORT", 8123))
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        secure = (port == 443)

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, secure=secure
    )
    
    client.command("""
        CREATE TABLE IF NOT EXISTS future_kline_5m (
            ticker String, openTime UInt64, open Decimal(38, 18), high Decimal(38, 18),
            low Decimal(38, 18), close Decimal(38, 18), volume Decimal(38, 18),
            closeTime UInt64, quoteAssetVolume Decimal(38, 18), numOfTrades UInt32,
            takerBuyBaseAssetVolume Decimal(38, 18), takerBuyQuoteAssetVolume Decimal(38, 18),
            ignore Decimal(38, 18), version DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(toDateTime(openTime / 1000))
        ORDER BY (ticker, openTime)
    """)
    client.command("""
        CREATE TABLE IF NOT EXISTS kline_indicators (
            ticker String, openTime UInt64, ma20 Decimal(38, 18), ma50 Decimal(38, 18),
            rsi Decimal(38, 18), bb_upper Decimal(38, 18), bb_lower Decimal(38, 18),
            is_volume_spike UInt8, version DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(toDateTime(openTime / 1000))
        ORDER BY (ticker, openTime)
    """)
    client.close()
    print("Ensured tables future_kline_5m and kline_indicators exist.")

if __name__ == "__main__":
    ensure_tables()
    process_klines()
    spark.stop()
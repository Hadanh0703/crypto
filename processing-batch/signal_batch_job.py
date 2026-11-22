import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType
import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def get_clickhouse_client():
    import urllib.parse
    
    url = os.getenv("CLICKHOUSE_URL")
    if url:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == 'https' else 8123)
        user = parsed.username or "default"
        password = parsed.password or ""
        secure = parsed.scheme == 'https'
    else:
        host = CLICKHOUSE_HOST
        port = CLICKHOUSE_PORT
        user = CLICKHOUSE_USER
        password = CLICKHOUSE_PASSWORD
        secure = (port == 443)

    return clickhouse_connect.get_client(
        host=host, 
        port=port, 
        username=user, 
        password=password,
        secure=secure
    )

def ensure_signals_table():
    client = get_clickhouse_client()
    client.command("""
        CREATE TABLE IF NOT EXISTS signals (
            id UUID DEFAULT generateUUIDv4(),
            ticker String,
            type String,
            price Decimal(38, 18),
            reason String,
            timestamp UInt64,
            version DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY (timestamp, ticker)
    """)
    print("Ensured signals table exists.")

def process_signals():
    # 1. Fetch recent candles from ClickHouse using clickhouse_connect
    # We fetch the last 24 candles (2 hours) for each ticker
    print("Fetching recent candles from ClickHouse...")
    client = get_clickhouse_client()
    query = """
        SELECT ticker, openTime, open, close, volume 
        FROM future_kline_5m
        WHERE openTime >= toUnixTimestamp(now() - INTERVAL 2 HOUR) * 1000
        ORDER BY ticker, openTime
    """
    
    result = client.query(query)
    
    if not result.result_rows:
        print("No recent candles found. Exiting.")
        return
        
    # 2. Start Spark
    spark = SparkSession.builder \
        .appName("Crypto-Signal-Batch-Processing") \
        .getOrCreate()
        
    # Convert to Spark DataFrame
    schema = ["ticker", "openTime", "open", "close", "volume"]
    df = spark.createDataFrame(result.result_rows, schema=schema)
    
    # Ensure types
    df = df.withColumn("open", F.col("open").cast("double")) \
           .withColumn("close", F.col("close").cast("double")) \
           .withColumn("volume", F.col("volume").cast("double"))
    
    # 3. Detect Signals using Window Functions
    window_spec = Window.partitionBy("ticker").orderBy("openTime")
    
    # Calculate previous close
    df = df.withColumn("prev_close", F.lag("close").over(window_spec))
    
    # Calculate price change percentage
    df = df.withColumn("price_change_pct", 
                      F.when(F.col("prev_close") > 0, 
                             (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100)
                       .otherwise(0.0))
    
    # Calculate average volume of last 5 periods
    df = df.withColumn("avg_vol_5", F.avg("volume").over(window_spec.rowsBetween(-5, -1)))
    
    # Define rules for Pump and Dump
    # Pump: Price surges by > 0.2% in 5 minutes AND Volume is at least 1.2x average
    # Dump: Price drops by < -0.2% in 5 minutes AND Volume is at least 1.2x average
    df = df.withColumn("signal_type", 
        F.when((F.col("price_change_pct") >= 0.2) & (F.col("volume") >= F.col("avg_vol_5") * 1.2), "Pump")
         .when((F.col("price_change_pct") <= -0.2) & (F.col("volume") >= F.col("avg_vol_5") * 1.2), "Dump")
         .otherwise(None)
    )
    
    # Filter only signals
    signals_df = df.filter(F.col("signal_type").isNotNull())
    
    # Create reason string
    signals_df = signals_df.withColumn("reason", 
        F.concat(F.lit("Price changed by "), F.round(F.col("price_change_pct"), 2), F.lit("% with high volume."))
    )
    
    # Format for ClickHouse
    final_signals = signals_df.select(
        F.col("ticker"),
        F.col("signal_type").alias("type"),
        F.col("close").cast(StringType()).alias("price"), # Decimal in clickhouse
        F.col("reason"),
        F.col("openTime").alias("timestamp")
    )
    
    signals_to_insert = final_signals.collect()
    
    if not signals_to_insert:
        print("No signals detected in this batch.")
        return
        
    print(f"Detected {len(signals_to_insert)} signals. Writing to ClickHouse...")
    
    # Write to ClickHouse
    insert_data = [list(row) for row in signals_to_insert]
    
    try:
        client.insert("signals", insert_data, column_names=["ticker", "type", "price", "reason", "timestamp"])
        print("Successfully wrote signals to ClickHouse.")
    except Exception as e:
        print(f"Error writing signals to ClickHouse: {e}")
        raise e
    finally:
        spark.stop()
        client.close()

if __name__ == "__main__":
    ensure_signals_table()
    try:
        process_signals()
    except Exception as e:
        print(f"Execution error in signal job: {e}")
        raise e

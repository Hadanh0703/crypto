from decimal import Decimal
import json
import math
import os
import urllib.parse
import urllib.request

import clickhouse_connect
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
LOOKBACK_HOURS = int(os.getenv("ML_LOOKBACK_HOURS", "24"))
KMEANS_K = int(os.getenv("ML_KMEANS_K", "4"))
ANOMALY_STD_MULTIPLIER = float(os.getenv("ML_ANOMALY_STD_MULTIPLIER", "2.0"))


def get_clickhouse_client():
    url = os.getenv("CLICKHOUSE_URL")
    if url:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 8123)
        user = parsed.username or "default"
        password = parsed.password or ""
        secure = parsed.scheme == "https"
    else:
        host = CLICKHOUSE_HOST
        port = CLICKHOUSE_PORT
        user = CLICKHOUSE_USER
        password = CLICKHOUSE_PASSWORD
        secure = port == 443

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        secure=secure,
    )


def ensure_anomalies_table(client) -> None:
    client.command(
        """
        CREATE TABLE IF NOT EXISTS ml_anomalies (
            ticker String,
            openTime UInt64,
            close Decimal(38, 18),
            volume Decimal(38, 18),
            price_change_pct Float64,
            volatility_pct Float64,
            cluster UInt32,
            anomaly_score Float64,
            version DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(toDateTime(openTime / 1000))
        ORDER BY (openTime, ticker)
        """
    )
    print("Ensured ml_anomalies table exists.")


def load_5m_klines(spark: SparkSession):
    schema = StructType(
        [
            StructField("ticker", StringType(), True),
            StructField("openTime", LongType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True),
        ]
    )

    clickhouse_reachable = False
    try:
        client = get_clickhouse_client()
        col_types = {
            row[0]: row[1]
            for row in client.query("DESCRIBE TABLE future_kline_5m").result_rows
        }
        is_datetime_open = "DateTime" in col_types.get("openTime", "")

        if is_datetime_open:
            recent_query = f"""
                SELECT
                    ticker,
                    toUnixTimestamp64Milli(openTime) AS openTime,
                    toFloat64OrZero(toString(open)) AS open,
                    toFloat64OrZero(toString(high)) AS high,
                    toFloat64OrZero(toString(low)) AS low,
                    toFloat64OrZero(toString(close)) AS close,
                    toFloat64OrZero(toString(volume)) AS volume
                FROM future_kline_5m FINAL
                WHERE openTime >= now() - INTERVAL {LOOKBACK_HOURS} HOUR
                ORDER BY ticker, openTime
            """
            fallback_ch_query = """
                SELECT
                    ticker,
                    toUnixTimestamp64Milli(openTime) AS openTime,
                    toFloat64OrZero(toString(open)) AS open,
                    toFloat64OrZero(toString(high)) AS high,
                    toFloat64OrZero(toString(low)) AS low,
                    toFloat64OrZero(toString(close)) AS close,
                    toFloat64OrZero(toString(volume)) AS volume
                FROM future_kline_5m FINAL
                ORDER BY openTime DESC
                LIMIT 2000
            """
        else:
            recent_query = f"""
                SELECT ticker, openTime, open, high, low, close, volume
                FROM future_kline_5m FINAL
                WHERE openTime >= toUnixTimestamp(now() - INTERVAL {LOOKBACK_HOURS} HOUR) * 1000
                ORDER BY ticker, openTime
            """
            fallback_ch_query = """
                SELECT ticker, openTime, open, high, low, close, volume
                FROM future_kline_5m FINAL
                ORDER BY openTime DESC
                LIMIT 2000
            """

        result = client.query(recent_query)
        clickhouse_reachable = True

        if not result.result_rows or len(result.result_rows) < 10:
            print(
                f"Fewer than 10 rows in last {LOOKBACK_HOURS}h; loading latest 2000 rows from ClickHouse..."
            )
            result = client.query(fallback_ch_query)

        client.close()

        if result.result_rows and len(result.result_rows) >= 10:
            rows = [
                (
                    str(r[0]),
                    int(r[1]),
                    float(r[2]),
                    float(r[3]),
                    float(r[4]),
                    float(r[5]),
                    float(r[6]),
                )
                for r in result.result_rows
            ]
            print(f"Loaded {len(rows)} 5m klines from ClickHouse.")
            return spark.createDataFrame(rows, schema=schema), True
        print("Insufficient ClickHouse data (<10 rows). Falling back to Binance API for demo...")
    except Exception as err:
        print(f"ClickHouse unavailable ({err}). Falling back to Binance API for demo...")

    try:
        req = urllib.request.Request(
            "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=500",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())
        rows = [
            (
                "BTCUSDT",
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            )
            for row in data
        ]
        print(f"Loaded {len(rows)} 5m klines from Binance public API.")
        return spark.createDataFrame(rows, schema=schema), clickhouse_reachable
    except Exception as fetch_err:
        print(f"Binance API fallback failed ({fetch_err}). Using synthetic demo dataset...")
        base_ts = 1710000000000
        rows = []
        for i in range(120):
            is_spike = i in (30, 75, 110)
            open_p = 60000.0 + (i % 15) * 40.0
            delta = (1800.0 if i % 2 == 0 else -1600.0) if is_spike else ((i % 5) - 2) * 35.0
            close_p = open_p + delta
            high_p = max(open_p, close_p) + (400.0 if is_spike else 50.0)
            low_p = min(open_p, close_p) - (400.0 if is_spike else 50.0)
            vol = 450.0 if is_spike else 25.0 + (i % 10) * 3.0
            rows.append(("BTCUSDT", base_ts + i * 300000, open_p, high_p, low_p, close_p, vol))
        return spark.createDataFrame(rows, schema=schema), clickhouse_reachable


def detect_anomalies() -> None:
    spark = (
        SparkSession.builder.appName("Crypto-MLlib-KMeans-Anomaly-Detection")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df, clickhouse_available = load_5m_klines(spark)

        # Feature engineering: price_change_pct, volatility_pct, and volume
        features_df = (
            df.filter(F.col("open") > 0)
            .withColumn(
                "price_change_pct",
                ((F.col("close") - F.col("open")) / F.col("open")) * F.lit(100.0),
            )
            .withColumn(
                "volatility_pct",
                ((F.col("high") - F.col("low")) / F.col("open")) * F.lit(100.0),
            )
            .na.drop(subset=["volume", "price_change_pct", "volatility_pct"])
        )

        row_count = features_df.count()
        if row_count < 4:
            print(f"Not enough valid rows ({row_count}) for KMeans clustering. Exiting.")
            return

        k = min(KMEANS_K, max(2, row_count // 10))
        assembler = VectorAssembler(
            inputCols=["volume", "price_change_pct", "volatility_pct"],
            outputCol="raw_features",
        )
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True,
        )
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="cluster",
            k=k,
            seed=42,
        )

        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        model = pipeline.fit(features_df)
        clustered_df = model.transform(features_df)

        kmeans_model = model.stages[-1]
        centers = [center.tolist() for center in kmeans_model.clusterCenters()]
        bc_centers = spark.sparkContext.broadcast(centers)

        @F.udf(DoubleType())
        def distance_to_centroid(vec, cluster_id):
            if vec is None or cluster_id is None:
                return 0.0
            center = bc_centers.value[int(cluster_id)]
            return float(math.sqrt(sum((float(a) - float(b)) ** 2 for a, b in zip(vec, center))))

        scored_df = clustered_df.withColumn(
            "anomaly_score",
            distance_to_centroid(F.col("scaled_features"), F.col("cluster")),
        )

        stats = scored_df.agg(
            F.mean("anomaly_score").alias("mean_score"),
            F.stddev("anomaly_score").alias("std_score"),
        ).collect()[0]

        mean_score = float(stats["mean_score"] or 0.0)
        std_score = float(stats["std_score"] or 0.0)
        threshold = mean_score + ANOMALY_STD_MULTIPLIER * std_score

        print("\n=== KMeans Cluster Summary ===")
        scored_df.groupBy("cluster").agg(
            F.count("*").alias("candles"),
            F.round(F.avg("volume"), 2).alias("avg_volume"),
            F.round(F.avg("price_change_pct"), 4).alias("avg_price_change_pct"),
            F.round(F.avg("volatility_pct"), 4).alias("avg_volatility_pct"),
            F.round(F.avg("anomaly_score"), 4).alias("avg_distance"),
        ).orderBy("cluster").show(truncate=False)

        anomalies_df = (
            scored_df.filter(F.col("anomaly_score") > F.lit(threshold))
            .select(
                "ticker",
                "openTime",
                "close",
                "volume",
                F.round("price_change_pct", 4).alias("price_change_pct"),
                F.round("volatility_pct", 4).alias("volatility_pct"),
                "cluster",
                F.round("anomaly_score", 4).alias("anomaly_score"),
            )
            .orderBy(F.col("anomaly_score").desc())
        )

        anomalies = anomalies_df.collect()
        print(
            f"Anomaly threshold (mean + {ANOMALY_STD_MULTIPLIER}*std): {threshold:.4f} "
            f"| Detected {len(anomalies)} anomalous candles out of {row_count}."
        )
        anomalies_df.show(20, truncate=False)

        if anomalies and clickhouse_available:
            client = get_clickhouse_client()
            try:
                ensure_anomalies_table(client)
                insert_rows = [
                    [
                        row["ticker"],
                        int(row["openTime"]),
                        Decimal(str(row["close"])),
                        Decimal(str(row["volume"])),
                        float(row["price_change_pct"]),
                        float(row["volatility_pct"]),
                        int(row["cluster"]),
                        float(row["anomaly_score"]),
                    ]
                    for row in anomalies
                ]
                client.insert(
                    "ml_anomalies",
                    insert_rows,
                    column_names=[
                        "ticker",
                        "openTime",
                        "close",
                        "volume",
                        "price_change_pct",
                        "volatility_pct",
                        "cluster",
                        "anomaly_score",
                    ],
                )
                print(f"Successfully wrote {len(insert_rows)} ML anomalies to ClickHouse 'ml_anomalies'.")
            except Exception as write_err:
                print(f"Warning: could not persist anomalies to ClickHouse: {write_err}")
            finally:
                client.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    detect_anomalies()

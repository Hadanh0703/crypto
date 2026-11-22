# Crypto BigData - Processing Batch

This repository contains the **Apache Spark Batch Jobs** for processing raw cryptocurrency candlestick data into aggregated metrics, and ultimately generating actionable trading signals.

## Architecture Role & Job Sequence
This component serves as the **Batch Layer** in the project's Lambda Architecture pipeline. It is orchestrated by a central `main.py` entrypoint which runs two jobs sequentially:

1. **`kline_batch_job.py` (Data Transformation & Indicators)**
   - **Extract**: Reads raw Parquet data from MinIO (Data Lake).
   - **Transform**: Uses PySpark Window Functions to compute 5-minute OHLCV candles, Moving Averages (MA20, MA50), Bollinger Bands, and RSI (via custom UDF).
   - **Load**: Writes to ClickHouse tables (`future_kline_5m` and `kline_indicators`).

2. **`signal_batch_job.py` (Signal Generation)**
   - **Extract**: Reads the recent 5-minute candles from the `future_kline_5m` table in ClickHouse.
   - **Transform**: Applies rule-based algorithms to detect "Pump" and "Dump" patterns based on sudden price percentage changes and volume spikes (e.g., price change > 0.2% with volume > 1.2x average).
   - **Load**: Writes the detected events to the `signals` table in ClickHouse for frontend dashboard rendering.

## Prerequisites
- Apache Spark 3.4.2 (or a Docker environment)
- Python 3.11+
- MinIO and ClickHouse instances running and accessible.

## Local Setup
1. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Or on Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Copy the environment variables template:
   ```bash
   cp .env.example .env
   ```
3. Update `.env` with your actual local or cluster credentials.

## Running the Pipeline
To execute the complete batch pipeline locally (both Klines and Signals):
```bash
python main.py
```
*(Note: `main.py` handles Spark Session creation and triggers the jobs sequentially. Ensure your `PYTHONPATH` includes the current directory if running manually).*

## Docker Deployment
This service is packaged into a Docker image and is intended to be scheduled via a **Kubernetes CronJob** or **Spark Operator** (configured in the `infra-k8s` repository).

To build the image locally:
```bash
docker build -t crypto-batch-job:latest .
```

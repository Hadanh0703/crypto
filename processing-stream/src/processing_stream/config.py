from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional convenience dependency
    load_dotenv = None


DEFAULT_INGESTION_TOPIC_SUFFIXES = (
    "ingestion-collector-klines-1m",
    "ingestion-collector-news",
)


def _load_env_file() -> None:
    if load_dotenv is not None:
        load_dotenv()


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class StreamConfig:
    kafka_bootstrap_servers: str
    kafka_topics: tuple[str, ...]
    kafka_starting_offsets: str
    kafka_max_offsets_per_trigger: str | None
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    bucket_name: str
    raw_output_path: str
    checkpoint_path: str
    clickhouse_url: str | None
    clickhouse_checkpoint_path: str
    output_format: str
    trigger_processing_time: str
    progress_log_interval_seconds: int

    @classmethod
    def from_env(cls) -> "StreamConfig":
        _load_env_file()

        topic_prefix = _env("KAFKA_TOPIC_PREFIX", "local")
        explicit_topics = _env("KAFKA_TOPICS")
        if explicit_topics:
            kafka_topics = tuple(
                topic.strip() for topic in explicit_topics.split(",") if topic.strip()
            )
        else:
            kafka_topics = tuple(
                f"{topic_prefix}-{suffix}" for suffix in DEFAULT_INGESTION_TOPIC_SUFFIXES
            )

        bucket_name = _env("BUCKET_NAME", "development")
        minio_host = _env("MINIO_HOST", "localhost")
        minio_port = _env("MINIO_PORT", "32126")
        minio_secure = _env_bool("MINIO_SECURE", False)
        minio_scheme = "https" if minio_secure else "http"
        minio_endpoint = f"{minio_scheme}://{minio_host}:{minio_port}"

        return cls(
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092"),
            kafka_topics=kafka_topics,
            kafka_starting_offsets=_env("KAFKA_STARTING_OFFSETS", "latest"),
            kafka_max_offsets_per_trigger=_env("KAFKA_MAX_OFFSETS_PER_TRIGGER"),
            minio_endpoint=minio_endpoint,
            minio_access_key=_env("MINIO_ACCESS_KEY", "development"),
            minio_secret_key=_env("MINIO_SECRET_KEY", "123456789"),
            minio_secure=minio_secure,
            bucket_name=bucket_name,
            raw_output_path=_env(
                "RAW_OUTPUT_PATH", f"s3a://{bucket_name}/raw/kafka"
            ),
            checkpoint_path=_env(
                "CHECKPOINT_PATH", f"s3a://{bucket_name}/checkpoints/kafka-raw"
            ),
            clickhouse_url=_env("CLICKHOUSE_URL"),
            clickhouse_checkpoint_path=_env(
                "CLICKHOUSE_CHECKPOINT_PATH",
                f"s3a://{bucket_name}/checkpoints/processing-stream/clickhouse",
            ),
            output_format=_env("OUTPUT_FORMAT", "parquet").lower(),
            trigger_processing_time=_env("TRIGGER_PROCESSING_TIME", "30 seconds"),
            progress_log_interval_seconds=int(_env("PROGRESS_LOG_INTERVAL_SECONDS", "10")),
        )

"""Pipeline configuration from YAML files or environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class KafkaConfig:
    """Kafka broker configuration."""

    bootstrap_servers: str = "localhost:9092"
    group_id: str = "swiftstream-default"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 30_000
    fetch_max_bytes: int = 52_428_800


@dataclass
class PostgresConfig:
    """PostgreSQL sink configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "swiftstream"
    user: str = "swiftstream"
    password: str = ""
    table: str = "events"
    batch_size: int = 100


@dataclass
class S3Config:
    """S3 (or MinIO) sink configuration."""

    endpoint_url: str = "http://localhost:9000"
    bucket: str = "swiftstream"
    prefix: str = "events/"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"
    batch_size: int = 1000
    file_format: str = "parquet"


@dataclass
class DeadLetterConfig:
    """Dead-letter queue configuration."""

    topic: str = "swiftstream-dlq"
    max_retries: int = 3


@dataclass
class MetricsConfig:
    """Prometheus metrics configuration."""

    enabled: bool = True
    port: int = 8000


@dataclass
class HealthConfig:
    """Health check endpoint configuration."""

    enabled: bool = True
    port: int = 8080


@dataclass
class TransformSpec:
    """Specification for a single transform in the pipeline."""

    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class SinkSpec:
    """Specification for a single sink in the pipeline."""

    name: str
    type: str  # "postgres", "s3", "stdout"


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration."""

    name: str = "default-pipeline"
    input_topics: list[str] = field(default_factory=lambda: ["events"])
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    s3: S3Config = field(default_factory=S3Config)
    dead_letter: DeadLetterConfig = field(default_factory=DeadLetterConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    transforms: list[TransformSpec] = field(default_factory=list)
    sinks: list[SinkSpec] = field(default_factory=list)
    backpressure_max_buffered: int = 10_000
    shutdown_timeout_seconds: int = 30


def _apply_env_overrides(config: PipelineConfig) -> PipelineConfig:
    """Override configuration values from environment variables.

    Environment variable naming: SWIFTSTREAM_<SECTION>_<KEY> in uppercase.
    Example: SWIFTSTREAM_KAFKA_BOOTSTRAP_SERVERS=broker:9092
    """
    env_map: dict[str, tuple[object, str]] = {
        "SWIFTSTREAM_KAFKA_BOOTSTRAP_SERVERS": (config.kafka, "bootstrap_servers"),
        "SWIFTSTREAM_KAFKA_GROUP_ID": (config.kafka, "group_id"),
        "SWIFTSTREAM_POSTGRES_HOST": (config.postgres, "host"),
        "SWIFTSTREAM_POSTGRES_PORT": (config.postgres, "port"),
        "SWIFTSTREAM_POSTGRES_DATABASE": (config.postgres, "database"),
        "SWIFTSTREAM_POSTGRES_USER": (config.postgres, "user"),
        "SWIFTSTREAM_POSTGRES_PASSWORD": (config.postgres, "password"),
        "SWIFTSTREAM_POSTGRES_TABLE": (config.postgres, "table"),
        "SWIFTSTREAM_S3_ENDPOINT_URL": (config.s3, "endpoint_url"),
        "SWIFTSTREAM_S3_BUCKET": (config.s3, "bucket"),
        "SWIFTSTREAM_S3_ACCESS_KEY": (config.s3, "access_key"),
        "SWIFTSTREAM_S3_SECRET_KEY": (config.s3, "secret_key"),
        "SWIFTSTREAM_DLQ_TOPIC": (config.dead_letter, "topic"),
        "SWIFTSTREAM_DLQ_MAX_RETRIES": (config.dead_letter, "max_retries"),
        "SWIFTSTREAM_METRICS_PORT": (config.metrics, "port"),
        "SWIFTSTREAM_HEALTH_PORT": (config.health, "port"),
    }

    for env_key, (obj, attr) in env_map.items():
        value = os.environ.get(env_key)
        if value is not None:
            current = getattr(obj, attr)
            if isinstance(current, int):
                setattr(obj, attr, int(value))
            elif isinstance(current, bool):
                setattr(obj, attr, value.lower() in ("true", "1", "yes"))
            else:
                setattr(obj, attr, value)

    return config


def load_config(path: str | Path | None = None) -> PipelineConfig:
    """Load pipeline configuration from a YAML file with env var overrides.

    If no path is provided, returns defaults with env overrides applied.
    """
    if path is None:
        return _apply_env_overrides(PipelineConfig())

    path = Path(path)
    with path.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    kafka_raw = raw.get("kafka", {})
    postgres_raw = raw.get("postgres", {})
    s3_raw = raw.get("s3", {})
    dlq_raw = raw.get("dead_letter", {})
    metrics_raw = raw.get("metrics", {})
    health_raw = raw.get("health", {})

    transforms_raw: list[dict[str, Any]] = raw.get("transforms", [])
    transforms = [
        TransformSpec(
            name=t.get("name", t["type"]),
            type=t["type"],
            params=t.get("params", {}),
        )
        for t in transforms_raw
    ]

    sinks_raw: list[dict[str, Any]] = raw.get("sinks", [])
    sinks = [SinkSpec(name=s.get("name", s["type"]), type=s["type"]) for s in sinks_raw]

    config = PipelineConfig(
        name=raw.get("name", "default-pipeline"),
        input_topics=raw.get("input_topics", ["events"]),
        kafka=KafkaConfig(**kafka_raw),
        postgres=PostgresConfig(**postgres_raw),
        s3=S3Config(**s3_raw),
        dead_letter=DeadLetterConfig(**dlq_raw),
        metrics=MetricsConfig(**metrics_raw),
        health=HealthConfig(**health_raw),
        transforms=transforms,
        sinks=sinks,
        backpressure_max_buffered=raw.get("backpressure_max_buffered", 10_000),
        shutdown_timeout_seconds=raw.get("shutdown_timeout_seconds", 30),
    )

    return _apply_env_overrides(config)

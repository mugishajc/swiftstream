"""S3 sink: writes messages as Parquet files to S3 or MinIO."""

from __future__ import annotations

import io
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from swiftstream.config import S3Config
from swiftstream.consumer import Message
from swiftstream.sinks.base import Sink

logger = logging.getLogger(__name__)


class S3Sink(Sink):
    """Writes messages as Parquet files to S3 or an S3-compatible store like MinIO.

    Messages are buffered until batch_size is reached, then written as a single
    Parquet file with a timestamped key. Each file contains:
        - topic (string)
        - key (string, nullable)
        - value (string, JSON-encoded)
        - timestamp (int64, epoch ms)

    File naming: {prefix}{date}/{hour}/{timestamp}_{count}.parquet
    """

    def __init__(self, config: S3Config, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._config = config
        self._buffer: list[dict[str, Any]] = []
        self._batch_size = config.batch_size
        self._client: Any = None
        self._file_counter = 0

    def _ensure_client(self) -> Any:
        """Create the S3 client if not already connected."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._config.endpoint_url,
                aws_access_key_id=self._config.access_key,
                aws_secret_access_key=self._config.secret_key,
                region_name=self._config.region,
            )
            # Ensure bucket exists (MinIO/localstack compatibility)
            try:
                self._client.head_bucket(Bucket=self._config.bucket)
            except Exception:
                try:
                    self._client.create_bucket(Bucket=self._config.bucket)
                    logger.info("Created S3 bucket: %s", self._config.bucket)
                except Exception as exc:
                    logger.warning("Could not create bucket %s: %s", self._config.bucket, exc)
        return self._client

    def _generate_key(self) -> str:
        """Generate a unique S3 key for the current batch."""
        now = datetime.now(UTC)
        date_part = now.strftime("%Y-%m-%d")
        hour_part = now.strftime("%H")
        ts = int(now.timestamp() * 1000)
        self._file_counter += 1
        return f"{self._config.prefix}{date_part}/{hour_part}/{ts}_{self._file_counter}.parquet"

    def _build_parquet_buffer(self, records: list[dict[str, Any]]) -> io.BytesIO:
        """Convert records to a Parquet file in memory."""
        schema = pa.schema(
            [
                pa.field("topic", pa.string()),
                pa.field("key", pa.string()),
                pa.field("value", pa.string()),
                pa.field("timestamp", pa.int64()),
            ]
        )

        arrays = [
            pa.array([r["topic"] for r in records], type=pa.string()),
            pa.array([r["key"] for r in records], type=pa.string()),
            pa.array([r["value"] for r in records], type=pa.string()),
            pa.array([r["timestamp"] for r in records], type=pa.int64()),
        ]

        table = pa.table(arrays, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    def write(self, message: Message) -> None:
        """Buffer a message for batch upload."""
        record = {
            "topic": message.topic,
            "key": message.key,
            "value": json.dumps(message.value),
            "timestamp": message.timestamp or int(time.time() * 1000),
        }
        self._buffer.append(record)

        if len(self._buffer) >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        """Write buffered messages as a Parquet file to S3."""
        if not self._buffer:
            return

        client = self._ensure_client()
        batch = list(self._buffer)
        self._buffer.clear()

        key = self._generate_key()
        parquet_buf = self._build_parquet_buffer(batch)

        start = time.monotonic()
        client.upload_fileobj(
            parquet_buf,
            self._config.bucket,
            key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        elapsed = time.monotonic() - start
        logger.info(
            "Uploaded %d records to s3://%s/%s in %.3fs",
            len(batch),
            self._config.bucket,
            key,
            elapsed,
        )

    def close(self) -> None:
        """Flush remaining data."""
        self.flush()
        self._client = None
        logger.info("S3 sink closed")

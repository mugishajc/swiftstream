"""Kafka consumer with consumer group management and offset control."""

from __future__ import annotations

import json
import logging
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from swiftstream.config import KafkaConfig

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """Normalized message envelope used throughout the pipeline."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    timestamp: int | None = None
    _retry_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        """Serialize message metadata and value."""
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
            "value": self.value,
            "headers": self.headers,
            "timestamp": self.timestamp,
        }


OnMessageCallback = Callable[[Message], None]
OnErrorCallback = Callable[[Exception, Message | None], None]


class KafkaConsumer:
    """Wraps confluent-kafka Consumer with offset management and graceful shutdown.

    Provides manual offset commits after successful processing to support
    at-least-once delivery. When used with the Pipeline class, this becomes
    the input stage of the processing chain.
    """

    def __init__(
        self,
        config: KafkaConfig,
        topics: list[str],
        on_message: OnMessageCallback | None = None,
        on_error: OnErrorCallback | None = None,
    ) -> None:
        self._config = config
        self._topics = topics
        self._on_message = on_message
        self._on_error = on_error
        self._running = False
        self._shutdown_event = threading.Event()
        self._consumer: Consumer | None = None
        self._committed_offsets: dict[tuple[str, int], int] = {}

    def _build_consumer_config(self) -> dict[str, Any]:
        """Build the confluent-kafka consumer configuration dict."""
        return {
            "bootstrap.servers": self._config.bootstrap_servers,
            "group.id": self._config.group_id,
            "auto.offset.reset": self._config.auto_offset_reset,
            "enable.auto.commit": self._config.enable_auto_commit,
            "max.poll.interval.ms": self._config.max_poll_interval_ms,
            "session.timeout.ms": self._config.session_timeout_ms,
            "fetch.max.bytes": self._config.fetch_max_bytes,
        }

    def _on_assign(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """Callback when partitions are assigned to this consumer."""
        topics = {p.topic for p in partitions}
        parts = [f"{p.topic}:{p.partition}" for p in partitions]
        logger.info("Partitions assigned: %s", ", ".join(parts))
        for t in topics:
            logger.info("Subscribed to topic: %s", t)

    def _on_revoke(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """Callback when partitions are revoked. Commit current offsets."""
        parts = [f"{p.topic}:{p.partition}" for p in partitions]
        logger.info("Partitions revoked: %s", ", ".join(parts))
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Failed to commit on revoke: %s", exc)

    def _parse_message(self, raw: Any) -> Message | None:
        """Parse a confluent-kafka Message into our Message dataclass."""
        if raw is None:
            return None

        err = raw.error()
        if err:
            if err.code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(err)

        try:
            value_bytes: bytes | None = raw.value()
            if value_bytes is None:
                return None
            value = json.loads(value_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to decode message at offset %d: %s", raw.offset(), exc)
            return None

        key_bytes: bytes | None = raw.key()
        key = key_bytes.decode("utf-8") if key_bytes else None

        headers: dict[str, str] = {}
        raw_headers = raw.headers()
        if raw_headers:
            for h_key, h_val in raw_headers:
                if h_val is not None:
                    headers[h_key] = h_val.decode("utf-8")

        ts_type, ts_val = raw.timestamp()
        timestamp = ts_val if ts_type != 0 else None

        return Message(
            topic=raw.topic(),
            partition=raw.partition(),
            offset=raw.offset(),
            key=key,
            value=value,
            headers=headers,
            timestamp=timestamp,
        )

    def commit_offset(self, message: Message) -> None:
        """Manually commit the offset for a processed message.

        This is called after successful processing to implement
        at-least-once delivery semantics.
        """
        if self._consumer is None:
            return

        tp = TopicPartition(message.topic, message.partition, message.offset + 1)
        try:
            self._consumer.commit(offsets=[tp], asynchronous=False)
            self._committed_offsets[(message.topic, message.partition)] = message.offset
        except KafkaException as exc:
            logger.error(
                "Offset commit failed for %s:%d@%d: %s",
                message.topic,
                message.partition,
                message.offset,
                exc,
            )

    def start(self) -> None:
        """Start consuming messages. Blocks until shutdown is requested."""
        conf = self._build_consumer_config()
        self._consumer = Consumer(conf)
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._running = True

        logger.info("Consumer started, topics=%s, group=%s", self._topics, self._config.group_id)

        try:
            while not self._shutdown_event.is_set():
                raw = self._consumer.poll(timeout=1.0)
                msg = self._parse_message(raw)
                if msg is None:
                    continue

                try:
                    if self._on_message:
                        self._on_message(msg)
                except Exception as exc:
                    logger.error("Message processing error: %s", exc)
                    if self._on_error:
                        self._on_error(exc, msg)
        finally:
            self._running = False
            logger.info("Consumer shutting down, committing final offsets")
            import contextlib

            with contextlib.suppress(KafkaException):
                self._consumer.commit(asynchronous=False)
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer stopped")

    def stop(self) -> None:
        """Signal the consumer to stop."""
        self._shutdown_event.set()

    @property
    def is_running(self) -> bool:
        """Whether the consumer loop is active."""
        return self._running

    def install_signal_handlers(self) -> None:
        """Register SIGTERM and SIGINT handlers for graceful shutdown."""

        def _handler(signum: int, frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            logger.info("Received %s, initiating shutdown", sig_name)
            self.stop()

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

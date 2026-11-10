"""Kafka producer for dead-letter queue and output topics."""

from __future__ import annotations

import json
import logging
import traceback
from typing import Any

from confluent_kafka import Producer

from swiftstream.config import KafkaConfig
from swiftstream.consumer import Message

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Wraps confluent-kafka Producer for DLQ and output topic writes.

    Messages that fail processing are sent to the dead-letter topic
    along with error metadata in headers.
    """

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer: Producer | None = None

    def _ensure_producer(self) -> Producer:
        """Lazily create the producer."""
        if self._producer is None:
            conf: dict[str, Any] = {
                "bootstrap.servers": self._config.bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 200,
                "linger.ms": 10,
                "compression.type": "lz4",
            }
            self._producer = Producer(conf)
        return self._producer

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback invoked on message delivery or failure."""
        if err is not None:
            logger.error("Delivery failed for topic %s: %s", msg.topic(), err)
        else:
            logger.debug(
                "Delivered to %s [%d] @ %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def produce(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Produce a message to a Kafka topic."""
        producer = self._ensure_producer()

        kafka_headers: list[tuple[str, str | bytes | None]] = []
        if headers:
            kafka_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]

        producer.produce(
            topic=topic,
            value=json.dumps(value).encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            headers=kafka_headers,
            callback=self._delivery_callback,
        )
        producer.poll(0)

    def send_to_dlq(
        self,
        dlq_topic: str,
        original_message: Message,
        error: Exception,
    ) -> None:
        """Send a failed message to the dead-letter queue with error metadata."""
        headers = dict(original_message.headers)
        headers["dlq.original_topic"] = original_message.topic
        headers["dlq.original_partition"] = str(original_message.partition)
        headers["dlq.original_offset"] = str(original_message.offset)
        headers["dlq.error_type"] = type(error).__name__
        headers["dlq.error_message"] = str(error)[:500]
        headers["dlq.traceback"] = traceback.format_exc()[:2000]
        headers["dlq.retry_count"] = str(original_message._retry_count)

        self.produce(
            topic=dlq_topic,
            value=original_message.value,
            key=original_message.key,
            headers=headers,
        )
        logger.warning(
            "Sent message to DLQ %s (original: %s:%d@%d, error: %s)",
            dlq_topic,
            original_message.topic,
            original_message.partition,
            original_message.offset,
            error,
        )

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages. Returns number of messages still in queue."""
        if self._producer is None:
            return 0
        return self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """Flush and close the producer."""
        if self._producer is not None:
            remaining = self._producer.flush(timeout=30.0)
            if remaining > 0:
                logger.warning("Producer closed with %d messages still in queue", remaining)
            self._producer = None

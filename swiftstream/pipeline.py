"""Pipeline: chains consumer, transforms, and sinks into a processing pipeline."""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections import deque
from typing import Any

from swiftstream.config import PipelineConfig, load_config
from swiftstream.consumer import KafkaConsumer, Message
from swiftstream.health import HealthCheckServer
from swiftstream.metrics import PipelineMetrics
from swiftstream.producer import KafkaProducer
from swiftstream.sinks.base import Sink
from swiftstream.sinks.postgres import PostgresSink
from swiftstream.sinks.s3 import S3Sink
from swiftstream.sinks.stdout import StdoutSink
from swiftstream.transforms.aggregate import AggregateTransform
from swiftstream.transforms.base import Transform
from swiftstream.transforms.deduplicate import DeduplicateTransform
from swiftstream.transforms.enrich import EnrichTransform
from swiftstream.transforms.filter import FilterTransform

logger = logging.getLogger(__name__)

# Registry of built-in transform types
TRANSFORM_REGISTRY: dict[str, type[Transform]] = {
    "filter": FilterTransform,
    "enrich": EnrichTransform,
    "aggregate": AggregateTransform,
    "deduplicate": DeduplicateTransform,
}

# Registry of built-in sink types
SINK_REGISTRY: dict[str, str] = {
    "postgres": "postgres",
    "s3": "s3",
    "stdout": "stdout",
}


class Pipeline:
    """Orchestrates the full event processing pipeline.

    Reads from Kafka, applies a chain of transforms, and writes to one or
    more sinks. Handles backpressure, dead-letter routing, metrics, and
    graceful shutdown.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._metrics = PipelineMetrics(config.name)
        self._producer = KafkaProducer(config.kafka)
        self._transforms: list[Transform] = []
        self._sinks: list[Sink] = []
        self._consumer: KafkaConsumer | None = None
        self._health_server: HealthCheckServer | None = None
        self._buffer: deque[Message] = deque()
        self._running = False
        self._shutdown_event = threading.Event()

        self._build_transforms()
        self._build_sinks()

    def _build_transforms(self) -> None:
        """Instantiate transforms from configuration."""
        for spec in self._config.transforms:
            cls = TRANSFORM_REGISTRY.get(spec.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transform type: {spec.type}. "
                    f"Available: {', '.join(TRANSFORM_REGISTRY)}"
                )
            transform = cls(name=spec.name, **spec.params)
            self._transforms.append(transform)
            logger.info("Registered transform: %s (%s)", spec.name, spec.type)

    def _build_sinks(self) -> None:
        """Instantiate sinks from configuration."""
        for spec in self._config.sinks:
            sink: Sink
            if spec.type == "postgres":
                sink = PostgresSink(self._config.postgres, name=spec.name)
            elif spec.type == "s3":
                sink = S3Sink(self._config.s3, name=spec.name)
            elif spec.type == "stdout":
                sink = StdoutSink(name=spec.name)
            else:
                raise ValueError(
                    f"Unknown sink type: {spec.type}. Available: {', '.join(SINK_REGISTRY)}"
                )
            self._sinks.append(sink)
            logger.info("Registered sink: %s (%s)", spec.name, spec.type)

    def _apply_transforms(self, message: Message) -> Message | None:
        """Run the message through the transform chain.

        Returns None if any transform drops the message.
        """
        current: Message | None = message
        for transform in self._transforms:
            if current is None:
                return None

            start = time.monotonic()
            current = transform.apply(current)
            elapsed = time.monotonic() - start
            self._metrics.observe_transform_latency(transform.name, elapsed)

        return current

    def _write_to_sinks(self, message: Message) -> None:
        """Write a processed message to all configured sinks."""
        for sink in self._sinks:
            start = time.monotonic()
            try:
                sink.write(message)
                elapsed = time.monotonic() - start
                self._metrics.observe_sink_write(sink.name, elapsed)
            except Exception as exc:
                self._metrics.record_sink_error(sink.name)
                logger.error("Sink %s write error: %s", sink.name, exc)
                raise

    def _process_message(self, message: Message) -> None:
        """Process a single message through transforms and sinks.

        On failure, routes the message to the dead-letter queue after
        max_retries attempts.
        """
        topic = message.topic
        self._metrics.record_consumed(topic)
        start = time.monotonic()

        try:
            # Check backpressure
            if len(self._buffer) >= self._config.backpressure_max_buffered:
                self._metrics.set_buffer_size(len(self._buffer))
                logger.warning(
                    "Backpressure: buffer at %d/%d, pausing consumption",
                    len(self._buffer),
                    self._config.backpressure_max_buffered,
                )
                # Drain buffer before accepting more
                self._flush_sinks()

            # Apply transforms
            result = self._apply_transforms(message)
            if result is None:
                # Message was filtered out
                return

            # Write to sinks
            self._write_to_sinks(result)

            # Commit offset after successful processing
            if self._consumer:
                self._consumer.commit_offset(message)

            elapsed = time.monotonic() - start
            self._metrics.record_processed(topic)
            self._metrics.observe_processing_latency(topic, elapsed)

        except Exception as exc:
            self._metrics.record_failed(topic)
            message._retry_count += 1

            if message._retry_count >= self._config.dead_letter.max_retries:
                # Send to DLQ
                try:
                    self._producer.send_to_dlq(
                        self._config.dead_letter.topic,
                        message,
                        exc,
                    )
                    self._metrics.record_dlq(topic)
                except Exception as dlq_exc:
                    logger.error("Failed to send to DLQ: %s", dlq_exc)

                # Commit offset to skip the poison message
                if self._consumer:
                    self._consumer.commit_offset(message)
            else:
                logger.warning(
                    "Retry %d/%d for %s:%d@%d: %s",
                    message._retry_count,
                    self._config.dead_letter.max_retries,
                    topic,
                    message.partition,
                    message.offset,
                    exc,
                )

    def _flush_sinks(self) -> None:
        """Flush all sinks."""
        for sink in self._sinks:
            try:
                sink.flush()
            except Exception as exc:
                logger.error("Error flushing sink %s: %s", sink.name, exc)

    def _get_health_status(self) -> dict[str, Any]:
        """Return health status for the health check endpoint."""
        return {
            "status": "ok" if self._running else "stopping",
            "pipeline": self._config.name,
            "buffer_size": len(self._buffer),
            "sinks": [s.name for s in self._sinks],
            "transforms": [t.name for t in self._transforms],
        }

    def run(self) -> None:
        """Start the pipeline. Blocks until shutdown is requested."""
        logger.info("Starting pipeline: %s", self._config.name)

        # Start metrics server
        if self._config.metrics.enabled:
            self._metrics.start_server(self._config.metrics.port)
            logger.info("Metrics server started on port %d", self._config.metrics.port)

        # Start health check server
        if self._config.health.enabled:
            self._health_server = HealthCheckServer(
                port=self._config.health.port,
                health_fn=self._get_health_status,
            )
            self._health_server.start()

        # Create and start consumer
        self._consumer = KafkaConsumer(
            config=self._config.kafka,
            topics=self._config.input_topics,
            on_message=self._process_message,
        )

        # Install signal handlers
        self._install_signal_handlers()

        self._running = True
        self._metrics.set_running(True)

        try:
            self._consumer.start()
        except KeyboardInterrupt:
            logger.info("Interrupted by keyboard")
        finally:
            self._shutdown()

    def _install_signal_handlers(self) -> None:
        """Register graceful shutdown on SIGTERM/SIGINT."""

        def _handler(signum: int, frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            logger.info("Pipeline received %s", sig_name)
            self.stop()

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

    def stop(self) -> None:
        """Signal the pipeline to stop."""
        self._shutdown_event.set()
        if self._consumer:
            self._consumer.stop()

    def _shutdown(self) -> None:
        """Clean shutdown: flush sinks, close transforms, stop servers."""
        logger.info("Pipeline shutting down: %s", self._config.name)
        self._running = False
        self._metrics.set_running(False)

        # Flush and close sinks
        for sink in self._sinks:
            try:
                sink.close()
            except Exception as exc:
                logger.error("Error closing sink %s: %s", sink.name, exc)

        # Close transforms
        for transform in self._transforms:
            try:
                transform.close()
            except Exception as exc:
                logger.error("Error closing transform %s: %s", transform.name, exc)

        # Flush producer
        self._producer.close()

        # Stop health server
        if self._health_server:
            self._health_server.stop()

        logger.info("Pipeline stopped: %s", self._config.name)

    @classmethod
    def from_config_file(cls, path: str) -> Pipeline:
        """Create a pipeline from a YAML configuration file."""
        config = load_config(path)
        return cls(config)

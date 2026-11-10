"""Prometheus metrics for pipeline observability."""

from __future__ import annotations

import threading
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server


class PipelineMetrics:
    """Collects and exposes pipeline metrics via Prometheus.

    Each instance gets its own CollectorRegistry so that multiple
    Pipeline objects (common in tests) do not collide.
    """

    def __init__(self, pipeline_name: str = "default") -> None:
        self.pipeline_name = pipeline_name
        self._server_started = False
        self._lock = threading.Lock()
        self._registry = CollectorRegistry()

        labels = ["pipeline", "topic"]

        self.messages_consumed = Counter(
            "swiftstream_messages_consumed_total",
            "Total messages consumed from Kafka",
            labels,
            registry=self._registry,
        )
        self.messages_processed = Counter(
            "swiftstream_messages_processed_total",
            "Total messages successfully processed through the pipeline",
            labels,
            registry=self._registry,
        )
        self.messages_failed = Counter(
            "swiftstream_messages_failed_total",
            "Total messages that failed processing",
            labels,
            registry=self._registry,
        )
        self.messages_dlq = Counter(
            "swiftstream_messages_dlq_total",
            "Total messages sent to the dead-letter queue",
            labels,
            registry=self._registry,
        )

        self.processing_latency = Histogram(
            "swiftstream_processing_latency_seconds",
            "Time spent processing a single message",
            labels,
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0),
            registry=self._registry,
        )

        self.transform_latency = Histogram(
            "swiftstream_transform_latency_seconds",
            "Time spent in a single transform step",
            ["pipeline", "transform"],
            buckets=(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
            registry=self._registry,
        )

        self.sink_write_latency = Histogram(
            "swiftstream_sink_write_latency_seconds",
            "Time spent writing to a sink",
            ["pipeline", "sink"],
            buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0),
            registry=self._registry,
        )

        self.sink_writes = Counter(
            "swiftstream_sink_writes_total",
            "Total writes to sinks",
            ["pipeline", "sink"],
            registry=self._registry,
        )
        self.sink_errors = Counter(
            "swiftstream_sink_errors_total",
            "Total sink write errors",
            ["pipeline", "sink"],
            registry=self._registry,
        )

        self.buffer_size = Gauge(
            "swiftstream_buffer_size",
            "Current number of messages in the processing buffer",
            ["pipeline"],
            registry=self._registry,
        )

        self.consumer_lag = Gauge(
            "swiftstream_consumer_lag",
            "Estimated consumer lag (messages behind)",
            labels,
            registry=self._registry,
        )

        self.pipeline_running = Gauge(
            "swiftstream_pipeline_running",
            "Whether the pipeline is currently running (1) or stopped (0)",
            ["pipeline"],
            registry=self._registry,
        )

    def start_server(self, port: int = 8000) -> None:
        """Start the Prometheus HTTP metrics server."""
        with self._lock:
            if not self._server_started:
                start_http_server(port, registry=self._registry)
                self._server_started = True

    def record_consumed(self, topic: str) -> None:
        """Record a message consumed from a topic."""
        self.messages_consumed.labels(pipeline=self.pipeline_name, topic=topic).inc()

    def record_processed(self, topic: str) -> None:
        """Record a successfully processed message."""
        self.messages_processed.labels(pipeline=self.pipeline_name, topic=topic).inc()

    def record_failed(self, topic: str) -> None:
        """Record a failed message."""
        self.messages_failed.labels(pipeline=self.pipeline_name, topic=topic).inc()

    def record_dlq(self, topic: str) -> None:
        """Record a message sent to the dead-letter queue."""
        self.messages_dlq.labels(pipeline=self.pipeline_name, topic=topic).inc()

    def observe_processing_latency(self, topic: str, seconds: float) -> None:
        """Record end-to-end processing latency for a message."""
        self.processing_latency.labels(pipeline=self.pipeline_name, topic=topic).observe(seconds)

    def observe_transform_latency(self, transform_name: str, seconds: float) -> None:
        """Record latency for a single transform step."""
        self.transform_latency.labels(
            pipeline=self.pipeline_name, transform=transform_name
        ).observe(seconds)

    def observe_sink_write(self, sink_name: str, seconds: float) -> None:
        """Record a sink write with its latency."""
        self.sink_writes.labels(pipeline=self.pipeline_name, sink=sink_name).inc()
        self.sink_write_latency.labels(pipeline=self.pipeline_name, sink=sink_name).observe(seconds)

    def record_sink_error(self, sink_name: str) -> None:
        """Record a sink write error."""
        self.sink_errors.labels(pipeline=self.pipeline_name, sink=sink_name).inc()

    def set_buffer_size(self, size: int) -> None:
        """Update the current buffer size gauge."""
        self.buffer_size.labels(pipeline=self.pipeline_name).set(size)

    def set_consumer_lag(self, topic: str, lag: int) -> None:
        """Update the consumer lag gauge for a topic."""
        self.consumer_lag.labels(pipeline=self.pipeline_name, topic=topic).set(lag)

    def set_running(self, running: bool) -> None:
        """Update the pipeline running state."""
        self.pipeline_running.labels(pipeline=self.pipeline_name).set(1 if running else 0)

    def get_stats(self) -> dict[str, Any]:
        """Return a snapshot of key metrics as a dict (for health checks)."""
        return {
            "pipeline": self.pipeline_name,
            "running": bool(self.pipeline_running.labels(pipeline=self.pipeline_name)._value.get()),
        }

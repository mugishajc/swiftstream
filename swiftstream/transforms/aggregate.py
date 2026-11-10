"""Aggregate transform: groups and aggregates messages in tumbling windows."""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from typing import Any

from swiftstream.consumer import Message
from swiftstream.transforms.base import Transform

logger = logging.getLogger(__name__)


class AggregateTransform(Transform):
    """Window-based aggregation on message fields.

    Accumulates messages in tumbling windows and emits aggregate results
    when the window closes. Useful for counting events, summing values,
    or computing averages over time intervals.

    Params:
        group_by: Field name to group messages by
        window_seconds: Tumbling window duration in seconds (default: 60)
        aggregations: List of dicts with {field, function} where function
                      is one of: count, sum, avg, min, max
        emit_field: Field name in the output value for the aggregate result

    The transform buffers messages and emits one aggregate message per group
    per window. Non-aggregate messages pass through unchanged.
    """

    def __init__(
        self,
        group_by: str = "",
        window_seconds: float = 60.0,
        aggregations: list[dict[str, str]] | None = None,
        emit_field: str = "aggregate",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._group_by = group_by
        self._window_seconds = window_seconds
        self._aggregations = aggregations or [{"field": "*", "function": "count"}]
        self._emit_field = emit_field

        self._windows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._window_start: dict[str, float] = {}
        self._lock = threading.Lock()

    def _get_group_key(self, value: dict[str, Any]) -> str:
        """Extract the group key from a message value."""
        if not self._group_by:
            return "__all__"
        parts = self._group_by.split(".")
        current: Any = value
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, "__none__")
            else:
                return "__none__"
        return str(current)

    def _compute_aggregations(self, values: list[dict[str, Any]]) -> dict[str, Any]:
        """Compute aggregation functions over a list of message values."""
        result: dict[str, Any] = {}
        for agg in self._aggregations:
            field = agg.get("field", "*")
            fn = agg.get("function", "count")
            key = f"{fn}_{field}" if field != "*" else fn

            if fn == "count":
                result[key] = len(values)
            elif field == "*":
                continue
            else:
                nums = []
                for v in values:
                    raw = v.get(field)
                    if raw is not None:
                        import contextlib

                        with contextlib.suppress(ValueError, TypeError):
                            nums.append(float(raw))

                if not nums:
                    result[key] = None
                    continue

                if fn == "sum":
                    result[key] = sum(nums)
                elif fn == "avg":
                    result[key] = sum(nums) / len(nums)
                elif fn == "min":
                    result[key] = min(nums)
                elif fn == "max":
                    result[key] = max(nums)

        return result

    def apply(self, message: Message) -> Message | None:
        """Buffer the message and emit aggregates when the window closes."""
        now = time.monotonic()
        group_key = self._get_group_key(message.value)

        with self._lock:
            if group_key not in self._window_start:
                self._window_start[group_key] = now

            self._windows[group_key].append(message.value)

            elapsed = now - self._window_start[group_key]
            if elapsed < self._window_seconds:
                # Window still open; buffer the message and suppress it
                return None

            # Window closed; compute and emit
            buffered = self._windows.pop(group_key)
            self._window_start.pop(group_key, None)

        aggregation = self._compute_aggregations(buffered)
        aggregation["group_key"] = group_key
        aggregation["window_start"] = self._window_start.get(group_key, now - elapsed)
        aggregation["window_end"] = now
        aggregation["message_count"] = len(buffered)

        message.value = {self._emit_field: aggregation}
        return message

    def flush(self) -> list[dict[str, Any]]:
        """Flush all open windows and return their aggregate results."""
        results: list[dict[str, Any]] = []
        with self._lock:
            for group_key, values in self._windows.items():
                if values:
                    agg = self._compute_aggregations(values)
                    agg["group_key"] = group_key
                    agg["message_count"] = len(values)
                    agg["flushed"] = True
                    results.append(agg)
            self._windows.clear()
            self._window_start.clear()
        return results

    def close(self) -> None:
        """Flush remaining windows on shutdown."""
        remaining = self.flush()
        if remaining:
            logger.info("Flushed %d aggregate windows on shutdown", len(remaining))

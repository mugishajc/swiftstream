"""Enrich transform: adds computed or static fields to messages."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from swiftstream.consumer import Message
from swiftstream.transforms.base import Transform

logger = logging.getLogger(__name__)

# Built-in enrichment functions
BUILTIN_ENRICHMENTS: dict[str, Callable[..., Any]] = {
    "timestamp_iso": lambda _msg: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "processing_timestamp_ms": lambda _msg: int(time.time() * 1000),
    "source_topic": lambda msg: msg.topic,
    "source_partition": lambda msg: msg.partition,
    "source_offset": lambda msg: msg.offset,
}


class EnrichTransform(Transform):
    """Add fields to the message value.

    Params:
        static_fields: dict of key-value pairs to add to every message
        computed_fields: dict mapping field names to built-in function names
        copy_fields: dict mapping new_field to existing_field (copies within the value)

    Example YAML config:
        - type: enrich
          params:
            static_fields:
              pipeline_version: "1.0"
              environment: production
            computed_fields:
              processed_at: timestamp_iso
              origin_topic: source_topic
    """

    def __init__(
        self,
        static_fields: dict[str, Any] | None = None,
        computed_fields: dict[str, str] | None = None,
        copy_fields: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._static_fields = static_fields or {}
        self._computed_fields = computed_fields or {}
        self._copy_fields = copy_fields or {}

        for fn_name in self._computed_fields.values():
            if fn_name not in BUILTIN_ENRICHMENTS:
                raise ValueError(
                    f"Unknown enrichment function: {fn_name}. "
                    f"Available: {', '.join(BUILTIN_ENRICHMENTS)}"
                )

    def apply(self, message: Message) -> Message | None:
        """Enrich the message with additional fields."""
        value = dict(message.value)

        # Add static fields
        value.update(self._static_fields)

        # Add computed fields
        for field_name, fn_name in self._computed_fields.items():
            fn = BUILTIN_ENRICHMENTS[fn_name]
            value[field_name] = fn(message)

        # Copy existing fields to new names
        for new_field, source_field in self._copy_fields.items():
            parts = source_field.split(".")
            current: Any = value
            for part in parts:
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    current = None
                    break
            if current is not None:
                value[new_field] = current

        message.value = value
        return message

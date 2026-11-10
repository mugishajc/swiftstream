"""Filter transform: drops messages that don't match criteria."""

from __future__ import annotations

import logging
import operator
from collections.abc import Callable
from typing import Any

from swiftstream.consumer import Message
from swiftstream.transforms.base import Transform

logger = logging.getLogger(__name__)

# Supported comparison operators for field-based filtering
OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "eq": operator.eq,
    "ne": operator.ne,
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
    "in": lambda val, options: val in options,
    "not_in": lambda val, options: val not in options,
    "contains": lambda val, substr: isinstance(val, str) and substr in val,
    "exists": lambda val, _: val is not None,
}


class FilterTransform(Transform):
    """Filter messages based on field conditions.

    Params:
        field: Dot-notation path to the field in the message value (e.g., "user.type")
        operator: One of eq, ne, gt, gte, lt, lte, in, not_in, contains, exists
        value: The comparison value
        negate: If True, invert the filter result (default False)

    Example YAML config:
        - type: filter
          params:
            field: event_type
            operator: in
            value: ["click", "purchase"]
    """

    def __init__(
        self,
        field: str = "",
        op: str = "eq",
        value: Any = None,
        negate: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._field = field
        self._op = op
        self._value = value
        self._negate = negate

        if self._op not in OPERATORS:
            raise ValueError(
                f"Unknown filter operator: {self._op}. Supported: {', '.join(OPERATORS)}"
            )

    def _resolve_field(self, data: dict[str, Any], path: str) -> Any:
        """Resolve a dot-notation field path against a dict."""
        parts = path.split(".")
        current: Any = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def apply(self, message: Message) -> Message | None:
        """Apply the filter. Returns the message if it passes, None otherwise."""
        field_value = self._resolve_field(message.value, self._field)
        op_fn = OPERATORS[self._op]

        try:
            result = op_fn(field_value, self._value)
        except (TypeError, ValueError):
            result = False

        if self._negate:
            result = not result

        if result:
            return message

        logger.debug(
            "Filtered out message at %s:%d@%d (field=%s, op=%s)",
            message.topic,
            message.partition,
            message.offset,
            self._field,
            self._op,
        )
        return None

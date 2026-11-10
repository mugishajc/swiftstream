"""Deduplicate transform: suppresses duplicate messages within a time window."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from typing import Any

from swiftstream.consumer import Message
from swiftstream.transforms.base import Transform

logger = logging.getLogger(__name__)


class DeduplicateTransform(Transform):
    """Drop duplicate messages based on a deduplication key.

    Uses an in-memory LRU cache with TTL expiry. Messages with the same
    dedup key seen within the TTL window are dropped.

    Params:
        key_fields: List of field names to use as the dedup key.
                    If empty, uses a hash of the entire message value.
        ttl_seconds: How long to remember seen keys (default: 300)
        max_cache_size: Maximum number of keys to track (default: 100000)
    """

    def __init__(
        self,
        key_fields: list[str] | None = None,
        ttl_seconds: float = 300.0,
        max_cache_size: int = 100_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._key_fields = key_fields or []
        self._ttl_seconds = ttl_seconds
        self._max_cache_size = max_cache_size

        # OrderedDict for LRU eviction: key -> expiry timestamp
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()
        self._duplicates_dropped = 0

    def _compute_key(self, message: Message) -> str:
        """Compute a deduplication key from the message."""
        if self._key_fields:
            parts: list[str] = []
            for field in self._key_fields:
                val = message.value.get(field)
                parts.append(str(val) if val is not None else "")
            raw = "|".join(parts)
        else:
            raw = json.dumps(message.value, sort_keys=True)

        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _evict_expired(self) -> None:
        """Remove expired entries from the cache."""
        now = time.monotonic()
        # Pop from the front (oldest) while expired
        while self._seen:
            key, expiry = next(iter(self._seen.items()))
            if expiry <= now:
                self._seen.pop(key)
            else:
                break

    def _evict_overflow(self) -> None:
        """Evict oldest entries if cache exceeds max size."""
        while len(self._seen) > self._max_cache_size:
            self._seen.popitem(last=False)

    def apply(self, message: Message) -> Message | None:
        """Check for duplicates. Returns None if the message is a duplicate."""
        dedup_key = self._compute_key(message)
        now = time.monotonic()
        expiry = now + self._ttl_seconds

        with self._lock:
            self._evict_expired()

            if dedup_key in self._seen:
                # Move to end (most recently seen) and update expiry
                self._seen.move_to_end(dedup_key)
                self._seen[dedup_key] = expiry
                self._duplicates_dropped += 1
                logger.debug(
                    "Duplicate dropped at %s:%d@%d (key=%s, total_dropped=%d)",
                    message.topic,
                    message.partition,
                    message.offset,
                    dedup_key[:8],
                    self._duplicates_dropped,
                )
                return None

            self._seen[dedup_key] = expiry
            self._evict_overflow()

        return message

    @property
    def duplicates_dropped(self) -> int:
        """Total number of duplicate messages dropped."""
        return self._duplicates_dropped

    @property
    def cache_size(self) -> int:
        """Current number of entries in the dedup cache."""
        with self._lock:
            return len(self._seen)

    def close(self) -> None:
        """Clear the dedup cache."""
        with self._lock:
            self._seen.clear()
        logger.info("Dedup cache cleared (dropped %d duplicates total)", self._duplicates_dropped)

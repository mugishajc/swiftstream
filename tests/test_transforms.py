"""Tests for transform functions."""

from __future__ import annotations

import pytest

from swiftstream.consumer import Message
from swiftstream.transforms.aggregate import AggregateTransform
from swiftstream.transforms.deduplicate import DeduplicateTransform
from swiftstream.transforms.enrich import EnrichTransform
from swiftstream.transforms.filter import FilterTransform


class TestFilterTransform:
    """Tests for the filter transform."""

    def test_eq_filter_passes(self, sample_message: Message) -> None:
        f = FilterTransform(field="event_type", op="eq", value="click")
        result = f.apply(sample_message)
        assert result is not None
        assert result.value["event_type"] == "click"

    def test_eq_filter_drops(self, sample_message: Message) -> None:
        f = FilterTransform(field="event_type", op="eq", value="purchase")
        result = f.apply(sample_message)
        assert result is None

    def test_in_filter(self, sample_message: Message) -> None:
        f = FilterTransform(field="event_type", op="in", value=["click", "purchase"])
        assert f.apply(sample_message) is not None

    def test_not_in_filter(self, bot_message: Message) -> None:
        f = FilterTransform(
            field="user_agent",
            op="not_in",
            value=["Googlebot", "Bingbot"],
        )
        result = f.apply(bot_message)
        assert result is None

    def test_gt_filter(self, sample_message: Message) -> None:
        f = FilterTransform(field="temperature", op="gt", value=20.0)
        assert f.apply(sample_message) is not None

    def test_negate_filter(self, sample_message: Message) -> None:
        f = FilterTransform(field="event_type", op="eq", value="click", negate=True)
        assert f.apply(sample_message) is None

    def test_nested_field(self) -> None:
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"user": {"type": "admin", "name": "root"}},
        )
        f = FilterTransform(field="user.type", op="eq", value="admin")
        assert f.apply(msg) is not None

    def test_invalid_operator_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown filter operator"):
            FilterTransform(field="x", op="nope", value=1)


class TestEnrichTransform:
    """Tests for the enrich transform."""

    def test_static_fields(self, sample_message: Message) -> None:
        e = EnrichTransform(static_fields={"env": "test", "version": "2.0"})
        result = e.apply(sample_message)
        assert result is not None
        assert result.value["env"] == "test"
        assert result.value["version"] == "2.0"

    def test_computed_fields(self, sample_message: Message) -> None:
        e = EnrichTransform(computed_fields={"origin": "source_topic"})
        result = e.apply(sample_message)
        assert result is not None
        assert result.value["origin"] == "test-topic"

    def test_copy_fields(self) -> None:
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"nested": {"id": 42}},
        )
        e = EnrichTransform(copy_fields={"flat_id": "nested.id"})
        result = e.apply(msg)
        assert result is not None
        assert result.value["flat_id"] == 42

    def test_unknown_computed_function_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown enrichment function"):
            EnrichTransform(computed_fields={"x": "nonexistent_fn"})


class TestDeduplicateTransform:
    """Tests for the dedup transform."""

    def test_first_message_passes(self, sample_message: Message) -> None:
        d = DeduplicateTransform(key_fields=["user_id"])
        assert d.apply(sample_message) is not None

    def test_duplicate_dropped(self, sample_message: Message) -> None:
        d = DeduplicateTransform(key_fields=["user_id"])
        d.apply(sample_message)
        # Same message again
        dup = Message(
            topic="test-topic",
            partition=0,
            offset=100,
            key="user-123",
            value={"user_id": "user-123", "x": "different"},
        )
        assert d.apply(dup) is None
        assert d.duplicates_dropped == 1

    def test_different_key_passes(self, sample_message: Message) -> None:
        d = DeduplicateTransform(key_fields=["user_id"])
        d.apply(sample_message)
        other = Message(
            topic="test-topic",
            partition=0,
            offset=100,
            key="user-456",
            value={"user_id": "user-456"},
        )
        assert d.apply(other) is not None

    def test_full_value_dedup(self) -> None:
        d = DeduplicateTransform()
        msg1 = Message(topic="t", partition=0, offset=0, key=None, value={"a": 1})
        msg2 = Message(topic="t", partition=0, offset=1, key=None, value={"a": 1})
        msg3 = Message(topic="t", partition=0, offset=2, key=None, value={"a": 2})
        assert d.apply(msg1) is not None
        assert d.apply(msg2) is None  # duplicate
        assert d.apply(msg3) is not None  # different value

    def test_cache_size_limit(self) -> None:
        d = DeduplicateTransform(max_cache_size=5)
        for i in range(10):
            msg = Message(topic="t", partition=0, offset=i, key=None, value={"id": i})
            d.apply(msg)
        assert d.cache_size <= 5


class TestAggregateTransform:
    """Tests for the aggregate transform."""

    def test_buffers_within_window(self) -> None:
        a = AggregateTransform(
            group_by="device_id",
            window_seconds=60.0,
            aggregations=[{"field": "*", "function": "count"}],
        )
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"device_id": "s1", "temp": 20},
        )
        # First message within window should be buffered (returns None)
        result = a.apply(msg)
        assert result is None

    def test_flush_returns_aggregates(self) -> None:
        a = AggregateTransform(
            group_by="device_id",
            window_seconds=60.0,
            aggregations=[
                {"field": "temp", "function": "avg"},
                {"field": "temp", "function": "sum"},
            ],
        )
        for i in range(5):
            msg = Message(
                topic="t",
                partition=0,
                offset=i,
                key=None,
                value={"device_id": "s1", "temp": 20 + i},
            )
            a.apply(msg)

        results = a.flush()
        assert len(results) == 1
        assert results[0]["group_key"] == "s1"
        assert results[0]["message_count"] == 5
        assert results[0]["avg_temp"] == 22.0  # (20+21+22+23+24)/5
        assert results[0]["sum_temp"] == 110.0

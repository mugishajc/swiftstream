"""Tests for the Kafka consumer wrapper."""

from __future__ import annotations

from swiftstream.consumer import Message


class TestMessage:
    """Tests for the Message dataclass."""

    def test_as_dict(self, sample_message: Message) -> None:
        d = sample_message.as_dict()
        assert d["topic"] == "test-topic"
        assert d["partition"] == 0
        assert d["offset"] == 42
        assert d["key"] == "user-123"
        assert d["value"]["event_type"] == "click"
        assert d["headers"] == {"source": "web"}

    def test_retry_count_starts_at_zero(self) -> None:
        msg = Message(topic="t", partition=0, offset=0, key=None, value={})
        assert msg._retry_count == 0

    def test_message_headers_default_empty(self) -> None:
        msg = Message(topic="t", partition=0, offset=0, key=None, value={"x": 1})
        assert msg.headers == {}

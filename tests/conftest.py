"""Shared fixtures for SwiftStream tests."""

from __future__ import annotations

import pytest

from swiftstream.consumer import Message


@pytest.fixture
def sample_message() -> Message:
    """A typical message for testing."""
    return Message(
        topic="test-topic",
        partition=0,
        offset=42,
        key="user-123",
        value={
            "event_type": "click",
            "user_id": "user-123",
            "page_url": "/products/widget",
            "user_agent": "Mozilla/5.0",
            "temperature": 22.5,
            "device_id": "sensor-01",
            "timestamp": 1700000000000,
        },
        headers={"source": "web"},
        timestamp=1700000000000,
    )


@pytest.fixture
def bot_message() -> Message:
    """A message from a bot crawler."""
    return Message(
        topic="test-topic",
        partition=0,
        offset=43,
        key=None,
        value={
            "event_type": "crawl",
            "user_id": None,
            "page_url": "/sitemap.xml",
            "user_agent": "Googlebot",
        },
        headers={},
        timestamp=1700000001000,
    )


@pytest.fixture
def batch_messages() -> list[Message]:
    """A batch of messages for testing sinks."""
    return [
        Message(
            topic="test-topic",
            partition=0,
            offset=i,
            key=f"key-{i}",
            value={"index": i, "data": f"payload-{i}"},
            headers={},
            timestamp=1700000000000 + i * 1000,
        )
        for i in range(10)
    ]

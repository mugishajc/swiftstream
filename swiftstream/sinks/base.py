"""Base class for pipeline sinks."""

from __future__ import annotations

import abc

from swiftstream.consumer import Message


class Sink(abc.ABC):
    """Base class for output sinks.

    Sinks receive processed messages and write them to an external system.
    They support batching via write_batch() and must implement flush()
    for clean shutdown.
    """

    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__

    @abc.abstractmethod
    def write(self, message: Message) -> None:
        """Write a single message to the sink."""
        ...

    def write_batch(self, messages: list[Message]) -> None:
        """Write a batch of messages. Default: iterate and call write()."""
        for msg in messages:
            self.write(msg)

    @abc.abstractmethod
    def flush(self) -> None:
        """Flush any buffered data to the sink."""
        ...

    def close(self) -> None:
        """Flush and release resources."""
        self.flush()

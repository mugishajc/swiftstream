"""Base class and protocol for pipeline transforms."""

from __future__ import annotations

import abc
from typing import Any

from swiftstream.consumer import Message


class Transform(abc.ABC):
    """Base class for pipeline transforms.

    Each transform receives a Message and returns either a modified Message
    (to continue processing) or None (to drop the message).
    """

    def __init__(self, name: str | None = None, **kwargs: Any) -> None:
        self.name = name or self.__class__.__name__

    @abc.abstractmethod
    def apply(self, message: Message) -> Message | None:
        """Process a message. Return None to drop it from the pipeline."""
        ...

    def __call__(self, message: Message) -> Message | None:
        """Allow transforms to be used as callables."""
        return self.apply(message)

    def close(self) -> None:  # noqa: B027
        """Clean up any resources held by the transform."""

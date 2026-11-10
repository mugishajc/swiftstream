"""Stdout sink: prints messages to standard output for debugging."""

from __future__ import annotations

import json
import sys
from typing import IO, Any

from swiftstream.consumer import Message
from swiftstream.sinks.base import Sink


class StdoutSink(Sink):
    """Writes messages to stdout in JSON format.

    Useful for debugging, local development, and piping output
    to other tools. Each message is printed as a single JSON line.
    """

    def __init__(
        self,
        output: IO[str] | None = None,
        pretty: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._output = output or sys.stdout
        self._pretty = pretty

    def write(self, message: Message) -> None:
        """Print the message as JSON to stdout."""
        data = message.as_dict()
        if self._pretty:
            line = json.dumps(data, indent=2, default=str)
        else:
            line = json.dumps(data, default=str)
        self._output.write(line + "\n")
        self._output.flush()

    def flush(self) -> None:
        """Flush the output stream."""
        self._output.flush()

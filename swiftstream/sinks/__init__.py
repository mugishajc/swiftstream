"""Output sinks for the pipeline."""

from swiftstream.sinks.base import Sink
from swiftstream.sinks.postgres import PostgresSink
from swiftstream.sinks.s3 import S3Sink
from swiftstream.sinks.stdout import StdoutSink

__all__ = [
    "Sink",
    "PostgresSink",
    "S3Sink",
    "StdoutSink",
]

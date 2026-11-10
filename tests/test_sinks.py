"""Tests for pipeline sinks."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

from swiftstream.config import PostgresConfig, S3Config
from swiftstream.consumer import Message
from swiftstream.sinks.stdout import StdoutSink


class TestStdoutSink:
    """Tests for the stdout sink."""

    def test_write_json_line(self, sample_message: Message) -> None:
        buf = io.StringIO()
        sink = StdoutSink(output=buf)
        sink.write(sample_message)
        output = buf.getvalue()
        parsed = json.loads(output.strip())
        assert parsed["topic"] == "test-topic"
        assert parsed["key"] == "user-123"
        assert parsed["value"]["event_type"] == "click"

    def test_write_pretty(self, sample_message: Message) -> None:
        buf = io.StringIO()
        sink = StdoutSink(output=buf, pretty=True)
        sink.write(sample_message)
        output = buf.getvalue()
        # Pretty output should have indentation
        assert "  " in output
        parsed = json.loads(output)
        assert parsed["topic"] == "test-topic"

    def test_batch_write(self, batch_messages: list[Message]) -> None:
        buf = io.StringIO()
        sink = StdoutSink(output=buf)
        sink.write_batch(batch_messages)
        lines = [line for line in buf.getvalue().strip().split("\n") if line]
        assert len(lines) == 10
        first = json.loads(lines[0])
        assert first["value"]["index"] == 0

    def test_flush_no_error(self) -> None:
        buf = io.StringIO()
        sink = StdoutSink(output=buf)
        sink.flush()  # should not raise


class TestPostgresSinkMocked:
    """Tests for the PostgreSQL sink with mocked database."""

    @patch("swiftstream.sinks.postgres.psycopg2")
    def test_write_buffers_messages(self, mock_pg: MagicMock, sample_message: Message) -> None:
        config = PostgresConfig(batch_size=5)
        from swiftstream.sinks.postgres import PostgresSink

        sink = PostgresSink(config)
        # Write fewer than batch_size messages
        sink.write(sample_message)
        assert len(sink._buffer) == 1
        # Should not have flushed yet
        mock_pg.connect.assert_not_called()

    @patch("swiftstream.sinks.postgres.psycopg2")
    def test_flush_calls_execute_values(
        self,
        mock_pg: MagicMock,
        batch_messages: list[Message],
    ) -> None:
        config = PostgresConfig(batch_size=100)
        from swiftstream.sinks.postgres import PostgresSink

        sink = PostgresSink(config)
        for msg in batch_messages:
            sink.write(msg)

        # Mock the connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.closed = False
        mock_pg.connect.return_value = mock_conn

        with patch("swiftstream.sinks.postgres.psycopg2.extras") as mock_extras:
            sink.flush()
            mock_extras.execute_values.assert_called_once()


class TestS3SinkMocked:
    """Tests for the S3 sink with mocked S3 client."""

    @patch("swiftstream.sinks.s3.boto3")
    def test_write_buffers(self, mock_boto: MagicMock, sample_message: Message) -> None:
        config = S3Config(batch_size=100)
        from swiftstream.sinks.s3 import S3Sink

        sink = S3Sink(config)
        sink.write(sample_message)
        assert len(sink._buffer) == 1

    @patch("swiftstream.sinks.s3.boto3")
    def test_flush_uploads_parquet(
        self,
        mock_boto: MagicMock,
        batch_messages: list[Message],
    ) -> None:
        config = S3Config(batch_size=100)
        from swiftstream.sinks.s3 import S3Sink

        sink = S3Sink(config)

        mock_client = MagicMock()
        mock_boto.client.return_value = mock_client

        for msg in batch_messages:
            sink.write(msg)

        sink.flush()
        mock_client.upload_fileobj.assert_called_once()
        call_args = mock_client.upload_fileobj.call_args
        assert call_args[0][1] == config.bucket
        assert call_args[0][2].endswith(".parquet")

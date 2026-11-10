"""Tests for the pipeline orchestration."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock

import pytest

from swiftstream.config import (
    DeadLetterConfig,
    PipelineConfig,
    SinkSpec,
    TransformSpec,
)
from swiftstream.consumer import Message
from swiftstream.pipeline import Pipeline


class TestPipelineConstruction:
    """Tests for pipeline setup from config."""

    def test_build_with_transforms_and_sinks(self) -> None:
        config = PipelineConfig(
            name="test-pipe",
            transforms=[
                TransformSpec(
                    name="f1",
                    type="filter",
                    params={"field": "x", "op": "eq", "value": 1},
                ),
                TransformSpec(name="e1", type="enrich", params={"static_fields": {"env": "test"}}),
            ],
            sinks=[
                SinkSpec(name="out", type="stdout"),
            ],
        )
        pipeline = Pipeline(config)
        assert len(pipeline._transforms) == 2
        assert len(pipeline._sinks) == 1
        assert pipeline._transforms[0].name == "f1"

    def test_unknown_transform_raises(self) -> None:
        config = PipelineConfig(
            transforms=[TransformSpec(name="bad", type="nonexistent")],
        )
        with pytest.raises(ValueError, match="Unknown transform type"):
            Pipeline(config)

    def test_unknown_sink_raises(self) -> None:
        config = PipelineConfig(
            sinks=[SinkSpec(name="bad", type="nonexistent")],
        )
        with pytest.raises(ValueError, match="Unknown sink type"):
            Pipeline(config)


class TestPipelineProcessing:
    """Tests for message processing through the pipeline."""

    def _make_pipeline(self) -> tuple[Pipeline, io.StringIO]:
        buf = io.StringIO()
        config = PipelineConfig(
            name="test",
            transforms=[
                TransformSpec(
                    name="keep-clicks",
                    type="filter",
                    params={"field": "event_type", "op": "eq", "value": "click"},
                ),
                TransformSpec(
                    name="tag",
                    type="enrich",
                    params={"static_fields": {"processed": True}},
                ),
            ],
            sinks=[SinkSpec(name="out", type="stdout")],
        )
        pipeline = Pipeline(config)
        # Replace the stdout sink's output
        from swiftstream.sinks.stdout import StdoutSink

        pipeline._sinks = [StdoutSink(output=buf, name="out")]
        return pipeline, buf

    def test_message_passes_through(self) -> None:
        pipeline, buf = self._make_pipeline()
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"event_type": "click", "page": "/home"},
        )
        pipeline._process_message(msg)
        output = json.loads(buf.getvalue().strip())
        assert output["value"]["event_type"] == "click"
        assert output["value"]["processed"] is True

    def test_filtered_message_not_written(self) -> None:
        pipeline, buf = self._make_pipeline()
        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"event_type": "purchase"},
        )
        pipeline._process_message(msg)
        assert buf.getvalue() == ""

    def test_dlq_on_repeated_failure(self) -> None:
        config = PipelineConfig(
            name="test",
            sinks=[SinkSpec(name="out", type="stdout")],
            dead_letter=DeadLetterConfig(max_retries=2),
        )
        pipeline = Pipeline(config)

        # Make the sink raise
        failing_sink = MagicMock()
        failing_sink.name = "failing"
        failing_sink.write.side_effect = RuntimeError("sink error")
        pipeline._sinks = [failing_sink]

        # Mock the producer
        pipeline._producer = MagicMock()

        msg = Message(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value={"data": 1},
        )

        # Process until DLQ fires (max_retries=2)
        pipeline._process_message(msg)  # retry 1
        pipeline._process_message(msg)  # retry 2, hits DLQ

        pipeline._producer.send_to_dlq.assert_called_once()

    def test_from_config_file(self, tmp_path: object) -> None:
        from pathlib import Path

        import yaml

        p = Path(str(tmp_path)) / "pipe.yaml"
        p.write_text(
            yaml.dump(
                {
                    "name": "file-test",
                    "sinks": [{"name": "out", "type": "stdout"}],
                }
            )
        )
        pipeline = Pipeline.from_config_file(str(p))
        assert pipeline._config.name == "file-test"

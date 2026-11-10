"""Tests for configuration loading."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from swiftstream.config import load_config


class TestConfigLoading:
    """Tests for YAML config loading and env overrides."""

    def test_default_config(self) -> None:
        config = load_config()
        assert config.name == "default-pipeline"
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.kafka.enable_auto_commit is False
        assert config.backpressure_max_buffered == 10_000

    def test_load_yaml_file(self, tmp_path: Path) -> None:
        yaml_content = {
            "name": "test-pipeline",
            "input_topics": ["orders", "payments"],
            "kafka": {
                "bootstrap_servers": "broker:9092",
                "group_id": "test-group",
            },
            "transforms": [
                {
                    "name": "my-filter",
                    "type": "filter",
                    "params": {"field": "x", "op": "eq", "value": 1},
                },
            ],
            "sinks": [
                {"name": "pg", "type": "postgres"},
            ],
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(yaml_content))

        config = load_config(str(config_file))
        assert config.name == "test-pipeline"
        assert config.input_topics == ["orders", "payments"]
        assert config.kafka.bootstrap_servers == "broker:9092"
        assert len(config.transforms) == 1
        assert config.transforms[0].name == "my-filter"
        assert len(config.sinks) == 1

    def test_env_override(self) -> None:
        os.environ["SWIFTSTREAM_KAFKA_BOOTSTRAP_SERVERS"] = "env-broker:9092"
        os.environ["SWIFTSTREAM_POSTGRES_PORT"] = "5433"
        try:
            config = load_config()
            assert config.kafka.bootstrap_servers == "env-broker:9092"
            assert config.postgres.port == 5433
        finally:
            del os.environ["SWIFTSTREAM_KAFKA_BOOTSTRAP_SERVERS"]
            del os.environ["SWIFTSTREAM_POSTGRES_PORT"]

    def test_empty_yaml_returns_defaults(self, tmp_path: Path) -> None:
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        config = load_config(str(config_file))
        assert config.name == "default-pipeline"

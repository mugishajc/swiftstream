"""CLI entry point for running a SwiftStream pipeline."""

from __future__ import annotations

import argparse
import logging

from swiftstream.config import load_config
from swiftstream.pipeline import Pipeline


def main() -> None:
    """Parse arguments and run the pipeline."""
    parser = argparse.ArgumentParser(
        prog="swiftstream",
        description="Run a SwiftStream event processing pipeline",
    )
    parser.add_argument(
        "-c",
        "--config",
        required=True,
        help="Path to the pipeline YAML configuration file",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config(args.config)
    pipeline = Pipeline(config)

    logging.getLogger(__name__).info(
        "Loaded pipeline %s with %d transforms and %d sinks",
        config.name,
        len(config.transforms),
        len(config.sinks),
    )

    pipeline.run()


if __name__ == "__main__":
    main()

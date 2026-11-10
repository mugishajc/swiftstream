"""Pluggable transform functions for the pipeline."""

from swiftstream.transforms.aggregate import AggregateTransform
from swiftstream.transforms.deduplicate import DeduplicateTransform
from swiftstream.transforms.enrich import EnrichTransform
from swiftstream.transforms.filter import FilterTransform

__all__ = [
    "AggregateTransform",
    "DeduplicateTransform",
    "EnrichTransform",
    "FilterTransform",
]

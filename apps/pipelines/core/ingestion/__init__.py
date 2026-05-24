"""Ingestion utilities."""

from .landing import LandingSpec
from .partitioning import LandingPartitionSchema

__all__ = [
    "LandingPartitionSchema",
    "LandingSpec",
]

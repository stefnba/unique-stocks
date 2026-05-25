"""Ingestion utilities."""

from .dataset import (
    BronzeDataset,
    already_ingested,
    bronze_record,
    bronze_records,
    canonical_json,
    normalized_row_hash,
    write_bronze,
)
from .keys import LandingDomain, LandingFileFormat, ObjectStorageKey
from .landing import LandingTarget, LandingTargetBase, PartitionedLandingTarget, SnapshotLandingTarget
from .parser import BronzeParseResult, attach_source_uri
from .partitioning import (
    LandingPartitionSchema,
    PartitionValue,
    normalize_partitions,
    partition_field_names,
    partition_path,
    serialize_partition_value,
)

__all__ = [
    "BronzeDataset",
    "BronzeParseResult",
    "LandingDomain",
    "LandingFileFormat",
    "LandingPartitionSchema",
    "LandingTarget",
    "LandingTargetBase",
    "ObjectStorageKey",
    "PartitionedLandingTarget",
    "PartitionValue",
    "SnapshotLandingTarget",
    "already_ingested",
    "attach_source_uri",
    "bronze_record",
    "bronze_records",
    "canonical_json",
    "normalized_row_hash",
    "normalize_partitions",
    "partition_field_names",
    "partition_path",
    "serialize_partition_value",
    "write_bronze",
]

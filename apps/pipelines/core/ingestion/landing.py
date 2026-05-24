from abc import ABC
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

from core.ingestion.partitioning import LandingPartitionBuilder, LandingPartitionSchema

type FileFormat = Literal["jsonl", "json"]


@dataclass(frozen=True, slots=True)
class LandingObjectReference:
    """A reference to a landing zone object."""

    key: str
    object_type: FileFormat

    @property
    def filename(self) -> str:
        """Return the filename of the landing zone object."""
        return self.key.split("/")[-1]

    @property
    def extension(self) -> str:
        """Return the extension of the landing zone object."""
        return self.filename.split(".")[-1]


@dataclass(frozen=True, slots=True)
class LandingSpecBase(ABC):
    """A base landing spec."""

    domain: str
    file_format: FileFormat
    provider: str = ""

    def _build_key(self, *args: Any, **kwargs: Any) -> str:
        """Build the landing key."""
        # specify the order of the paths
        paths = [
            self.domain,
            self.provider,
            LandingPartitionBuilder(**kwargs).build(),
            self.file_format,
        ]

        return "/".join(paths)


@dataclass(frozen=True, slots=True)
class PartitionedLandingSpec[P: LandingPartitionSchema](LandingSpecBase):
    """A partitioned landing spec."""

    partition_fields: type[P] = field(kw_only=True)

    def create_landing_key(self, partitions: P) -> LandingObjectReference:
        """Return the key for the landing spec."""
        return LandingObjectReference(key=self._build_key(**partitions), object_type=self.file_format)


@dataclass(frozen=True, slots=True)
class SnapshotLandingSpec(LandingSpecBase):
    """A snapshot landing spec."""

    snapshot_date: date = field(kw_only=True)

    def create_landing_key(self, snapshot_date: date) -> LandingObjectReference:
        """Return the key for the landing spec."""
        return LandingObjectReference(key=self._build_key(snapshot_date=snapshot_date), object_type=self.file_format)


class LandingSpec:
    """A landing spec."""

    partitioned = PartitionedLandingSpec
    snapshot = SnapshotLandingSpec

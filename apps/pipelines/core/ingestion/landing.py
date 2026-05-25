from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from datetime import date, datetime

from core.clients.storage.s3.base import S3ObjectRef, S3StorageClient
from core.ingestion.keys import LandingDomain, LandingFileFormat, ObjectStorageKey
from core.ingestion.partitioning import LandingPartitionSchema, normalize_partitions, partition_field_names
from core.ingestion.serialization import jsonable


@dataclass(frozen=True, slots=True)
class LandingTargetBase(ABC):
    """Base landing-zone target for raw provider payloads."""

    domain: LandingDomain
    file_format: LandingFileFormat = "jsonl"

    def _format_key(self, key: ObjectStorageKey) -> str:
        return key.key(self.file_format)

    def _save(self, s3: S3StorageClient, key: str, data: object) -> S3ObjectRef:
        return s3.save(key, jsonable(data), format=self.file_format)


@dataclass(frozen=True, slots=True)
class SnapshotLandingTarget(LandingTargetBase):
    """Landing target for full-replacement snapshot payloads."""

    def key(
        self,
        *,
        provider: str,
        snapshot_date: date | str,
        ingested_at: datetime | date | str | None = None,
    ) -> str:
        """Build the landing object key for a snapshot payload."""
        key = ObjectStorageKey.snapshot(
            provider,
            self.domain,
            snapshot_date=snapshot_date,
            ingested_at=ingested_at,
        )
        return self._format_key(key)

    def save(
        self,
        s3: S3StorageClient,
        *,
        provider: str,
        data: object,
        snapshot_date: date | str,
        ingested_at: datetime | date | str | None = None,
    ) -> S3ObjectRef:
        """Serialize and save raw provider data to this landing target."""
        key = self.key(provider=provider, snapshot_date=snapshot_date, ingested_at=ingested_at)
        return self._save(s3, key, data)


@dataclass(frozen=True, slots=True)
class PartitionedLandingTarget[P: LandingPartitionSchema](LandingTargetBase):
    """Landing target for payloads scoped by typed logical partitions."""

    partition_fields: type[P] = field(kw_only=True)
    include_ingested_at: bool = True

    @property
    def partition_field_names(self) -> tuple[str, ...]:
        """Return required partition field names in declaration order."""
        return partition_field_names(self.partition_fields)

    def key(
        self,
        *,
        provider: str,
        partitions: P,
        ingested_at: datetime | date | str | None = None,
    ) -> str:
        """Build the landing object key for a typed partition payload."""
        key = ObjectStorageKey.partitioned_from_mapping(
            provider,
            self.domain,
            normalize_partitions(
                self.partition_fields,
                partitions,
                include_ingested_at=self.include_ingested_at,
                ingested_at=ingested_at,
            ),
        )
        return self._format_key(key)

    def save(
        self,
        s3: S3StorageClient,
        *,
        provider: str,
        data: object,
        partitions: P,
        ingested_at: datetime | date | str | None = None,
    ) -> S3ObjectRef:
        """Serialize and save raw provider data to this landing target."""
        key = self.key(provider=provider, partitions=partitions, ingested_at=ingested_at)
        return self._save(s3, key, data)


class LandingTarget:
    """Factory catalog for concrete landing target types."""

    snapshot = SnapshotLandingTarget
    partitioned = PartitionedLandingTarget


__all__ = ["LandingFileFormat", "LandingTarget"]

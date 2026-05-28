from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime

from core.clients.storage.s3.base import S3ObjectRef, S3StorageClient
from core.ingestion.keys import LandingDomain, LandingFileFormat, ObjectStorageKey
from core.ingestion.partitioning import LandingPartitionSchema, normalize_partitions, partition_field_names
from core.ingestion.serialization import jsonable


@dataclass(frozen=True, slots=True)
class LandingWrite:
    """Metadata returned by landing-zone write tasks for audit recording."""

    dataset: str
    source_uri: str
    partition: Mapping[str, object] | None = None
    rows_raw: int | None = None
    byte_count: int | None = None
    content_hash: str | None = None


@dataclass(frozen=True, slots=True)
class LandingTargetBase(ABC):
    """Base landing-zone target for raw provider payloads."""

    domain: LandingDomain
    file_format: LandingFileFormat = "jsonl"
    audit_dataset: str | None = None

    @property
    def audit_dataset_name(self) -> str:
        """Return the dataset label stored in ``pipeline.landing_objects``."""
        return self.audit_dataset or str(self.domain)

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

    def landing_write(
        self,
        ref: S3ObjectRef,
        *,
        snapshot_date: date | str,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> LandingWrite:
        """Build audit metadata for a saved snapshot landing object."""
        return LandingWrite(
            dataset=self.audit_dataset_name,
            source_uri=ref.uri,
            partition={"snapshot_date": snapshot_date},
            rows_raw=rows_raw,
            byte_count=byte_count,
            content_hash=content_hash,
        )


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

    def landing_write(
        self,
        ref: S3ObjectRef,
        *,
        partitions: P,
        rows_raw: int | None = None,
        byte_count: int | None = None,
        content_hash: str | None = None,
    ) -> LandingWrite:
        """Build audit metadata for a saved partitioned landing object."""
        partition = normalize_partitions(self.partition_fields, partitions, include_ingested_at=False)
        return LandingWrite(
            dataset=self.audit_dataset_name,
            source_uri=ref.uri,
            partition=partition,
            rows_raw=rows_raw,
            byte_count=byte_count,
            content_hash=content_hash,
        )


class LandingTarget:
    """Factory catalog for concrete landing target types."""

    snapshot = SnapshotLandingTarget
    partitioned = PartitionedLandingTarget


__all__ = ["LandingFileFormat", "LandingTarget", "LandingWrite"]

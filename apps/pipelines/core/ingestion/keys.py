"""Canonical object-key layout for ingestion data."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Literal, Self

from core.ingestion.partitioning import PartitionValue, partition_path, serialize_partition_value

type LandingLayer = Literal["landing"]
type LandingFileFormat = Literal["json", "jsonl", "csv"]


class LandingDomain(StrEnum):
    """Stable storage dataset names used in ingestion object keys."""

    EXCHANGE = "exchange"
    EXCHANGE_SCHEDULE = "exchange_schedule"
    EOD_PRICE = "eod_price"
    FUNDAMENTAL = "fundamental"
    INSTRUMENT = "instrument"


def utc_now_stamp() -> str:
    """Return current UTC datetime as a path-safe string."""
    now = datetime.now(UTC).replace(microsecond=0)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


@dataclass(frozen=True, slots=True)
class ObjectStorageKey:
    """Construct canonical, Hive-compatible keys for object storage."""

    provider: str
    domain: LandingDomain
    partitions: dict[str, str] = field(default_factory=dict)
    layer: LandingLayer = "landing"
    filename: str | None = None

    @classmethod
    def snapshot(
        cls,
        provider: str,
        domain: LandingDomain,
        *,
        snapshot_date: date | str,
        ingested_at: datetime | date | str | None = None,
        layer: LandingLayer = "landing",
    ) -> Self:
        """Return a key builder for a full-replacement snapshot payload."""
        stamp = utc_now_stamp() if ingested_at is None else serialize_partition_value(ingested_at)
        return cls(
            provider=str(provider),
            domain=domain,
            partitions={"snapshot_date": serialize_partition_value(snapshot_date), "ingested_at": stamp},
            layer=layer,
            filename=str(domain),
        )

    @classmethod
    def partitioned(
        cls,
        provider: str,
        domain: LandingDomain,
        *,
        layer: LandingLayer = "landing",
        **partition_kwargs: PartitionValue,
    ) -> Self:
        """Return a key builder for a partitioned payload."""
        partitions = {key: serialize_partition_value(value) for key, value in partition_kwargs.items()}
        return cls(provider=str(provider), domain=domain, partitions=partitions, layer=layer)

    @classmethod
    def partitioned_from_mapping(
        cls,
        provider: str,
        domain: LandingDomain,
        partitions: Mapping[str, PartitionValue],
        *,
        layer: LandingLayer = "landing",
    ) -> Self:
        """Return a key builder from dynamic partition key/value pairs."""
        serialized = {key: serialize_partition_value(value) for key, value in partitions.items()}
        return cls(provider=str(provider), domain=domain, partitions=serialized, layer=layer)

    def key(self, suffix: str) -> str:
        """Return the full object key with *suffix* as the file extension."""
        ext = suffix.lstrip(".")
        filename = f"{self.filename}.{ext}" if self.filename else f"data.{ext}"
        parts = [self.layer, self.provider, str(self.domain)]
        partition_segments = partition_path(self.partitions)
        if partition_segments:
            parts.append(partition_segments)
        return "/".join(parts) + f"/{filename}"

    def jsonl(self) -> str:
        """Return this key with a ``.jsonl`` suffix."""
        return self.key("jsonl")

    def json(self) -> str:
        """Return this key with a ``.json`` suffix."""
        return self.key("json")

    def csv(self) -> str:
        """Return this key with a ``.csv`` suffix."""
        return self.key("csv")


__all__ = [
    "LandingDomain",
    "LandingFileFormat",
    "LandingLayer",
    "ObjectStorageKey",
    "utc_now_stamp",
]

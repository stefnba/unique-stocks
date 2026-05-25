from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import TypedDict

type PartitionValue = date | datetime | str | int


class LandingPartitionSchema(TypedDict):
    """Base class for typed landing partition payloads.

    Example:
    -------
    >>> from datetime import date
    >>> from pipelines.core.ingestion.partitioning import LandingPartitionSchema
    >>> class DailyPricePartition(LandingPartitionSchema):
    ...     exchange: str
    ...     bar_date: date
    >>> DailyPricePartition(exchange="US", bar_date=date(2026, 5, 9))
    {'exchange': 'US', 'bar_date': datetime.date(2026, 5, 9)}
    """


def partition_value(value: PartitionValue) -> str:
    """Serialize a partition value to a stable, path-safe string."""
    if isinstance(value, datetime):
        return value.replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%SZ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def partition_path(partitions: Mapping[str, PartitionValue]) -> str:
    """Return Hive-style ``key=value`` path segments for partition values."""
    return "/".join(f"{key}={partition_value(value)}" for key, value in partitions.items())


def partition_field_names(schema: type[LandingPartitionSchema]) -> tuple[str, ...]:
    """Return typed partition field names in declaration order."""
    return tuple(schema.__annotations__)


def normalize_partitions(
    schema: type[LandingPartitionSchema],
    partitions: LandingPartitionSchema,
    *,
    include_ingested_at: bool = True,
    ingested_at: datetime | date | str | None = None,
) -> dict[str, PartitionValue]:
    """Validate and order typed partition values for object-key building."""
    fields = partition_field_names(schema)
    missing = [field for field in fields if field not in partitions]
    if missing:
        raise ValueError(f"Missing landing partition fields for {schema.__name__}: {missing}")

    values = {field: partitions[field] for field in fields}
    if include_ingested_at:
        values["ingested_at"] = ingested_at or datetime.now(UTC).replace(microsecond=0)
    return values

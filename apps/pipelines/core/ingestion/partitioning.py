"""Typed partition helpers for landing object keys and audit metadata."""

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import TypedDict

type PartitionValue = date | datetime | str | int


class LandingPartitionSchema(TypedDict):
    """Base class for typed landing partition payloads.

    Example:
    -------
    >>> from datetime import date
    >>> from core.ingestion.partitioning import LandingPartitionSchema
    >>> class DailyPricePartition(LandingPartitionSchema):
    ...     exchange: str
    ...     bar_date: date
    >>> DailyPricePartition(exchange="US", bar_date=date(2026, 5, 9))
    {'exchange': 'US', 'bar_date': datetime.date(2026, 5, 9)}
    """


def serialize_partition_value(value: PartitionValue) -> str:
    """Serialize a partition value to a stable, path-safe string.

    Args:
        value: Date, datetime, string, or integer partition value.

    Returns:
        Path-safe string representation.
    """
    if isinstance(value, datetime):
        return value.replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%SZ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def partition_path(partitions: Mapping[str, PartitionValue]) -> str:
    """Return Hive-style ``key=value`` path segments for partition values.

    Args:
        partitions: Ordered partition mapping.

    Returns:
        Slash-joined ``key=value`` path segments.
    """
    return "/".join(f"{key}={serialize_partition_value(value)}" for key, value in partitions.items())


def partition_field_names(schema: type[LandingPartitionSchema]) -> tuple[str, ...]:
    """Return typed partition field names in declaration order.

    Args:
        schema: TypedDict partition schema.

    Returns:
        Declared partition field names.
    """
    return tuple(schema.__annotations__)


def normalize_partitions(
    schema: type[LandingPartitionSchema],
    partitions: LandingPartitionSchema,
    *,
    include_ingested_at: bool = True,
    ingested_at: datetime | date | str | None = None,
) -> dict[str, PartitionValue]:
    """Validate and order typed partition values for object-key building.

    ``LandingPartitionSchema`` is a ``TypedDict``, so static type checkers enforce
    field names and value types at compile time only. At runtime, tasks can still
    pass ``None``, floats, or other unexpected objects. This function validates
    every partition value before landing keys or audit metadata are built so bad
    inputs fail fast with ``TypeError`` instead of silently stringifying into
    malformed object-store paths (for example ``bar_date=None``).

    Args:
        schema: TypedDict partition schema that declares required fields.
        partitions: Runtime partition values.
        include_ingested_at: Whether to add an ``ingested_at`` value.
        ingested_at: Optional ingestion timestamp/date. Defaults to current UTC time.

    Returns:
        Ordered partition mapping ready for key construction or audit metadata.

    Raises:
        ValueError: If a declared partition field is missing.
        TypeError: If a partition or ``ingested_at`` value is not a
            ``date``, ``datetime``, ``str``, or ``int``.
    """
    fields = partition_field_names(schema)
    missing = [field for field in fields if field not in partitions]
    if missing:
        raise ValueError(f"Missing landing partition fields for {schema.__name__}: {missing}")

    provided = dict(partitions.items())
    values: dict[str, PartitionValue] = {}
    for field in fields:
        value = provided[field]
        if not isinstance(value, date | datetime | str | int):
            raise TypeError(f"Invalid landing partition value for {schema.__name__}.{field}: {value!r}")
        values[field] = value
    if include_ingested_at:
        resolved_ingested_at = ingested_at or datetime.now(UTC).replace(microsecond=0)
        if not isinstance(resolved_ingested_at, date | datetime | str | int):
            raise TypeError(
                f"Invalid landing partition value for {schema.__name__}.ingested_at: {resolved_ingested_at!r}"
            )
        values["ingested_at"] = resolved_ingested_at
    return values

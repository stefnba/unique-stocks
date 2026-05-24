from datetime import date, datetime
from typing import Any, TypedDict


class LandingPartitionSchema(TypedDict, total=False):
    """A partition of an ingestion run.

    This is a placeholder for the actual partition keys.
    """

    pass


class LandingPartitionBuilder:
    """A builder for landing partitions."""

    schema: dict[str, Any]

    def __init__(self, **kwargs: Any):
        """Initialize the builder."""
        self.schema = kwargs

    def build(self) -> str:
        """Build the partition key as hive partition."""
        if not self.schema:
            return ""

        serialized = {k: self._serialize_partition(v) for k, v in self.schema.items()}

        return "/".join([f"{k}={v}" for k, v in serialized.items()])

    def _serialize_partition(self, value: Any) -> str:
        """Serialize the partition value."""
        if isinstance(value, datetime):
            return value.replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%SZ")
        elif isinstance(value, date):
            return value.isoformat()
        else:
            return str(value)

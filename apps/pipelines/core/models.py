"""Base Pydantic models for the pipeline layer."""

import hashlib
import json
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict


class ProviderModel(BaseModel):
    """Base class for all raw provider API response models.

    Provides consistent Pydantic config and a ``to_bronze_record()`` method so
    any provider model can be written directly to a bronze lake table without a
    separate domain model or parser.

    Subclasses must declare a ``provider`` class variable::

        class MyModel(ProviderModel):
            provider: ClassVar[str] = "my_provider"
            some_field: str

    Fields should use snake_case Python names with ``Field(alias=...)`` for
    PascalCase or camelCase API keys. ``model_dump()`` then produces bronze
    column names directly.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    provider: ClassVar[str]

    def to_bronze_record(self) -> dict[str, Any]:
        """Serialise this provider response to a bronze lake row.

        Returns a dict of all model fields (snake_case) plus the bronze
        envelope columns: ``provider``, ``raw_json``, ``row_hash``.
        Pipeline-time columns not in the API response (e.g. ``snapshot_date``)
        should be merged in by the caller after this call.
        """
        payload = self.model_dump(mode="json")
        raw_json = json.dumps(payload, sort_keys=True)
        row_hash = hashlib.sha256(raw_json.encode()).hexdigest()
        return {**payload, "provider": self.provider, "raw_json": raw_json, "row_hash": row_hash}


class BronzeModel(BaseModel):
    """Base class for all domain models that are written to the bronze lake layer.

    Provides a standard ``to_bronze_record`` method so every domain model gets
    the same bronze envelope (raw_json + row_hash + provider) without repeating
    the serialisation logic.

    Subclasses just define their fields; this class handles the rest.
    """

    def to_bronze_record(self, provider: str = "eodhd") -> dict[str, Any]:
        """Serialise to a dict suitable for inserting into a ``bronze.*`` table.

        Uses ``model_dump(mode="json")`` so Pydantic handles type coercion
        (``Decimal`` → string, ``date`` → ISO-8601, etc.) without manual field
        mapping. The resulting JSON is deterministically sorted and SHA-256 hashed
        for deduplication at the lake layer.

        Args:
            provider: Data source identifier stored alongside the raw payload.

        Returns:
            Dict with keys matching the bronze table schema:
            ``ticker``, ``bar_date`` (or domain equivalent), ``provider``,
            ``raw_json``, ``row_hash``, plus all model fields for typed columns.
        """
        payload = self.model_dump(mode="json")
        raw_json = json.dumps(payload, sort_keys=True)
        row_hash = hashlib.sha256(raw_json.encode()).hexdigest()
        return {**payload, "provider": provider, "raw_json": raw_json, "row_hash": row_hash}

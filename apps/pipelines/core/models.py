"""Base Pydantic models for the pipeline layer."""

import hashlib
import json
from typing import Any

from pydantic import BaseModel


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

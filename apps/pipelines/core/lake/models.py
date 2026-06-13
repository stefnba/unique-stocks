"""Base Pydantic contract for normalized Bronze row models."""

from typing import Any

from pydantic import BaseModel, ConfigDict


class BronzeModel(BaseModel):
    """Base class for all domain models that are written to the bronze lake layer.

    Bronze models describe normalized DuckDB row shape only. Ingestion policy such as
    provider, landing keys, idempotency columns, ``raw_json``, ``row_hash``, and
    ``source_uri`` is handled by ``core.ingestion`` dataset specs.

    A Bronze model should be boring: typed columns, domain validation, and no
    knowledge of where the row came from or where it will be written.
    """

    model_config = ConfigDict(extra="forbid")

    def to_payload(self) -> dict[str, Any]:
        """Return the normalized Bronze row payload without ingestion metadata."""
        return self.model_dump(mode="json")

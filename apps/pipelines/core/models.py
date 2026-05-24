"""Base Pydantic model contracts for provider and domain boundaries.

These classes deliberately stay small. Provider-specific response models live
under ``providers/`` and inherit from :class:`ProviderModel`. Domain-owned
Bronze row models live under ``domains/<domain>/`` and inherit from
:class:`BronzeModel`.

Ingestion policy does not live here. Provider names, landing paths, idempotency
rules, ``raw_json``, ``row_hash``, and ``source_uri`` are owned by dataset specs
and helpers in ``core.ingestion``.
"""

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict


class ProviderModel(BaseModel):
    """Base class for all raw provider API response models.

    Provider models represent external API contracts only. They validate
    provider responses before landing storage and must not know how Bronze
    records are shaped or written.

    Use aliases to describe provider field names exactly, especially when the
    provider uses PascalCase or camelCase. Parser code should consume the
    Python field names; landing and ``raw_json`` serialization can still emit
    provider aliases to preserve source-payload fidelity.

    Subclasses must declare a ``provider`` class variable::

        class MyModel(ProviderModel):
            provider: ClassVar[str] = "my_provider"
            some_field: str

    ``extra="forbid"`` is set here on purpose so unexpected API fields fail
    validation early instead of quietly drifting into storage.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    provider: ClassVar[str]


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

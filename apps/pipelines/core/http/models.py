"""Base Pydantic contract for raw provider API response models."""

from typing import ClassVar

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
            provider = "my_provider"
            some_field: str

    ``extra="forbid"`` is set here on purpose so unexpected API fields fail
    validation early instead of quietly drifting into storage.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    provider: ClassVar[str]

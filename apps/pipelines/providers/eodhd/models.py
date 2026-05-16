"""Raw response models for the EODHD API.

Field names mirror the API response verbatim. Normalisation to domain model
field names (e.g. code → ticker, date → bar_date) happens in the domain
transform layer, where exchange context is also available.
"""

from pydantic import BaseModel, TypeAdapter


class EODBulkPriceRaw(BaseModel):
    """One row from the EODHD bulk EOD endpoint."""

    code: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: float | None = None


bulk_price_adapter: TypeAdapter[list[EODBulkPriceRaw]] = TypeAdapter(list[EODBulkPriceRaw])

"""Bronze models for exchange reference data."""

from datetime import date

from core.models import BronzeModel


class ExchangeSnapshot(BronzeModel):
    """One exchange reference row for a snapshot date."""

    snapshot_date: date
    exchange_code: str
    name: str
    operating_mic: str | None = None
    country: str
    currency: str
    country_iso2: str
    country_iso3: str

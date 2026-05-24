"""Bronze models for instrument reference data."""

from datetime import date

from core.models import BronzeModel


class InstrumentSnapshot(BronzeModel):
    """One active instrument row for a snapshot date."""

    snapshot_date: date
    exchange_code: str
    ticker: str
    name: str
    country: str | None = None
    exchange: str
    currency: str | None = None
    asset_type: str | None = None
    isin: str | None = None

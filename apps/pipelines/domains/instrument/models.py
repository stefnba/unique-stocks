"""Bronze models for instrument reference data."""

from datetime import date

from core.models import BronzeModel


class InstrumentSnapshot(BronzeModel):
    """One active instrument row for a snapshot date.

    ``provider_exchange_code`` is the provider request/symbol-suffix code used to
    fetch the instrument list. ``provider_listing_exchange_code`` is the
    exchange-like code the provider returns on the individual instrument row.
    """

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    name: str
    country: str | None = None
    provider_listing_exchange_code: str
    currency: str | None = None
    asset_type: str | None = None
    isin: str | None = None

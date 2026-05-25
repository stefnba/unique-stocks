"""Bronze models for exchange reference data."""

from datetime import date

from core.models import BronzeModel


class ExchangeSnapshot(BronzeModel):
    """One provider exchange reference row for a snapshot date.

    ``provider_exchange_code`` is the provider's catalog/API code. ``operating_mic_codes``
    stores the official MIC value or comma-separated MIC values supplied by the provider.
    """

    snapshot_date: date
    provider_exchange_code: str
    name: str
    operating_mic_codes: str | None = None
    country: str
    currency: str
    country_iso2: str
    country_iso3: str

"""Bronze models for exchange reference data."""

from datetime import date

from core.models import BronzeModel


class ExchangeCatalogSnapshot(BronzeModel):
    """One provider-supported exchange catalog row for a snapshot date.

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


class ExchangeMicRegistrySnapshot(BronzeModel):
    """One ISO 10383 MIC registry row for a snapshot date."""

    snapshot_date: date
    mic: str
    operating_mic: str
    mic_type: str
    name: str
    legal_entity_name: str | None = None
    lei: str | None = None
    market_category_code: str | None = None
    acronym: str | None = None
    country_iso2: str
    city: str
    website: str | None = None
    status: str
    creation_date: date
    last_update_date: date | None = None
    last_validation_date: date | None = None
    expiry_date: date | None = None
    comments: str | None = None
    provider_supported: bool = True

"""Models for the raw responses from the EODHD API."""

from typing import ClassVar

from pydantic import Field

from core.models import ProviderModel
from providers.registry import Provider

class EODHDProviderModel(ProviderModel):
    """Base class for all EODHD provider models."""
    provider = Provider.EODHD


class EODBulkPriceRaw(EODHDProviderModel):
    """One row from the EODHD bulk EOD endpoint."""



    code: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: float | None = None


class SupportedExchange(EODHDProviderModel):
    """One exchange from the EODHD supported exchanges endpoint.

    Field names are snake_case (bronze column names); PascalCase aliases
    match the raw API response keys.
    """

    exchange_code: str = Field(alias="Code")
    name: str = Field(alias="Name")
    operating_mic: str | None = Field(alias="OperatingMIC", default=None)
    country: str = Field(alias="Country")
    currency: str = Field(alias="Currency")
    country_iso2: str = Field(alias="CountryISO2")
    country_iso3: str = Field(alias="CountryISO3")

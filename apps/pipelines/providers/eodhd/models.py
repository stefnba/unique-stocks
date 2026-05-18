
from pydantic import BaseModel


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


class ExchangeList(BaseModel):
    """
    Model representing an exchange available via EODHD.

    Attributes:
        Name: Full name of the exchange
        Code: Exchange code used in EODHD APIs
        OperatingMIC: MIC codes for operating venues
        Country: Country where the exchange operates
        Currency: Default trading currency
        CountryISO2: ISO2 country code
        CountryISO3: ISO3 country code
    """

    Name: str
    Code: str
    OperatingMIC: str | None 
    Country: str
    Currency: str
    CountryISO2: str
    CountryISO3: str
"""Models for the raw responses from the EODHD API."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from core.models import ProviderModel
from providers.registry import Provider


class EODHDProviderModel(ProviderModel):
    """Base class for all EODHD provider models."""

    provider = Provider.EODHD


class EODBulkPriceRaw(EODHDProviderModel):
    """One row from the EODHD bulk EOD endpoint (GET /eod-bulk-last-day/{exchange}).

    Includes a ``code`` field because the bulk response contains all tickers
    for the exchange. The ticker is unknown from context alone.
    """

    code: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: float | None = None
    exchange_short_name: str | None = None


class EODPriceBarRaw(EODHDProviderModel):
    """One OHLCV bar from the per-ticker historical EOD endpoint (GET /eod/{symbol}).

    No ``code`` field — the ticker is the URL path parameter and known by the caller.
    """

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


class TradingHoursRaw(BaseModel):
    """Trading session times from the v2 exchange-details endpoint."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    session_open: str = Field(alias="Open")
    session_close: str = Field(alias="Close")
    working_days: str = Field(alias="WorkingDays")
    pre_market_open: str | None = Field(alias="PreMarketOpen", default=None)
    pre_market_close: str | None = Field(alias="PreMarketClose", default=None)
    after_hours_open: str | None = Field(alias="AfterHoursOpen", default=None)
    after_hours_close: str | None = Field(alias="AfterHoursClose", default=None)
    lunch_break_start: str | None = Field(alias="LunchBreakStart", default=None)
    lunch_break_end: str | None = Field(alias="LunchBreakEnd", default=None)


class ExchangeHolidayRaw(BaseModel):
    """One holiday entry from the v2 exchange-details endpoint."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    holiday_name: str = Field(alias="Holiday")
    holiday_type: str = Field(alias="Type")
    early_close_time: str | None = Field(alias="EarlyClose", default=None)


class ExchangeSchedule(EODHDProviderModel):
    """Exchange trading hours and holidays from the EODHD v2 exchange-details endpoint."""

    name: str = Field(alias="Name")
    exchange_code: str = Field(alias="Code")
    timezone: str = Field(alias="Timezone")
    trading_hours: TradingHoursRaw = Field(alias="TradingHours")
    exchange_holidays: dict[str, ExchangeHolidayRaw] = Field(
        alias="ExchangeHolidays",
        default_factory=dict,
    )



class Instrument(EODHDProviderModel):
    """One instrument from the EODHD exchange-symbol-list endpoint.

    Covers all asset classes: equities, ETFs, forex pairs, cryptocurrencies,
    bonds, and funds. ``exchange`` is the sub-exchange from the API response
    (e.g. NYSE, NASDAQ). The API call code used to fetch this instrument
    (e.g. "US", "FOREX", "CC") is added separately at the task layer as
    ``exchange_code``.
    """

    ticker: str = Field(alias="Code")
    name: str = Field(alias="Name")
    country: str | None = Field(alias="Country", default=None)
    exchange: str = Field(alias="Exchange")
    currency: str | None = Field(alias="Currency", default=None)
    asset_type: str | None = Field(alias="Type", default=None)
    isin: str | None = Field(alias="Isin", default=None)


class ExchangeDetails(BaseModel):
    """Wrapper for the v2 exchange-details API response envelope."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    data: ExchangeSchedule
    meta: list[Any] = Field(default_factory=list)
    links: list[Any] = Field(default_factory=list)


class ExchangeDetailsCode(BaseModel):
    """Wrapper for GET /v2/exchange-details — list of supported schedule codes."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    data: list[str]
    meta: list[Any] = Field(default_factory=list)
    links: list[Any] = Field(default_factory=list)

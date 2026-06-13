"""Models for the raw responses from the EODHD API."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from config.providers import Provider
from core.http.models import ProviderModel


class EODHDProviderModel(ProviderModel):
    """Base class for all EODHD provider models."""

    provider = Provider.EODHD


class EODBulkPriceRaw(EODHDProviderModel):
    """One row from the EODHD bulk EOD endpoint (GET /eod-bulk-last-day/{exchange}).

    Includes a ``code`` field because the bulk response contains all instruments
    for the exchange. The instrument code is unknown from context alone.
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
    """One OHLCV bar from the per-instrument historical EOD endpoint (GET /eod/{api_symbol}).

    No ``code`` field; the EODHD API symbol is the URL path parameter and known by the caller.
    """

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: float | None = None


class FundamentalRaw(EODHDProviderModel):
    """One fundamentals document from GET /v1.1/fundamentals/{symbol}.

    The Fundamentals API is a large, instrument-family-specific JSON document:
    common stocks, ETFs, mutual funds, and indices expose different nested
    sections. This model validates the provider's top-level section contract
    while leaving family-specific nested validation to domain parsers.
    """

    general: dict[str, Any] = Field(alias="General")
    highlights: dict[str, Any] | None = Field(alias="Highlights", default=None)
    valuation: dict[str, Any] | None = Field(alias="Valuation", default=None)
    shares_stats: dict[str, Any] | None = Field(alias="SharesStats", default=None)
    technicals: dict[str, Any] | None = Field(alias="Technicals", default=None)
    splits_dividends: dict[str, Any] | None = Field(alias="SplitsDividends", default=None)
    analyst_ratings: dict[str, Any] | None = Field(alias="AnalystRatings", default=None)
    holders: dict[str, Any] | None = Field(alias="Holders", default=None)
    insider_transactions: dict[str, Any] | None = Field(alias="InsiderTransactions", default=None)
    esg_scores: dict[str, Any] | None = Field(alias="ESGScores", default=None)
    outstanding_shares: dict[str, Any] | None = Field(alias="outstandingShares", default=None)
    earnings: dict[str, Any] | None = Field(alias="Earnings", default=None)
    financials: dict[str, Any] | None = Field(alias="Financials", default=None)
    etf_data: dict[str, Any] | None = Field(alias="ETF_Data", default=None)
    mutual_fund_data: dict[str, Any] | None = Field(alias="MutualFund_Data", default=None)
    components: dict[str, Any] | None = Field(alias="Components", default=None)
    historical_components: dict[str, Any] | None = Field(alias="HistoricalComponents", default=None)
    historical_ticker_components: dict[str, Any] | None = Field(alias="HistoricalTickerComponents", default=None)


class SupportedExchange(EODHDProviderModel):
    """One exchange from the EODHD supported exchanges endpoint.

    ``Code`` is the provider-specific exchange code used in EODHD endpoints and
    API symbol suffixes (for example ``US`` or ``LSE``). ``OperatingMIC`` contains one or
    more official MIC values when EODHD supplies them.
    """

    provider_exchange_code: str = Field(alias="Code")
    name: str = Field(alias="Name")
    operating_mic_codes: str | None = Field(alias="OperatingMIC", default=None)
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
    """Exchange trading hours and holidays from the EODHD v2 exchange-details endpoint.

    ``Code`` is the code used by the v2 endpoint. It can look like a MIC
    (for example ``XHKG``), but EODHD also returns provider codes such as
    ``US`` here, so it is stored separately from official MIC metadata.
    """

    name: str = Field(alias="Name")
    provider_schedule_exchange_code: str = Field(alias="Code")
    timezone: str = Field(alias="Timezone")
    trading_hours: TradingHoursRaw = Field(alias="TradingHours")
    exchange_holiday: dict[str, ExchangeHolidayRaw] = Field(
        alias="ExchangeHolidays",
        default_factory=dict,
    )


class Instrument(EODHDProviderModel):
    """One instrument from the EODHD exchange-symbol-list endpoint.

    Covers all asset classes: equities, ETFs, forex pairs, cryptocurrencies,
    bonds, and funds. ``provider_listing_exchange_code`` is EODHD's per-row
    ``Exchange`` value (for example ``NYSE`` or ``NASDAQ``), when supplied.
    The API call code used to fetch this instrument (for example ``US``,
    ``FOREX``, or ``CC``) is added separately at the task layer as
    ``provider_exchange_code``.
    """

    provider_instrument_code: str = Field(alias="Code")
    name: str = Field(alias="Name")
    country: str | None = Field(alias="Country", default=None)
    provider_listing_exchange_code: str | None = Field(alias="Exchange", default=None)
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

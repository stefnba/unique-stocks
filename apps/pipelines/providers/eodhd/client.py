"""EODHD API client.

Docs: https://eodhd.com/financial-apis/
Bulk EOD endpoint: GET /api/eod-bulk-last-day/{exchange}?api_token=...&date=YYYY-MM-DD&fmt=json
"""

from collections.abc import Generator
from datetime import date

import httpx
import structlog

from core.clients.http.base import HttpClientBase
from providers.registry import Provider

from .models import (
    EODBulkPriceRaw,
    EODPriceBarRaw,
    ExchangeDetails,
    ExchangeDetailsCode,
    ExchangeSchedule,
    Instrument,
    SupportedExchange,
)

log = structlog.get_logger(__name__)


class _EodhdAuth(httpx.Auth):
    """Injects the EODHD API token as a query parameter on every request."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        request.url = request.url.copy_merge_params({"api_token": self._api_key})
        yield request


class EODHDClient(HttpClientBase):
    """HTTP client for the EODHD financial data API."""

    PROVIDER = Provider.EODHD
    BASE_URL = "https://eodhd.com/api"

    def __init__(self, api_key: str, timeout: float = 30.0) -> None:
        """Initialize with an EODHD API key and optional request timeout."""
        self._api_key = api_key
        self._timeout = timeout

    @property
    def _auth(self) -> httpx.Auth:
        return _EodhdAuth(self._api_key)

    @property
    def _default_params(self) -> dict[str, str]:
        return {"fmt": "json"}

    async def get_eod_price_bulk(self, exchange: str, bar_date: date | None = None) -> list[EODBulkPriceRaw]:
        """All tickers for an exchange on one date (one API call per exchange).

        If ``bar_date`` is omitted, EODHD returns its latest available trading
        day for the exchange.
        Response is validated against EODBulkPriceRaw — raises ValidationError
        if EODHD changes their schema.
        """
        params: dict[str, str] = {}
        if bar_date is not None:
            params["date"] = bar_date.isoformat()
        rows = await self._get_list(
            f"/eod-bulk-last-day/{exchange}",
            model=EODBulkPriceRaw,
            params=params,
        )
        return rows

    async def get_eod_price_ticker(
        self,
        symbol: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[EODPriceBarRaw]:
        """Full OHLCV history for one instrument (GET /eod/{symbol}).

        ``symbol`` is the exchange-qualified identifier, e.g. ``AAPL.US``,
        ``EURUSD.FOREX``, ``BTC-USD.CC``.
        One API call regardless of the requested date range.
        """
        params: dict[str, str] = {"period": "d"}
        if from_date:
            params["from"] = from_date.isoformat()
        if to_date:
            params["to"] = to_date.isoformat()
        return await self._get_list(f"/eod/{symbol}", model=EODPriceBarRaw, params=params)

    async def get_exchange(self) -> list[SupportedExchange]:
        """Get the exchange catalog available via EODHD."""
        return await self._get_list(
            "/exchanges-list",
            model=SupportedExchange,
        )

    async def get_exchange_details(self, exchange_code: str) -> ExchangeSchedule:
        """Trading hours and holidays for one exchange (v2 endpoint).

        Raises httpx.HTTPStatusError when the exchange is not supported by v2.
        """
        response = await self._get(
            f"/v2/exchange-details/{exchange_code}",
            model=ExchangeDetails,
        )
        return response.data

    async def get_instrument(
        self,
        exchange_code: str,
        asset_type: str | None = None,
    ) -> list[Instrument]:
        """All active instruments for one exchange (GET /exchange-symbol-list/{code}).

        Covers equities, ETFs, forex pairs, crypto, bonds, and funds depending
        on the exchange code. ``asset_type`` filters by instrument type;
        supported values: common_stock, preferred_stock, stock, etf, fund.
        For US equities pass exchange_code="US" — it covers NYSE, NASDAQ,
        NYSE ARCA, and OTC in a single call.
        """
        params: dict[str, str] = {}
        if asset_type:
            params["type"] = asset_type
        return await self._get_list(
            f"/exchange-symbol-list/{exchange_code}",
            model=Instrument,
            params=params,
        )

    async def get_exchange_details_codes(self) -> list[str]:
        """Return exchange codes supported by the v2 trading-hours/holidays endpoint.

        These codes differ from ``/exchanges-list`` (e.g. ``XETR`` vs ``XETRA``).
        """
        response = await self._get(
            "/v2/exchange-details",
            model=ExchangeDetailsCode,
        )
        return response.data

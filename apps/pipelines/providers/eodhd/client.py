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
    ExchangeDetails,
    ExchangeDetailsCode,
    ExchangeSchedule,
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

    async def get_eod_prices_bulk(self, exchange: str, bar_date: date) -> list[EODBulkPriceRaw]:
        """All tickers for an exchange on one date (one API call per exchange).

        Response is validated against EODBulkPriceRaw — raises ValidationError
        if EODHD changes their schema.
        """
        rows = await self._get_list(
            f"/eod-bulk-last-day/{exchange}",
            model=EODBulkPriceRaw,
            params={"date": bar_date.isoformat()},
        )
        return rows

    async def get_exchanges(self) -> list[SupportedExchange]:
        """Get all exchanges available via EODHD."""
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

    async def get_exchange_details_codes(self) -> list[str]:
        """Exchange codes supported by the v2 trading-hours/holidays endpoint.

        These codes differ from ``/exchanges-list`` (e.g. ``XETR`` vs ``XETRA``).
        """
        response = await self._get(
            "/v2/exchange-details",
            model=ExchangeDetailsCode,
        )
        return response.data

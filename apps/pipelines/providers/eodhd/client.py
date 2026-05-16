"""EODHD API client.

Docs: https://eodhd.com/financial-apis/
Bulk EOD endpoint: GET /api/eod-bulk-last-day/{exchange}?api_token=...&date=YYYY-MM-DD&fmt=json
"""

from collections.abc import Generator
from datetime import date

import httpx
import structlog

from core.client.base import BaseClient

from .models import EODBulkPriceRaw, bulk_price_adapter

log = structlog.get_logger(__name__)


class _EodhdAuth(httpx.Auth):
    """Injects the EODHD API token as a query parameter on every request."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response]:
        request.url = request.url.copy_merge_params({"api_token": self._api_key})
        yield request


class EODHDClient(BaseClient):
    """HTTP client for the EODHD financial data API."""

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
        data = await self._request(
            f"/eod-bulk-last-day/{exchange}",
            params={"date": bar_date.isoformat()},
        )
        rows = bulk_price_adapter.validate_python(data)
        log.info("eodhd.bulk_prices_fetched", exchange=exchange, bar_date=bar_date, row_count=len(rows))
        return rows

"""
EODHD API client.

Docs: https://eodhd.com/financial-apis/
Bulk EOD endpoint: GET /api/eod-bulk-last-day/{exchange}?api_token=...&date=YYYY-MM-DD&fmt=json
Single ticker EOD: GET /api/eod/{ticker}.{exchange}?api_token=...&from=...&to=...&fmt=json
"""

import logging
from datetime import date

import httpx
import structlog

from shared.clients.base import BaseClient

log = structlog.get_logger(__name__)

BASE_URL = "https://eodhd.com/api"

# EODHD returns extended field on the bulk endpoint — normalise to our expected keys
_BULK_KEY_MAP = {
    "code": "ticker",
    "date": "bar_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "adjusted_close": "adjusted_close",
}


class EODHDClient(BaseClient):
    def __init__(self, api_key: str, timeout: float = 30.0) -> None:
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=httpx.Timeout(timeout),
            # Retry is handled by Prefect task retries — keep the client simple
        )

    async def __aenter__(self) -> "EODHDClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # EOD prices
    # ------------------------------------------------------------------

    async def get_eod_prices(self, ticker: str, bar_date: date) -> list[dict]:
        """Single ticker, single date.  Useful for backfill of individual tickers."""
        date_str = bar_date.isoformat()
        # EODHD single ticker format: TICKER.EXCHANGE  e.g. AAPL.US
        resp = await self._get(
            f"/eod/{ticker}",
            params={"from": date_str, "to": date_str, "fmt": "json"},
        )
        rows: list[dict] = resp
        return [self._normalise_single(r, ticker) for r in rows]

    async def get_eod_prices_bulk(self, exchange: str, bar_date: date) -> list[dict]:
        """
        Bulk endpoint — returns all tickers for an exchange on a given date.
        This is the preferred path for the daily flow (one API call per exchange
        instead of N calls for N tickers).
        """
        resp = await self._get(
            f"/eod-bulk-last-day/{exchange}",
            params={"date": bar_date.isoformat(), "fmt": "json"},
        )
        rows: list[dict] = resp
        log.info(
            "eodhd.bulk_prices_fetched",
            exchange=exchange,
            bar_date=bar_date,
            row_count=len(rows),
        )
        return [self._normalise_bulk(r, exchange) for r in rows]

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_securities(self, exchange: str) -> list[dict]:
        resp = await self._get(f"/exchange-symbol-list/{exchange}", params={"fmt": "json"})
        return resp  # type: ignore[return-value]

    async def get_exchanges(self) -> list[dict]:
        resp = await self._get("/exchanges-list", params={"fmt": "json"})
        return resp  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    async def get_fundamentals(self, ticker: str) -> dict:
        resp = await self._get(f"/fundamentals/{ticker}", params={"fmt": "json"})
        return resp  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: dict | None = None) -> list[dict] | dict:
        all_params = {"api_token": self._api_key, **(params or {})}
        log.debug("eodhd.request", path=path)
        response = await self._client.get(path, params=all_params)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalise_single(row: dict, ticker: str) -> dict:
        return {
            "ticker": ticker,
            "bar_date": row["date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "adjusted_close": row.get("adjusted_close"),
        }

    @staticmethod
    def _normalise_bulk(row: dict, exchange: str) -> dict:
        # Bulk response uses "code" for ticker — append exchange suffix for uniqueness
        ticker = f"{row['code']}.{exchange}"
        return {
            "ticker": ticker,
            "bar_date": row["date"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "adjusted_close": row.get("adjusted_close"),
        }

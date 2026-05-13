from abc import ABC, abstractmethod
from datetime import date


class BaseClient(ABC):
    @abstractmethod
    async def get_eod_prices(self, ticker: str, bar_date: date) -> list[dict]:
        """Fetch EOD OHLCV for a single ticker on a single date."""
        ...

    @abstractmethod
    async def get_eod_prices_bulk(self, exchange: str, bar_date: date) -> list[dict]:
        """Fetch EOD OHLCV for all tickers on an exchange for a single date (bulk endpoint)."""
        ...

    @abstractmethod
    async def get_securities(self, exchange: str) -> list[dict]:
        """List all securities for a given exchange."""
        ...

    @abstractmethod
    async def get_exchanges(self) -> list[dict]:
        """List all available exchanges."""
        ...

    @abstractmethod
    async def get_fundamentals(self, ticker: str) -> dict:
        """Fetch full fundamentals payload for a ticker."""
        ...

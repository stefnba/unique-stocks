from datetime import date
from typing import List, Optional

from lib.hooks.http.hook import BaseHttpHook, HttpJSONResponseModel
from pydantic import RootModel


class IndexMemberGeneralModel(HttpJSONResponseModel):
    Code: str
    Type: str
    Name: str
    Exchange: str
    MarketCap: int | float | None
    CurrencyCode: str
    CurrencyName: str
    CurrencySymbol: str
    CountryName: str
    CountryISO: str
    OpenFigi: str | None


class IndexMemberModel(HttpJSONResponseModel):
    General: IndexMemberGeneralModel
    Components: dict


class IndexMemberListModel(RootModel):
    root: List[IndexMemberModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> IndexMemberModel:
        return self.root[item]


class ExchangeSecurityModel(HttpJSONResponseModel):
    Code: str
    Name: str
    Exchange: str
    Country: str
    Currency: str
    Type: Optional[str] = None
    Isin: Optional[str] = None


class ExchangeSecurityListModel(RootModel):
    root: List[ExchangeSecurityModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> ExchangeSecurityModel:
        return self.root[item]


class ExchangeModel(HttpJSONResponseModel):
    Name: str
    Code: str
    OperatingMIC: str | None
    Country: str
    Currency: str
    CountryISO2: str
    CountryISO3: str


class ExchangeListModel(RootModel):
    root: List[ExchangeModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> ExchangeModel:
        return self.root[item]


class HistoricalRatesModel(HttpJSONResponseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    adjusted_close: float
    volume: int | None


class HistoricalRatesListModel(RootModel):
    root: List[HistoricalRatesModel]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item) -> HistoricalRatesModel:
        return self.root[item]


class EodHistoricalDataHook(BaseHttpHook):

    base_url = "https://eodhd.com/api/"

    def __init__(self, api_token: str) -> None:
        self.base_query_params["api_token"] = api_token

    def exchanges(self):
        """
        Get a list of exchanges.
        """

        return self.make_request("exchanges-list", response_model=ExchangeListModel)

    def url_exchange_listed_securities(self, exchange_code: str) -> str:
        """Resolve URL for exchange listed securities endpoint."""

        endpoint = "exchange-symbol-list/{exchange_code}"
        return self._build_url(endpoint, path_params={"exchange_code": exchange_code}, query_params={"fmt": "json"})

    def exchange_listed_securities(self, exchange_code: str):
        """
        Get a list of securities listed on a specific exchange.

        Args:
            exchange_code (str): Exchange code of the security.
        """

        endpoint = "exchange-symbol-list/{exchange_code}"
        return self.make_request(
            endpoint,
            path_params={"exchange_code": exchange_code},
            response_model=ExchangeSecurityListModel,
            query_params={"fmt": "json"},
        )

    def historical_rates(self, symbol: str, exchange_code: str):
        """
        Get historical rates for a symbol on a specific exchange.

        Args:
            symbol (str): Symbol of the security.
            exchange_code (str): Exchange code of the security.
        """

        endpoint = "eod/{symbol}.{exchange_code}"
        return self.make_request(
            endpoint,
            path_params={"symbol": symbol, "exchange_code": exchange_code},
            query_params={"fmt": "json"},
            response_model=HistoricalRatesListModel,
        )

    def url_historical_rates(self, symbol: str, exchange_code: str) -> str:
        endpoint = "eod/{symbol}.{exchange_code}"
        return self._build_url(
            endpoint,
            path_params={"symbol": symbol, "exchange_code": exchange_code},
            query_params={"fmt": "json"},
        )

    def indexes(self):
        """
        Get a list of indexes.
        """
        return self.exchange_listed_securities("INDX")

    def url_fundamental(self, symbol: str, exchange_code: str) -> str:
        """
        Resolve URL for fundamental data endpoint.
        """
        endpoint = "fundamentals/{symbol}.{exchange_code}"
        return self._build_url(
            endpoint,
            path_params={"symbol": symbol, "exchange_code": exchange_code},
            query_params={"fmt": "json"},
        )

    def url_index_members(self, index_symbol: str) -> str:
        """
        Resolve URL for index members endpoint.
        """
        return self.url_fundamental(symbol=index_symbol, exchange_code="INDX")

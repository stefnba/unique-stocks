from typing import Optional

from shared.config import CONFIG
from shared.hooks.api import ApiHook


class EodHistoricalDataApiClient(ApiHook):
    """
    For more info, see https://eodhistoricaldata.com
    """

    client_key = "EodHistoricalData"
    client_key_short = "eod"

    us_exchanges = [
        "NYSE",
        "NASDAQ",
        "BATS",
        "OTCQB",
        "PINK",
        "OTCQX",
        "OTCMKTS",
        "NMFQS",
        "NYSE MKT",
        "OTCBB",
        "OTCGREY",
        "BATS",
        "OTC",
    ]

    virtual_exchanges = [
        "BOND",
        "CC",
        "FOREX",
        "MONEY",
        "EUFUND",
        "GBOND",
        "EUBOND",
        "MCX",
    ]
    exchanges_drop = ["IL", "VX"]
    index_exhange_code = "INDX"

    _base_url = "https://eodhistoricaldata.com/api"
    _base_params = {"api_token": CONFIG.api_keys.eod_historical_data, "fmt": "json"}

    @classmethod
    def get_exchanges(cls) -> dict:
        """
        Get list of all supported exchanges

        Returns:
            dict: JSON of exchanges
        """

        endpoint = "/exchanges-list"
        api = cls()
        return api.request_json(endpoint)

    @classmethod
    def get_securities_listed_at_exchange(cls, exhange_code: str) -> dict:
        """
        Get list of securities that are listed at this exchange.

        By default, this API provides only tickers that were active at least a month
        ago, to get the list of inactive (delisted) tickers please use the parameter
        “delisted=1”:

        Args:
            exhange_code (str): Identifying code of exhange as specified on
            eodhistoricaldata.com

        Returns:
            dict: JSON of exchanges
        """

        endpoint = "/exchange-symbol-list/"
        api = cls()
        return api.request_json(f"{endpoint}/{exhange_code}")

    @classmethod
    def get_exchange_details(cls, exhange_code: str) -> dict:
        """
        Get details like timezone and holidays of an exchange.

        Args:
            exhange_code (str): Identifying code of exhange as specified on
            eodhistoricaldata.com

        Returns:
            _type_: _description_
        """

        endpoint = "/exchange-details"
        api = cls()
        return api.request_json(f"{endpoint}/{exhange_code}")

    @classmethod
    def get_fundamentals(cls, security_code: str, exchange_code: Optional[str] = None):
        """
        Simple access to fundamental data API for stocks, ETFs, Mutual Funds, and Indices from different exchanges and
        countries. Almost all major US, UK, EU, India, and Asia exchanges.

        Args:
            code (str): Security for which fundamentals are requested.
            exchange_code (Optional[str], optional): Exchange filter. Defaults to None.

        Returns:
            dict: _description_
        """
        api = cls()
        endpoint = "/fundamentals"

        # fundamentals need to have generic US exchange symbol
        if exchange_code in api.us_exchanges:
            exchange_code = "US"

        # some securities need exchange code, other don't
        security = f"{security_code}" if not exchange_code else f"{security_code}.{exchange_code}"

        return api.request_json(f"{endpoint}/{security}")

    @classmethod
    def get_historical_eod_quotes(cls, security_code: str, exchange_code: str):
        """
        Historical stock price data end of day.

        Args:
            security_code (str): _description_
            exchange_code (str): _description_

        Returns:
            _type_: _description_
        """
        api = cls()
        endpoint = "/eod"

        # quotes need to have generic US exchange symbol
        if exchange_code in api.us_exchanges:
            exchange_code = "US"

        return api.request_json(f"{endpoint}/{security_code}.{exchange_code}")

from services.config import config
from services.hooks.api import ApiHook


class EodHistoricalDataApiClient(ApiHook):
    """
    For more info, see https://eodhistoricaldata.com
    """

    client_key = "EodHistoricalData"
    _base_url = "https://eodhistoricaldata.com/api"

    _base_params = {"api_token": config.api_keys.eod_historical_data, "fmt": "json"}

    @classmethod
    def list_exhanges(cls) -> dict:
        """
        Get list of all supported exchanges

        Returns:
            dict: JSON of exchanges
        """

        endpoint = "/exchanges-list"
        api = cls()
        return api.request_json(endpoint)

    @classmethod
    def list_securities_of_exhanges(cls, exhange_code: str) -> dict:
        """_summary_

        By default, this API provides only tickers that were active at least a month
        ago, to get the list of inactive (delisted) tickers please use the parameter
        “delisted=1”:

        Args:
            exhange_code (str): Identifying code of exhange as specified on
            eodhistoricaldata.com

        Returns:
            dict: JSON of exchanges
        """

        endpoint = "exchange-symbol-list/"
        api = cls()
        return api.request_json(f"{endpoint}/{exhange_code}")

    @classmethod
    def get_exchange_details(cls, exhange_code: str) -> dict:
        """

        Args:
            exhange_code (str): Identifying code of exhange as specified on
            eodhistoricaldata.com

        Returns:
            _type_: _description_
        """

        endpoint = "exchange-details"
        api = cls()
        return api.request_json(f"{endpoint}/{exhange_code}")

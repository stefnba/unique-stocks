from shared.config import config
from shared.hooks.api import ApiHook


class MarketStackApiClient(ApiHook):
    """
    For more info, see https://marketstack.com/
    """

    client_key = "MarketStack"
    client_key_short = "msk"

    _base_url = "http://api.marketstack.com/v1"
    _base_params = {"access_key": config.api_keys.market_stack, "limit": 1000}

    @classmethod
    def get_exchanges(cls):
        """
        Get list of exchanges that are available.

        Returns:
            dict: JSON of exchanges
        """

        endpoint = "/exchanges"
        api = cls()
        response = api.request_json(endpoint)
        return response["data"]

from config import config

from ..base import Api


class MarketStackApi(Api):
    """
    For more info, see https://marketstack.com/
    """

    client_key = "MarketStack"
    _base_url = "http://api.marketstack.com/v1/"

    _base_params = {"access_key": config.api_keys.market_stack, "limit": 1000}

    @classmethod
    def list_exhanges(cls):
        """
        List exchanges that are available.

        Returns:
            json: Exchanges
        """

        endpoint = "/exchanges"
        api = cls()
        response = api.request_json(endpoint)
        return response["data"]

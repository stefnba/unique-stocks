from shared.config import config
from shared.hooks.api import ApiHook


class OpenFigiApiClient(ApiHook):
    """
    For more info, see https://www.openfigi.com
    """

    client_key = "OpenFigi"
    client_key_short = "figi"

    _base_url = "https://api.openfigi.com/v3/"
    _base_headers = {"X-OPENFIGI-APIKEY": config.api_keys.open_figi}

    @classmethod
    def get_mappings(cls):
        """
        Map third party identifiers to FIGIs.

        Returns:
            list[dict]: list of mapping objects
        """

        endpoint = "/mapping"
        api = cls()
        response = api.request_json(endpoint, method="POST", json=[{"idType": "ID_WERTPAPIER", "idValue": "851399"}])
        return response

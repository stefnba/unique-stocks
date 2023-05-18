from shared.clients.api.open_figi.types import MappingInput
from shared.config import CONFIG
from shared.hooks.api import ApiHook


class OpenFigiApiClient(ApiHook):
    """
    For more info, see https://www.openfigi.com
    """

    client_key = "OpenFigi"
    client_key_short = "open_figi"

    _base_url = "https://api.openfigi.com/v3/"
    _base_headers = {"X-OPENFIGI-APIKEY": CONFIG.api_keys.open_figi}

    @classmethod
    def get_mapping(cls, mapping_input: MappingInput):
        """
        Map third party identifiers to FIGIs.

        Returns:
            list[dict]: list of mapping objects
        """

        endpoint = "/mapping"
        api = cls()
        response = api.request_json(
            endpoint,
            method="POST",
            json=mapping_input,
        )

        return response

        if len(mapping_input) is not len(response):
            raise Exception("Mapping input and response have different length")

        mapping_output = []
        for index, _mapping in enumerate(mapping_input):
            data = response[index].get("data", None)

            mapping = {}

            if data is None:
                mapping = {**_mapping, "mapping": None}
            else:
                if len(data) > 1:
                    print(_mapping, len(data))
                mapping = {**_mapping, "mapping": data[0]}

            mapping_output.append(mapping)

        return mapping_output

    @classmethod
    def get_figi(cls):
        """
        Search for FIGIs using key words and other filters. The results are listed alphabetically by FIGI
        and include the number of results.


        Returns:
            list[dict]: list of mapping objects
        """

        endpoint = "/search"
        api = cls()
        response = api.request_json(endpoint, method="POST", json={"micCode": "XETR", "marketSecDes": "Equity"})
        return response

    @classmethod
    def get_values(cls):
        """
        Get the current list of values for the enum-like properties on Mapping Jobs.

        """
        endpoint = "/mapping/values/exchCode"
        api = cls()

        return api.request_json(endpoint, method="GET")

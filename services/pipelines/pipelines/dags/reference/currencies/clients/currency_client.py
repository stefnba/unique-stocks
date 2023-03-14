from services.hooks.api import ApiHook


class CurrencyApiClient(ApiHook):
    _base_url = "https://raw.githubusercontent.com/vijinho/ISO-Country-Data/master/"

    @classmethod
    def get_currencies(cls) -> bytes:
        endpoint = "currencies.csv"
        api = cls()
        return api._download_file_to_bytes(endpoint).content

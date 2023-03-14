from services.hooks.api import ApiHook


class CountryApiClient(ApiHook):
    _base_url = "https://raw.githubusercontent.com/stefangabos/world_countries/master/data/countries/en"

    @classmethod
    def get_countries(cls):
        endpoint = "world.csv"
        api = cls()
        return api._download_file_to_bytes(endpoint).content

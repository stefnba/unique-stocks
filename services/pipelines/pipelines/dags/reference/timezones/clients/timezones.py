from services.hooks.api import ApiHook


class TimezoneApiClient(ApiHook):
    _base_url = "https://raw.githubusercontent.com/bproctor/timezones/master/"

    @classmethod
    def get_timezones(cls) -> bytes:
        endpoint = "timezones.csv"
        api = cls()
        return api._download_file_to_bytes(endpoint).content

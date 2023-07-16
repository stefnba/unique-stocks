name = "GlobalLegalEntityIdentifierFoundation"
from shared.hooks.api import ApiHook


class GleifApiClient(ApiHook):
    """
    This International Standard specifies a universal method of identifying exchanges,
    trading platforms, regulated or non-regulated markets and trade reporting
    facilities as sources of prices and related information in order to facilitate
    automated processing. For more info,
    see https://www.iso20022.org/market-identifier-codes
    """

    client_key = "GlobalLegalEntityIdentifierFoundation"
    client_key_short = "gleif"
    _base_url = "https://api.gleif.org/api/v1/"

    @classmethod
    def get_level1_info(cls):
        """
        Get list of all registered exchanges under ISO.

        Returns:
            RequestFileBytes: _description_
        """
        api = cls()
        endpoint = "lei-records?page[size]=2&page[number]=100"
        return api.request_json(endpoint)

    @classmethod
    def get_entity_isin_mapping(cls, file_path: str):
        """
        Get list of all registered exchanges under ISO.

        Returns:
            RequestFileBytes: _description_
        """
        api = cls()
        api._base_url = ""
        endpoint = "https://mapping.gleif.org/api/v2/isin-lei/d6996d23-cdaf-413e-b594-5219d40f3da5/download"

        downloaded = api._download_file_to_disk(endpoint, file_destination=file_path)

        return downloaded.path

    @classmethod
    def get_entity_list(cls, file_path: str):
        """
        Get list of all registered exchanges under ISO.

        Returns:
            RequestFileBytes: _description_
        """
        api = cls()
        api._base_url = ""
        endpoint = "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/05/29/789258/20230529-0800-gleif-goldencopy-lei2-golden-copy.csv.zip"

        downloaded = api._download_file_to_disk(endpoint, file_destination=file_path)

        return downloaded.path

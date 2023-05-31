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

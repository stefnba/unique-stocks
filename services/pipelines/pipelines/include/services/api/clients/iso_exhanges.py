from include.services.api import Api


class IsoExchangesApi(Api):
    """
    This International Standard specifies a universal method of identifying exchanges,
    trading platforms, regulated or non-regulated markets and trade reporting
    facilities as sources of prices and related information in order to facilitate
    automated processing. For more info,
    see https://www.iso20022.org/market-identifier-codes
    """

    client_key = "IsoExchanges"
    _base_url = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/"

    @classmethod
    def download_exhange_list(cls):
        api = cls()
        endpoint = "ISO10383_MIC.csv"
        return api._download_file_to_bytes(endpoint)

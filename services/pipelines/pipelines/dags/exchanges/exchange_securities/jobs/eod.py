import io

import polars as pl
from dags.exchanges.exchange_securities.jobs.config import ExchangeSecuritiesPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodExchangeSecurityJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from processed exchange list layer.
        """
        file_content = datalake_client.download_file_into_memory(file_path)

        exchanges = pl.read_parquet(file_content)

        exchange_codes = exchanges["source_code"].to_list()

        return exchange_codes

    @staticmethod
    def download_securities(exchange_code: str):
        """
        Retrieves listed securities for a given exchange code.
        """

        # api data
        exhange_details = ApiClient.get_securities_listed_at_exhange(exhange_code=exchange_code)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangeSecuritiesPath(
                zone="raw", exchange=exchange_code, asset_source=ASSET_SOURCE, file_type="json"
            ),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process_securities(exchange_code: str, file_path: str):
        """
        Retrieves listed securities for a given exchange code.
        """

        file_content = datalake_client.download_file_into_memory(file_path)

        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangeSecuritiesPath(
                zone="processed", exchange=exchange_code, asset_source=ASSET_SOURCE
            ),
            file=file_content,
        )
        return uploaded_file.file.full_path

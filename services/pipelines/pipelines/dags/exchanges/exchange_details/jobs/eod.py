import io

import polars as pl
from dags.exchanges.exchange_details.jobs.config import ExchangeDetailsPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodExchangeJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from processed exchange list layer.
        """
        # download file
        file_content = datalake_client.download_file_into_memory(file_path)

        exchanges = pl.read_parquet(io.BytesIO(file_content))

        exchange_codes = exchanges["source_code"].to_list()

        return exchange_codes

    @staticmethod
    def prepare_download_of_exchange_details(exchange_codes: list[str]):
        """
        Triggers download of details for each code.
        """

        exchange_details_paths = []
        for exchange_code in exchange_codes:
            exchange_details_paths.append(EodExchangeJobs.download_exchange_details(exchange_code))

        return exchange_details_paths

    @staticmethod
    def download_exchange_details(exchange_code: str):
        """
        Retrieves details for a given exchange code.
        """

        # api data
        exhange_details = ApiClient.get_exchange_details(exhange_code=exchange_code)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangeDetailsPath(zone="raw", asset_source=ASSET_SOURCE, exchange=exchange_code),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process_raw_exchange_details(file_path: str):
        pass

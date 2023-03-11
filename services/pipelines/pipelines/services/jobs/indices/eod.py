from services.clients.api.eod.client import EodHistoricalDataApiClient
from services.jobs.exchanges.eod import EodExchangeJobs
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config
from services.jobs.indices.datalake_path import IndexListPath

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodIndexJobs:
    @staticmethod
    def download_index_list():
        index_exchange = "INDX"
        file_path = EodExchangeJobs.download_exchange_securities(index_exchange)

        return file_path

    @staticmethod
    def process_index_list(file_path: str):
        # donwload file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=IndexListPath(stage="raw", asset_source=ASSET_SOURCE, file_type="parquet"),
            file_system=config.azure.file_system,
            local_file="",
        )

        # return uploaded_file.file_path

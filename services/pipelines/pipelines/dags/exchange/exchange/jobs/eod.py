import io

import polars as pl
import requests
from dags.exchange.exchange.jobs.utils import ExchangePath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodExchangeJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchange codes from processed exchange list layer.
        """
        # download file
        file_content = dl_client.download_file_into_memory(file_path)

        exchange = pl.read_parquet(io.BytesIO(file_content))

        exchange_codes = exchange["source_code"].to_list()

        return exchange_codes

    @staticmethod
    def prepare_download_of_exchange_securities(exchange_codes: list[str]):
        """
        Triggers download of securities listed at each exchange.
        """

        failed = []
        exchange_securities_paths = []

        for exchange_code in exchange_codes:
            try:
                file_path = EodExchangeJobs.download_exchange_securities(exchange_code)
                exchange_securities_paths.append(file_path)
            except requests.exceptions.HTTPError:
                failed.append(exchange_code)

        print(failed)

        return exchange_securities_paths

    @staticmethod
    def download_exchange_securities(exchange_code: str):
        """
        Retrieves listed securities for a given exchange code.
        """

        # api data
        exhange_details = ApiClient.get_securities_listed_at_exhange(exhange_code=exchange_code)

        # upload to data_lake
        uploaded_file = dl_client.upload_file(
            destination_file_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def download_exchange_list() -> str:
        """
        Retrieves list of exchange from eodhistoricaldata.com and uploads
        into the Data Lake.

        Returns:
            str: Path to file in data lake
        """

        # api data
        exchanges_json = ApiClient.get_exchanges()

        # upload to data_lake
        uploaded_file = dl_client.upload_file(
            destination_file_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(exchanges_json),
        )

        return uploaded_file.file.full_path

    @staticmethod
    def process_raw_exchange_list(file_path: str) -> str:
        """
        File content is in JSON format.
        """

        exchange = duck.get_data(file_path, handler="azure_abfs", format="json").pl()
        exchange_code_mappings = DbQueryRepositories.mappings.get_mappings(
            product="exchange", source="EodHistoricalData", field="exchange_code"
        )
        virtual_exchange_mappings = DbQueryRepositories.mappings.get_mappings(
            product="exchange", source="EodHistoricalData", field="is_virtual"
        )

        exchange = exchange.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )
        exchange = exchange.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        transformed = duck.query(
            "./sql/transform_raw_eod.sql",
            exchange=exchange,
            exchange_code_mappings=exchange_code_mappings,
            virtual_exchange_mappings=virtual_exchange_mappings,
            source=ASSET_SOURCE,
        ).df()

        return dl_client.upload_file(
            destination_file_path=ExchangePath.temp(),
            file=transformed.to_parquet(),
        ).file.full_path

    @staticmethod
    def join_eod_details(exchanges_path: str, details_path: str) -> str:
        """
        Joins EodHistoricalData exchange and details for all exchange.
        """

        joined = duck.query(
            "./sql/join_eod_details.sql",
            exchange=duck.get_data(exchanges_path, handler="azure_abfs"),
            details=duck.get_data(details_path, handler="azure_abfs"),
        )

        return dl_client.upload_file(
            destination_file_path=ExchangePath.processed(source=ASSET_SOURCE),
            file=joined.df().to_parquet(),
        ).file.full_path
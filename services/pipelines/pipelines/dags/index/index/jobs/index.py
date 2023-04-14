import polars as pl
from dags.index.index.jobs.utils import IndexPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key
INDEX_EXCHANGE_CODE = ApiClient.index_exhange_code


class IndexJobs:
    """
    Only EOD as data source
    """

    @staticmethod
    def download() -> str:
        """
        Downloads list with all indicies available at EOD.


        Returns:
            str: File path of downloaded file.
        """
        index = ApiClient.get_securities_listed_at_exhange(INDEX_EXCHANGE_CODE)

        return dl_client.upload_file(
            destination_file_path=IndexPath.raw(source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(index),
        ).file.full_path

    @staticmethod
    def transform(file_path: str) -> str:
        """
        Processed list of index downloaded from EOD.

        Returns:
            str: File path processed .parquet file.
        """

        index = duck.get_data(file_path, handler="azure_abfs", format="json").pl()

        index = index.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )

        transformed = duck.query("./sql/transform_raw.sql", index=index).df()

        return dl_client.upload_file(
            destination_file_path=IndexPath.processed(source=ASSET_SOURCE),
            file=transformed.to_parquet(),
        ).file.full_path

    @staticmethod
    def curate(file_path: str):
        """
        Finalize processing and save file in curated data_lake zone.
        """

        index = duck.get_data(file_path, handler="azure_abfs")

        dl_client.upload_file(
            destination_file_path=IndexPath.curated(version="history"),
            file=index.df().to_parquet(),
        )

        return dl_client.upload_file(
            destination_file_path=IndexPath.curated(),
            file=index.df().to_parquet(),
        ).file.full_path

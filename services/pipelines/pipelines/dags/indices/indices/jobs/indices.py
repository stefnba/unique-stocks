import polars as pl
from dags.indices.indices.jobs.utils import IndexPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
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
        indices = ApiClient.get_securities_listed_at_exhange(INDEX_EXCHANGE_CODE)

        return datalake_client.upload_file(
            destination_file_path=IndexPath.raw(source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(indices),
        ).file.full_path

    @staticmethod
    def transform(file_path: str) -> str:
        """
        Processed list of indices downloaded from EOD.

        Returns:
            str: File path processed .parquet file.
        """

        indices = duck.get_data(file_path, handler="azure_abfs", format="json").pl()

        indices = indices.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )

        transformed = duck.query("./sql/transform_raw.sql", indices=indices).df()

        return datalake_client.upload_file(
            destination_file_path=IndexPath.processed(source=ASSET_SOURCE),
            file=transformed.to_parquet(),
        ).file.full_path

    @staticmethod
    def curate(file_path: str):
        """
        Finalize processing and save file in curated datalake zone.
        """

        indices = duck.get_data(file_path, handler="azure_abfs")

        datalake_client.upload_file(
            destination_file_path=IndexPath.curated(version="history"),
            file=indices.df().to_parquet(),
        )

        return datalake_client.upload_file(
            destination_file_path=IndexPath.curated(),
            file=indices.df().to_parquet(),
        ).file.full_path

import io

import duckdb
import polars as pl
from dags.indices.indices.jobs.datalake_path import IndicesPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.datalake.azure.file_system import abfs_client, build_abfs_path
from shared.config import config
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key

# virtual exchange code of EOD for indices
VIRTUAL_EXCHANGE_CODE = "INDX"


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
        indices = ApiClient.get_securities_listed_at_exhange(VIRTUAL_EXCHANGE_CODE)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=IndicesPath(stage="raw", asset_source=ASSET_SOURCE, file_type="json"),
            file_system=config.azure.file_system,
            local_file=converter.json_to_bytes(indices),
        )

        return uploaded_file.file_path

    @staticmethod
    def process(file_path: str) -> str:
        """
        Processed list of indices downloaded from EOD.

        Returns:
            str: File path processed .parquet file.
        """

        # download file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df = pl.read_json(io.BytesIO(file_content))

        df = df.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )

        df_upload = duckdb.sql(
            """
                --sql
                SELECT
                    Code AS code,
                    Name as name,
                    Currency AS currency,
                    Type AS type,
                    Isin AS isin,
                    data_source
                FROM
                    df;
            """
        ).df()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=IndicesPath(stage="processed", asset_source=ASSET_SOURCE),
            file_system=config.azure.file_system,
            local_file=df_upload.to_parquet(),
        )

        return uploaded_file.file_path

    @staticmethod
    def curate(file_path: str):
        """
        Finalize processing and save file in curated datalake zone.
        """

        db = duckdb.connect()
        db.register_filesystem(abfs_client)

        data = db.read_parquet(build_abfs_path(file_path))

        # datalake destination
        datalake_client.upload_file(
            remote_file="/curated/product=indices/indices.parquet",
            file_system=config.azure.file_system,
            local_file=data.df().to_parquet(),
        )

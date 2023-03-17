import io

import duckdb
import polars as pl
from dags.indices.jobs.datalake_path import IndexMembersPath, IndicesPath
from services.clients.api.eod.client import EodHistoricalDataApiClient
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config
from services.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key

# virtual exchange code of EOD for indices
VIRTUAL_EXCHANGE_CODE = "INDX"


class EodIndexJobs:
    @staticmethod
    def extract_index_codes(file_path: str):
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df = pl.read_parquet(io.BytesIO(file_content))
        return list(df["code"].unique())[:50]

    @staticmethod
    def download_index_list():
        indices = ApiClient.get_securities_listed_at_exhange(VIRTUAL_EXCHANGE_CODE)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=IndicesPath(stage="raw", asset_source=ASSET_SOURCE, file_type="json"),
            file_system=config.azure.file_system,
            local_file=converter.json_to_bytes(indices),
        )

        return uploaded_file.file_path

    @staticmethod
    def process_index_list(file_path: str):
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

        df = duckdb.sql(
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
            local_file=df.to_parquet(),
        )

        return uploaded_file.file_path

    @staticmethod
    def download_members_of_index(index_code: str):
        members = ApiClient.get_fundamentals(security_code=index_code, exchange_code=VIRTUAL_EXCHANGE_CODE)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=IndexMembersPath(stage="raw", asset_source=ASSET_SOURCE, file_type="json", index=index_code),
            file_system=config.azure.file_system,
            local_file=converter.json_to_bytes(members),
        )

        return uploaded_file.file_path

    @staticmethod
    def process_members_of_index(file_path: str):
        import json

        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )
        data_dict = json.loads(file_content)

        members = list(data_dict["Components"].values())
        meta_info = data_dict["General"]
        index_code = meta_info.get("Code", None)

        #
        if len(members) == 0:
            # todo warning
            raise Exception(f"{index_code} has no members")

        df = pl.from_dicts(members)

        df = df.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
                pl.lit(meta_info.get("CountryISO", None)).alias("country"),
                pl.lit(meta_info.get("Name", None)).alias("index_name"),
                pl.lit(meta_info.get("Code", None)).alias("index_code"),
            ]
        )

        df = duckdb.sql(
            """
        --sql
        SELECT
            Code AS code,
            Name as name,
            Exchange AS exchange,
            country,
            index_name,
            index_code,
            data_source
        FROM
            df;
        """
        ).df()

        print(df)

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=IndexMembersPath(stage="processed", asset_source=ASSET_SOURCE, index=index_code),
            file_system=config.azure.file_system,
            local_file=df.to_parquet(),
        )

        return uploaded_file.file_name

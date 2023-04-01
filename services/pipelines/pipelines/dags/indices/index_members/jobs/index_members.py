import io
import json

import duckdb
import polars as pl
from dags.indices.index_members.jobs.config import IndexMembersPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.duck.client import duck
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key

# virtual exchange code of EOD for indices
VIRTUAL_EXCHANGE_CODE = "INDX"


class IndexMembersJobs:
    @staticmethod
    def extract_index_codes(file_path: str):
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        df = pl.read_parquet(io.BytesIO(file_content))
        return list(df["code"].unique())

    @staticmethod
    def download(index_code: str):
        members = ApiClient.get_fundamentals(security_code=index_code, exchange_code=VIRTUAL_EXCHANGE_CODE)

        return datalake_client.upload_file(
            destination_file_path=IndexMembersPath(
                zone="raw", asset_source=ASSET_SOURCE, file_type="json", index=index_code
            ),
            file=converter.json_to_bytes(members),
        ).file.full_path

    @staticmethod
    def transform(file_path: str):
        import logging

        file_content = datalake_client.download_file_into_memory(file_path=file_path)
        data_dict = json.loads(file_content)

        members = list(data_dict["Components"].values())
        meta_info = data_dict["General"]
        index_code = meta_info.get("Code", None)

        # index has no members
        if len(members) == 0:
            logging.warning(f"{index_code} has no members")

            return

        members = pl.from_dicts(members)

        members = members.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )

        transformed = duck.query("./sql/transform_raw.sql", members=members, source=ASSET_SOURCE, index_code=index_code)

        return datalake_client.upload_file(
            destination_file_path=IndexMembersPath(zone="processed", asset_source=ASSET_SOURCE, index=index_code),
            file=transformed.df().to_parquet(),
        ).file.full_path

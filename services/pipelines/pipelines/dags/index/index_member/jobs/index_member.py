import io
import json

import polars as pl
from dags.index.index_member.jobs.utils import IndexMemberPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key
INDEX_EXCHANGE_CODE = ApiClient.index_exhange_code


class IndexMembersJobs:
    @staticmethod
    def extract_index_codes(file_path: str):
        file_content = dl_client.download_file_into_memory(file_path=file_path)

        df = pl.read_parquet(io.BytesIO(file_content))
        return list(df["code"].unique())

    @staticmethod
    def download(index_code: str):
        members = ApiClient.get_fundamentals(security_code=index_code, exchange_code=INDEX_EXCHANGE_CODE)

        return dl_client.upload_file(
            destination_file_path=IndexMemberPath.raw(source=ASSET_SOURCE, file_type="json", bin=index_code),
            file=converter.json_to_bytes(members),
        ).file.full_path

    @staticmethod
    def transform(file_path: str):
        import logging

        file_content = dl_client.download_file_into_memory(file_path=file_path)
        data_dict = json.loads(file_content)

        members = list(data_dict["Components"].values())
        meta_info = data_dict["General"]
        index_code = meta_info.get("Code", None)

        # index has no members
        if len(members) == 0:
            logging.warning(f"{index_code} has no members")

            return

        members_df = pl.from_dicts(members)

        members_df = members_df.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )

        transformed = duck.query(
            "./sql/transform_raw.sql", members=members_df, source=ASSET_SOURCE, index_code=index_code
        )

        return dl_client.upload_file(
            destination_file_path=IndexMemberPath.processed(source=ASSET_SOURCE, bin=index_code),
            file=transformed.df().to_parquet(),
        ).file.full_path

import json

import polars as pl
from dags.exchanges.exchange_details.jobs.config import (
    ExchangeDetailsPath,
    ExchangeDetailsPathCuratedCurrent,
    ExchangeHolidaysPath,
    ExchangeHolidaysPathCuratedCurrent,
)
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.paths.path import TempPath
from shared.utils.conversion import converter
from shared.utils.path.datalake.builder import DatalakePathBuilder
from shared.utils.utils.list import flatten_list

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class ExchangeDetailsJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from processed exchanges.
        """
        return [
            "US",
            "LSE",
        ]
        #  "XETRA", "STU", "PA", "SW"]
        # download file
        file_content = datalake_client.download_file_into_memory(file_path)

        exchanges = pl.read_parquet(file_content)

        exchange_codes = exchanges["source_code"].to_list()

        return exchange_codes

    @staticmethod
    def download_details(exchange_code: str):
        """
        Retrieves details from api for a given exchange code and dumps it to datalake raw.
        """

        # api data
        exhange_details = ApiClient.get_exchange_details(exhange_code=exchange_code)

        # upload to datalake
        return datalake_client.upload_file(
            destination_file_path=ExchangeDetailsPath(
                zone="raw", asset_source=ASSET_SOURCE, exchange=exchange_code, file_type="json"
            ),
            file=converter.json_to_bytes(exhange_details),
        ).file.full_path

    @staticmethod
    def curate_merged_details(file_path: str):
        """
        Tranformations for curated zone.
        - map surrogate key
        """

        data = duck.get_data(file_path, handler="azure_abfs")

        # todo delete temp file

        datalake_client.upload_file(
            destination_file_path=ExchangeDetailsPathCuratedCurrent(),
            file=data.df().to_parquet(),
        )

    @staticmethod
    def curate_merged_holidays(file_path: str):
        """
        Tranformations for curated zone.

        - map surrogate key
        """

        data = duck.get_data(file_path, handler="azure_abfs")

        datalake_client.upload_file(
            destination_file_path=ExchangeHolidaysPathCuratedCurrent(),
            file=data.df().to_parquet(),
        )

        # todo delete temp file

    @staticmethod
    def merge(file_paths: list[str | list[str]]):
        """
        Takes various file_paths (can also be nested list of file_paths) and merges them into one file.
        """
        file_paths_flattened: list[str] = flatten_list(file_paths)

        holidays = duck.db.read_parquet(
            [DatalakePathBuilder.build_abfs_path(file_path) for file_path in file_paths_flattened]
        ).pl()

        print(holidays)

        uploaded_file = datalake_client.upload_file(
            destination_file_path=TempPath(file_type="parquet"),
            file=holidays.to_pandas().to_parquet(),
        )

        return uploaded_file.file.full_path

    @staticmethod
    def transform_holidays(file_path: str):
        """
        Transforms holidays of exchange details from EOD
        - unnesting
        - maps exchange uid

        Save each exchange uid to datalake processed zone.


        """
        details_raw_data = json.loads(datalake_client.download_file_into_memory(file_path))

        holidays = details_raw_data.get("ExchangeHolidays", None)
        exchange_code = details_raw_data.get("Code", None)

        print(exchange_code)

        holidays_df = pl.DataFrame(list(holidays.values()))
        holidays_df = holidays_df.with_columns(
            [
                pl.col("Date").str.strptime(pl.Date).cast(pl.Date),
                pl.lit(exchange_code).alias("exchange_source_code"),
            ]
        )

        data = duck.query(
            "./sql/transform_raw_holidays.sql",
            holidays_data=holidays_df,
            mappings_data=DbQueryRepositories.mappings.get_mappings(source="EodHistoricalData"),
        ).pl()

        uploaded_file_paths = []
        for exchange_uid in list(data["exchange_uid"].unique()):
            # upload to datalake
            uploaded_file = datalake_client.upload_file(
                destination_file_path=ExchangeHolidaysPath(
                    zone="processed", asset_source=ASSET_SOURCE, exchange=exchange_uid
                ),
                file=data.filter(pl.col("exchange_uid") == exchange_uid).to_pandas().to_parquet(),
            )

            uploaded_file_paths.append(uploaded_file.file.full_path)

        return uploaded_file_paths

    @staticmethod
    def transform_details(file_path: str):
        """
        Transforms raw exchange details
        - maps exchange uid
        - maps country

        Save each exchange uid to datalake processed zone.
        """

        data = duck.query(
            "./sql/transform_raw_details.sql",
            details_data=duck.get_data(file_path, handler="azure_abfs", format="json"),
            mappings_data=DbQueryRepositories.mappings.get_mappings(source="EodHistoricalData"),
        ).pl()

        uploaded_file_paths = []
        for exchange_uid in list(data["exchange_uid"].unique()):
            # upload to datalake
            uploaded_file = datalake_client.upload_file(
                destination_file_path=ExchangeDetailsPath(
                    zone="processed", asset_source=ASSET_SOURCE, exchange=exchange_uid
                ),
                file=data.filter(pl.col("exchange_uid") == exchange_uid).to_pandas().to_parquet(),
            )

            uploaded_file_paths.append(uploaded_file.file.full_path)

        return uploaded_file_paths

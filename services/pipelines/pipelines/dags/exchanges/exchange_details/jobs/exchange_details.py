import json

import duckdb
import pandas as pd
import polars as pl
from dags.exchanges.exchange_details.jobs.config import ExchangeDetailsPath, ExchangeHolidaysPath, TempPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.datalake.azure.file_system import abfs_client
from shared.clients.db.postgres.client import db_client
from shared.clients.db.postgres.repositories import DbRepositories
from shared.utils.conversion import converter
from shared.utils.path.datalake.builder import DatalakePathBuilder

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


def flatten_list(input_list):
    if not isinstance(input_list, list):  # if not list
        return [input_list]
    return [x for sub in input_list for x in flatten_list(sub)]  # recurse and collect


class ExchangeDetailsJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from processed exchanges.
        """
        return ["US", "LSE"]
        # download file
        file_content = datalake_client.download_file_into_memory(file_path)

        exchanges = pl.read_parquet(file_content)

        exchange_codes = exchanges["source_code"].to_list()

        return exchange_codes

    @staticmethod
    def download_details(exchange_code: str):
        """
        Retrieves details for a given exchange code.
        """

        # api data
        exhange_details = ApiClient.get_exchange_details(exhange_code=exchange_code)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangeDetailsPath(
                zone="raw", asset_source=ASSET_SOURCE, exchange=exchange_code, file_type="json"
            ),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def curate_merged_details(file_path: str):
        """
        Tranformations for curated zone.
        - abc
        - abc
        """

    @staticmethod
    def curate_merged_holidays(file_path: str):
        """
        Tranformations for curated zone.

        - abc
        - abc
        """
        db = duckdb.connect()
        db.register_filesystem(abfs_client)

        abfs_file_path = DatalakePathBuilder.build_abfs_path(file_path)

        mappings_df = DbRepositories.mappings.get_mappings(product="exchange", source=ASSET_SOURCE)
        holidays_df = db.read_parquet(abfs_file_path).pl()

        print(mappings_df)
        print(holidays_df)

        merged = db.sql(
            """
            --sql
            SELECT holidays_df.*, mappings_df.uid
            FROM holidays_df
            LEFT JOIN mappings_df
            ON holidays_df.exchange_code = mappings_df.source_value
            ;
            """
        )
        print(merged.pl())

        # todo delete temp file

    @staticmethod
    def merge(file_paths: list[str | list[str]]):
        db = duckdb.connect()
        db.register_filesystem(abfs_client)

        file_paths_flattened: list[str] = flatten_list(file_paths)

        abfs_file_paths = [DatalakePathBuilder.build_abfs_path(file_path) for file_path in file_paths_flattened]

        holidays = db.read_parquet(abfs_file_paths).pl()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=TempPath(file_type="parquet"),
            file=holidays.to_pandas().to_parquet(),
        )

        return uploaded_file.file.full_path

    @staticmethod
    def process_holidays(details_raw: dict):
        processed_file_paths: list[str] = []

        exchanges = details_raw.get("OperatingMIC", "").split(",")
        ex = details_raw.get("Code", "")

        for exchange_code in exchanges:
            exchange_code = exchange_code.strip()
            holidays = details_raw.get("ExchangeHolidays", None)

            if not holidays:
                continue

            holidays_df = pl.DataFrame(list(holidays.values()))
            holidays_df = holidays_df.with_columns(
                [
                    pl.col("Date").str.strptime(pl.Date).cast(pl.Date),
                    pl.lit(exchange_code).alias("mic"),
                    pl.lit(ex).alias("exchange_code"),
                ]
            )

            # upload to datalake
            uploaded_file = datalake_client.upload_file(
                destination_file_path=ExchangeHolidaysPath(
                    zone="processed", asset_source=ASSET_SOURCE, exchange=exchange_code
                ),
                file=holidays_df.to_pandas().to_parquet(),
            )

            processed_file_paths.append(uploaded_file.file.full_path)

        return processed_file_paths

    @staticmethod
    def process_details(details_raw):
        # eod api has two exchange for code US (XNAS, XNYS)
        exchanges = details_raw.get("OperatingMIC", "").split(",")

        processed_file_paths: list[str] = []

        for exchange_code in exchanges:
            exchange_code = exchange_code.strip()
            processed = {
                "name": details_raw.get("Name", None),
                "code": details_raw.get("Code", None),
                "mic": exchange_code.strip(""),
                "country": details_raw.get("Country", None),
                "currency": details_raw.get("Currency", None),
                "timezone": details_raw.get("Timezone", None),
                "trading_hours": {
                    "open": details_raw.get("TradingHours", None).get("Open", None),
                    "close": details_raw.get("TradingHours", None).get("Close", None),
                    "open_utc": details_raw.get("TradingHours", None).get("OpenUTC", None),
                    "close_utc": details_raw.get("TradingHours", None).get("CloseUTC", None),
                },
                "working_days": details_raw.get("TradingHours", None).get("WorkingDays", []).split(","),
                "active_tickers": details_raw.get("ActiveTickers", None),
                "previous_day_updated_tickers": details_raw.get("PreviousDayUpdatedTickers", None),
                "updated_tickers": details_raw.get("UpdatedTickers", None),
            }

            # upload to datalake
            uploaded_file = datalake_client.upload_file(
                destination_file_path=ExchangeDetailsPath(
                    zone="processed", asset_source=ASSET_SOURCE, exchange=exchange_code
                ),
                file=pd.DataFrame([processed]).to_parquet(),
            )

            processed_file_paths.append(uploaded_file.file.full_path)

        return processed_file_paths

    @staticmethod
    def extract_raw_details(file_path: str) -> dict:
        details_raw: dict = json.loads(datalake_client.download_file_into_memory(file_path=file_path))

        return details_raw

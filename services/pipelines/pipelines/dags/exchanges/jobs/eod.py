import io

import duckdb
import polars as pl
import requests
from dags.exchanges.jobs.config import ExchangeDetailsPath, ExchangesPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodExchangeJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from processed exchange list layer.
        """
        # download file
        file_content = datalake_client.download_file_into_memory(file_path)

        exchanges = pl.read_parquet(io.BytesIO(file_content))

        exchange_codes = exchanges["source_code"].to_list()

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

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangesPath(zone="raw", asset_source=ASSET_SOURCE),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def prepare_download_of_exchange_details(exchange_codes: list[str]):
        """
        Triggers download of details for each code.
        """

        exchange_details_paths = []
        for exchange_code in exchange_codes:
            exchange_details_paths.append(EodExchangeJobs.download_exchange_details(exchange_code))

        return exchange_details_paths

    @staticmethod
    def download_exchange_details(exchange_code: str):
        """
        Retrieves details for a given exchange code.
        """

        # api data
        exhange_details = ApiClient.get_exchange_details(exhange_code=exchange_code)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangeDetailsPath(zone="raw", asset_source=ASSET_SOURCE, exchange=exchange_code),
            file=converter.json_to_bytes(exhange_details),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process_raw_exchange_details(file_path: str):
        pass

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

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangesPath(zone="raw", asset_source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(exchanges_json),
        )

        return uploaded_file.file.full_path

    @staticmethod
    def process_raw_exchange_list(file_path: str) -> str:
        """
        File content is in JSON format.

        Args:
            file_path (str): _description_
        """

        # donwload file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        df_exchanges = pl.read_json(io.BytesIO(file_content))

        df_exchanges = df_exchanges.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )
        df_exchanges = df_exchanges.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )
        base_table = duckdb.execute(
            """
        --sql
            SELECT
                    string_split(OperatingMIC, ',') AS mic,
                    Name AS name,
                    NULL AS acronym,
                    Currency AS currency,
                    NULL AS city,
                    CountryISO2 as country,
                    NULL AS website,
                    NULL AS timezone,
                    data_source,
                    Code AS source_code,
                    list_contains($1, source_code) as is_virtual,
                FROM
                    df_exchanges
                WHERE
                    NOT(list_contains($2, source_code));
        """,
            [ApiClient.virtual_exchanges, ApiClient.exchanges_drop],
        ).pl()

        processed = duckdb.sql(
            """
            --sql
            SELECT
                COALESCE(trim(mic[1]), source_code) app_id,
                trim(mic[1]) mic,
                * EXCLUDE (mic)
            FROM
                base_table
            UNION
            SELECT
                COALESCE(trim(mic[2]), source_code) app_id,
                trim(mic[2]) mic,
                * EXCLUDE (mic)
            FROM
                base_table
            WHERE
                source_code == 'US';
        """
        )

        parquet_file = processed.df().to_parquet()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="processed"),
            file=parquet_file,
        )

        return uploaded_file.file.full_path

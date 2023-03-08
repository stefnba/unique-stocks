import io

import duckdb
import polars as pl
from services.clients.api.eod.client import EodHistoricalDataApiClient
from services.clients.data_lake.azure_data_lake import datalake_client
from services.config import config
from services.utils import formats

from .remote_locations import ExchangeDetailLocation, ExchangeListLocation

API_CLIENT = EodHistoricalDataApiClient
ASSET_SOURCE = API_CLIENT.client_key


class EodExchangeJobs:
    @staticmethod
    def download_details_for_exchanges(exchange_list: list[str]):
        """
        Takes a list of exchanges, iterates over them to call exchange details
        """
        exchange_detail_paths = []
        for exchange in exchange_list:
            exchange_detail_paths.append(
                EodExchangeJobs.download_exchange_details(exchange)
            )

        return exchange_detail_paths

    @staticmethod
    def download_exchange_details(exchange_code: str):
        """
        Retrieves details for a given exchange
        """

        # api data
        exhange_details = API_CLIENT.get_exchange_details(exhange_code=exchange_code)

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangeDetailLocation.raw(
                asset_source=ASSET_SOURCE, exchange=exchange_code
            ),
            file_system=config.azure.file_system,
            local_file=formats.convert_json_to_bytes(exhange_details),
        )
        return uploaded_file.file_path

    @staticmethod
    def download_exchange_list():
        """
        Retrieves list of exchange from eodhistoricaldata.com and uploads
        into the Data Lake.
        """

        # api data
        exchanges_json = API_CLIENT.list_exhanges()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangeListLocation.raw(
                asset_source=ASSET_SOURCE, file_extension="json"
            ),
            file_system=config.azure.file_system,
            local_file=formats.convert_json_to_bytes(exchanges_json),
        )

        return uploaded_file.file_path

    @staticmethod
    def process_raw_exchange_list(file_path: str):
        """
        File content is in JSON format.

        Args:
            file_path (str): _description_
        """

        # donwload file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df_exchanges = pl.read_json(io.BytesIO(file_content))

        df_exchanges = df_exchanges.with_columns(
            [
                pl.when(pl.col(pl.Utf8) == "Unknown")
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name(),
                pl.lit(ASSET_SOURCE).alias("data_source"),
            ]
        )
        df_exchanges = df_exchanges.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name(),
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
            [API_CLIENT.virtual_exchanges, API_CLIENT.exchanges_drop],
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
            remote_file=ExchangeListLocation.processed(asset_source=ASSET_SOURCE),
            file_system=config.azure.file_system,
            local_file=parquet_file,
        )

        return uploaded_file.file_path

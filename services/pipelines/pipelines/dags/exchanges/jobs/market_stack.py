import io

import duckdb
import pandas as pd
import polars as pl
from dags.exchanges.jobs.datalake_path import ExchangesPath
from shared.clients.api.market_stack.market_stack import MarketStackApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.config import config
from shared.utils.conversion import converter

ApiClient = MarketStackApiClient
ASSET_SOURCE = ApiClient.client_key


class MarketStackExchangeJobs:
    @staticmethod
    def download_exchanges():
        """
        Retrieves list of exchange from eodhistoricaldata.com and uploads
        into the Data Lake.
        """
        # config

        # api data
        exchanges_json = ApiClient.get_exchanges()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangesPath(asset_source=ASSET_SOURCE, file_type="json", stage="raw"),
            file_system=config.azure.file_system,
            local_file=converter.json_to_bytes(exchanges_json),
        )

        return uploaded_file.file_path

    @staticmethod
    def process_raw_exchanges(file_path: str):
        # donwload file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df_exchanges = pl.from_pandas(pd.read_json(io.BytesIO(file_content)))

        df_exchanges = df_exchanges.with_columns(
            [
                pl.lit(ASSET_SOURCE).alias("data_source"),
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        df_mic_correction = pl.from_dicts([{"mic": k, "mic_corrected": v} for k, v in ApiClient.mic_correction.items()])
        df_virtual_exchanges = pl.from_dict({"mic": ApiClient.virtual_exchanges})

        processed = duckdb.sql(
            """
            --sql
            SELECT
                COALESCE(df_mic_correction.mic_corrected, df_exchanges.mic) AS app_id,
                COALESCE(df_mic_correction.mic_corrected, df_exchanges.mic) mic,
                name,
                acronym,
                STRUCT_EXTRACT(currency, 'code') AS currency,
                city,
                if(df_virtual_exchanges.mic IS NOT NULL, NULL, country_code) AS country,
                if(LENGTH(website) == 0, NULL, website) AS website,
                STRUCT_EXTRACT(timezone, 'timezone') AS timezone,
                data_source,
                COALESCE(df_mic_correction.mic_corrected, df_exchanges.mic) AS source_code,
                if(df_virtual_exchanges.mic IS NOT NULL, true, false) AS is_virtual
            FROM
                df_exchanges
            LEFT JOIN df_mic_correction
            ON df_exchanges.mic = df_mic_correction.mic
            LEFT JOIN df_virtual_exchanges
            ON df_exchanges.mic = df_virtual_exchanges.mic
            ;
            """
        )

        parquet_file = processed.to_df().to_parquet()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangesPath(asset_source=ASSET_SOURCE, stage="processed"),
            file_system=config.azure.file_system,
            local_file=parquet_file,
        )

        return uploaded_file.file_path

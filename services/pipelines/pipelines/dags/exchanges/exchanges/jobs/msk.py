import polars as pl
from dags.exchanges.exchanges.jobs.config import ExchangesPath
from shared.clients.api.market_stack.client import MarketStackApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
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
        return datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, file_type="json", zone="raw"),
            file=converter.json_to_bytes(exchanges_json),
        ).file.full_path

    @staticmethod
    def transform_raw_exchanges(file_path: str):
        exchanges = duck.get_data(file_path, handler="azure_abfs", format="json").pl()

        exchanges = exchanges.with_columns(
            [
                pl.lit(ASSET_SOURCE).alias("data_source"),
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        transformed = duck.query(
            "./sql/transform_raw_msk.sql",
            exchanges=exchanges,
            virtual_exchange_mapping=DbQueryRepositories.mappings.get_mappings(
                source=ASSET_SOURCE, field="is_virtual", product="exchange"
            ),
            exchange_code_mappings=DbQueryRepositories.mappings.get_mappings(
                source=ASSET_SOURCE, field="exchange_code", product="exchange"
            ),
        ).df()

        return datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="processed"),
            file=transformed.to_parquet(),
        ).file.full_path

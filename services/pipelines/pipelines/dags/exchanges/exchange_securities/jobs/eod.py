import polars as pl
from dags.exchanges.exchange_securities.jobs.config import ExchangeSecuritiesPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.types.types import CodeFilePath
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class EodExchangeSecurityJobs:
    @staticmethod
    def extract_exchange_codes(file_path: str) -> list[str]:
        """
        Extracts exchanges codes from eod processed exchanges.
        """

        exchanges = duck.get_data(file_path, handler="azure_abfs").pl()

        return exchanges["exchange_source_code"].to_list()

        return ["BOND", "GBOND"]

    @staticmethod
    def download(exchange_code: str):
        """
        Retrieves listed securities for a given exchange code.
        """

        exhange_details = ApiClient.get_securities_listed_at_exhange(exhange_code=exchange_code)

        return datalake_client.upload_file(
            destination_file_path=ExchangeSecuritiesPath(
                zone="raw", exchange=exchange_code, asset_source=ASSET_SOURCE, file_type="json"
            ),
            file=converter.json_to_bytes(exhange_details),
        ).file.full_path

    @staticmethod
    def curate(file_path: str, exchange_uid: str):
        curated = duck.get_data(file_path, handler="azure_abfs")

        return datalake_client.upload_file(
            destination_file_path=ExchangeSecuritiesPath(
                zone="curated", exchange=exchange_uid, asset_source=ASSET_SOURCE
            ),
            file=curated.df().to_parquet(),
        ).file.full_path

    @staticmethod
    def transform(file_path: str) -> CodeFilePath:
        """
        Retrieves listed securities for a given exchange code.
        """
        import io
        import logging

        import polars as pl

        # Use pandas since duckDB has error: Unexpected yyjson tag in ValTypeToString
        json = datalake_client.download_file_into_memory(file_path)
        securities = pl.read_json(io.BytesIO(json))

        # some exchanges like CC or MONEY don't have ISIN column, so needs to be added
        if not "ISIN" in securities.columns:
            securities = securities.with_columns(pl.lit(None).alias("ISIN"))

        print(securities)

        transformed = duck.query(
            "./sql/transform_raw_securities.sql",
            securities=securities,
            security_type_mapping=DbQueryRepositories.mappings.get_mappings(
                source="EodHistoricalData", product="security", field="security_type"
            ),
            country_mapping=DbQueryRepositories.mappings.get_mappings(source="EodHistoricalData", product="country"),
            exchange_mapping=DbQueryRepositories.mappings.get_mappings(
                source="EodHistoricalData", product="exchange", field="exchange_code"
            ),
            source="EodHistoricalData",
        )

        exchange_uids = list(transformed.pl()["exchange_uid"].unique())
        exchange_uids = list(securities["Exchange"].unique())

        if len(exchange_uids) > 1:
            logging.warning(f"Exchange {file_path} {exchange_uids} more than one exchange found")

        exchange_uid = exchange_uids[0]

        return {
            "file_path": datalake_client.upload_file(
                destination_file_path=ExchangeSecuritiesPath(
                    zone="processed", exchange=exchange_uid, asset_source=ASSET_SOURCE
                ),
                file=transformed.df().to_parquet(),
            ).file.full_path,
            "code": exchange_uid,
        }

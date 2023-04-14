from dags.exchange.exchange_security.jobs.utils import ExchangeSecurityPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
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
        Extracts exchange codes from eod processed exchange.
        """

        exchange = duck.get_data(file_path, handler="azure_abfs").pl()

        return exchange["exchange_source_code"].to_list()

        return ["BOND", "GBOND"]

    @staticmethod
    def download(exchange_code: str):
        """
        Retrieves listed securities for a given exchange code.
        """

        exhange_details = ApiClient.get_securities_listed_at_exhange(exhange_code=exchange_code)

        return dl_client.upload_file(
            destination_file_path=ExchangeSecurityPath.raw(bin=exchange_code, source=ASSET_SOURCE, file_type="json"),
            file=converter.json_to_bytes(exhange_details),
        ).file.full_path

    @staticmethod
    def curate(file_path: str, exchange_uid: str):
        curated = duck.get_data(file_path, handler="azure_abfs")

        return dl_client.upload_file(
            destination_file_path=ExchangeSecurityPath.curated(bin=exchange_uid),
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
        json = dl_client.download_file_into_memory(file_path)
        securities = pl.read_json(io.BytesIO(json))

        # some exchange like CC or MONEY don't have ISIN column, so needs to be added
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
            "file_path": dl_client.upload_file(
                destination_file_path=ExchangeSecurityPath.processed(bin=exchange_uid, source=ASSET_SOURCE),
                file=transformed.df().to_parquet(),
            ).file.full_path,
            "code": exchange_uid,
        }

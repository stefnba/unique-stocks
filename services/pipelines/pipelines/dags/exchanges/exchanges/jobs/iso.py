import polars as pl
from dags.exchanges.exchanges.jobs.config import ExchangesPath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.duck.client import duck

ApiClient = IsoExchangesApiClient
ASSET_SOURCE = ApiClient.client_key


class IsoExchangeJobs:
    @staticmethod
    def download_exchanges():
        """
        Retrieves and upploads into the Data Lake raw ISO list with information
        on exchanges and their MIC codes.
        """

        # api data
        exchange_file = ApiClient.get_exchanges()

        return datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="raw", file_type="csv"),
            file=exchange_file.content,
        ).file.full_path

    @staticmethod
    def transform_raw_exchanges(file_path: str):
        exchanges = duck.get_data(file_path, handler="azure_abfs", format="csv").pl()

        exchanges = exchanges.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        transformed = duck.query("./sql/transform_raw_iso.sql", exchanges=exchanges, source=ASSET_SOURCE).df()

        # encoding="ISO8859-1")

        transformed["city"] = transformed["city"].str.title()
        transformed["market_name_institution"] = transformed["market_name_institution"].str.title()
        transformed["exchange_name"] = transformed["exchange_name"].str.title()
        transformed["comments"] = transformed["comments"].str.title()
        transformed["website"] = transformed["website"].str.lower()

        return datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="processed"),
            file=transformed.to_parquet(),
        ).file.full_path

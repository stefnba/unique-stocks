import polars as pl
from dags.exchange.exchange.jobs.utils import ExchangePath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck

ApiClient = IsoExchangesApiClient
ASSET_SOURCE = ApiClient.client_key


class IsoExchangeJobs:
    @staticmethod
    def download_exchanges():
        """
        Retrieves and upploads into the Data Lake raw ISO list with information
        on exchange and their MIC codes.
        """

        # api data
        exchange_file = ApiClient.get_exchanges()

        return dl_client.upload_file(
            destination_file_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="csv"),
            file=exchange_file.content,
        ).file.full_path

    @staticmethod
    def transform_raw_exchanges(file_path: str):
        exchange = duck.get_data(file_path, handler="azure_abfs", format="csv").pl()

        exchange = exchange.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        transformed = duck.query("./sql/transform_raw_iso.sql", exchange=exchange, source=ASSET_SOURCE).df()

        # encoding="ISO8859-1")

        transformed["city"] = transformed["city"].str.title()
        transformed["market_name_institution"] = transformed["market_name_institution"].str.title()
        transformed["exchange_name"] = transformed["exchange_name"].str.title()
        transformed["comments"] = transformed["comments"].str.title()
        transformed["website"] = transformed["website"].str.lower()

        return dl_client.upload_file(
            destination_file_path=ExchangePath.processed(source=ASSET_SOURCE),
            file=transformed.to_parquet(),
        ).file.full_path

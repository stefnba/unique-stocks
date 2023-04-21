import polars as pl
from dags.exchange.exchange.jobs.utils import ExchangePath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck

ApiClient = IsoExchangesApiClient
ASSET_SOURCE = ApiClient.client_key


class IsoExchangeJobs:
    @staticmethod
    def ingest():
        """
        Retrieves and uploads into the Data Lake raw ISO list with information
        on exchange and their MIC codes.
        """

        # api data
        exchange_file = ApiClient.get_exchanges()

        return dl_client.upload_file(
            destination_file_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="csv"),
            file=exchange_file.content,
        ).file.full_path

    @staticmethod
    def transform_raw(file_path: str):
        raw = pl.read_csv(dl_client.download_file_into_memory(file_path), encoding="latin1")

        raw = raw.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )

        transformed = duck.query("./sql/transform_raw_iso.sql", exchanges=raw, source=ASSET_SOURCE).df()

        transformed["city"] = transformed["city"].str.title()
        transformed["legal_entity_name"] = transformed["legal_entity_name"].str.title()
        transformed["name"] = transformed["name"].str.title()
        transformed["comments"] = transformed["comments"].str.title()
        transformed["website"] = transformed["website"].str.lower()

        return dl_client.upload_file(
            destination_file_path=ExchangePath.processed(source=ASSET_SOURCE),
            file=transformed.to_parquet(),
        ).file.full_path
